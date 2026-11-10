#!/usr/bin/env python3
"""Pure-Python, single-UE OCUDU gNB <-> OAI UE ZMQ channel bridge.

The hot path is a single pyzmq poll loop.  In ideal mode a ZMQ Frame is
forwarded directly, without converting/copying the IQ payload.  Other modes
interpret payloads as native-endian complex64 and apply a streaming channel.
No GNU Radio module or scheduler is used.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import glob
import importlib
import logging
import pickle
import re
import signal
import socket as stdlib_socket
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import zmq


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("ocudu_oai_bridge_py")
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CIR_DIR = SCRIPT_DIR.parent
DEFAULT_CIR_FILE = SCRIPT_DIR / "CIR.dat"
DEFAULT_TAPS_FILE = SCRIPT_DIR / "msu_cir_taps.npz"
COMPLEX64_BYTES = np.dtype(np.complex64).itemsize


def db_to_linear(path_loss_db):
    return 10.0 ** (-path_loss_db / 20.0)


def _install_numpy_pickle_compat():
    aliases = {
        "numpy._core": "numpy.core",
        "numpy._core.numeric": "numpy.core.numeric",
        "numpy._core.multiarray": "numpy.core.multiarray",
        "numpy._core._multiarray_umath": "numpy.core._multiarray_umath",
    }
    for alias, target in aliases.items():
        if alias not in sys.modules:
            try:
                sys.modules[alias] = importlib.import_module(target)
            except ImportError:
                pass


def _resolve_path_axis(shape, requested):
    if not shape or requested == "first":
        return 0
    if requested == "last":
        return len(shape) - 1
    if len(shape) == 1:
        return 0
    if shape[0] > max(shape[1:]):
        return 0
    if shape[-1] > max(shape[:-1]):
        return len(shape) - 1
    if shape[0] >= 16 and shape[0] >= shape[-1]:
        return 0
    if shape[-1] >= 16:
        return len(shape) - 1
    return len(shape) - 1


def _as_paths_matrix(arr, requested_path_axis):
    arr = np.asarray(arr)
    if arr.ndim == 0:
        return arr.reshape(1, 1), ()
    moved = np.moveaxis(arr, _resolve_path_axis(arr.shape, requested_path_axis), 0)
    return moved.reshape(moved.shape[0], -1), moved.shape[1:]


def _antenna_column(nonpath_shape, rx_ant, tx_ant):
    if not nonpath_shape:
        return 0
    if len(nonpath_shape) == 1:
        return min(rx_ant, nonpath_shape[0] - 1)
    idx = [0] * len(nonpath_shape)
    idx[0] = min(rx_ant, nonpath_shape[0] - 1)
    idx[1] = min(tx_ant, nonpath_shape[1] - 1)
    return int(np.ravel_multi_index(tuple(idx), nonpath_shape))


def _select_cir_vectors(values, delays, path_axis, antenna_mode, rx_ant, tx_ant):
    values, value_shape = _as_paths_matrix(values, path_axis)
    delays, delay_shape = _as_paths_matrix(delays, path_axis)
    n_paths = min(values.shape[0], delays.shape[0])
    values, delays = values[:n_paths], delays[:n_paths]
    if antenna_mode == "sum":
        if delays.shape[1] == 1:
            delays = np.repeat(delays, values.shape[1], axis=1)
        elif delays.shape[1] != values.shape[1]:
            repeats = int(np.ceil(values.shape[1] / delays.shape[1]))
            delays = np.tile(delays, (1, repeats))[:, :values.shape[1]]
        return values.reshape(-1), delays.reshape(-1)
    if antenna_mode == "strongest":
        col = int(np.argmax(np.sum(np.abs(values) ** 2, axis=0)))
    else:
        col = min(_antenna_column(value_shape, rx_ant, tx_ant), values.shape[1] - 1)
        if not np.any(np.abs(values[:, col]) > 0):
            col = int(np.argmax(np.sum(np.abs(values) ** 2, axis=0)))
    delay_col = min(col, delays.shape[1] - 1) if delay_shape == value_shape else 0
    return values[:, col], delays[:, delay_col]


def cir_to_taps(values, delays, samp_rate, keep_absolute_delay=False,
                normalize_power=True, path_loss_db=0.0, path_axis="auto",
                antenna_mode="sum", rx_ant=0, tx_ant=0, max_channel_taps=4096):
    amp, dly = _select_cir_vectors(values, delays, path_axis, antenna_mode, rx_ant, tx_ant)
    amp = np.asarray(amp, dtype=np.complex128).reshape(-1)
    dly = np.asarray(dly, dtype=np.float64).reshape(-1)
    valid = np.isfinite(dly) & np.isfinite(amp.real) & np.isfinite(amp.imag) & (np.abs(amp) > 0)
    amp, dly = amp[valid], dly[valid]
    if not amp.size:
        return np.array([1 + 0j], dtype=np.complex64)
    if not keep_absolute_delay:
        dly -= np.min(dly)
    tap_idx = np.round(np.maximum(dly, 0) * samp_rate).astype(np.int64)
    if max_channel_taps and tap_idx.size and tap_idx.max() >= max_channel_taps:
        keep = tap_idx < max_channel_taps
        if np.any(keep):
            amp, tap_idx = amp[keep], tap_idx[keep]
        else:
            strongest = int(np.argmax(np.abs(amp)))
            amp, tap_idx = amp[strongest:strongest + 1], np.array([0])
    taps = np.zeros(int(tap_idx.max()) + 1 if tap_idx.size else 1, dtype=np.complex128)
    np.add.at(taps, tap_idx, amp)
    if normalize_power:
        energy = np.sqrt(np.sum(np.abs(taps) ** 2))
        if energy:
            taps /= energy
    if path_loss_db:
        taps *= db_to_linear(path_loss_db)
    return (taps if np.any(taps) else np.array([1 + 0j])).astype(np.complex64)


class StreamingChannel:
    """Stateful complex64 FIR and AWGN channel, continuous across messages."""

    def __init__(self, taps, delay_samples=0, noise_power_dbfs=None, snr_db=None,
                 noise_seed=None):
        self._lock = threading.Lock()
        self._delay_samples = max(0, int(delay_samples))
        self._noise_power_dbfs = noise_power_dbfs
        self._snr_db = snr_db
        # Generate I/Q normal streams concurrently. SFC64 is substantially
        # faster than NumPy's default PCG64 for high-rate float32 normals,
        # while remaining suitable for channel simulation.
        if noise_power_dbfs is not None or snr_db is not None:
            seeds = np.random.SeedSequence(noise_seed).spawn(2)
            self._noise_rngs = tuple(
                np.random.Generator(np.random.SFC64(seed)) for seed in seeds)
            self._noise_executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=2, thread_name_prefix="awgn-rng")
        else:
            self._noise_rngs = ()
            self._noise_executor = None
        self._noise_buffer = np.empty(0, dtype=np.float32)
        self._signal_rms = None
        self._snr_message_count = 0
        self._taps = np.array([1 + 0j], dtype=np.complex64)
        self._history = np.empty(0, dtype=np.complex64)
        self.set_taps(taps)

    def set_taps(self, taps):
        taps = np.asarray(taps, dtype=np.complex64).reshape(-1)
        if not taps.size:
            taps = np.array([1 + 0j], dtype=np.complex64)
        if self._delay_samples:
            taps = np.pad(taps, (self._delay_samples, 0))
        with self._lock:
            old = self._history
            self._taps = taps
            self._history = np.zeros(max(0, taps.size - 1), dtype=np.complex64)
            keep = min(old.size, self._history.size)
            if keep:
                self._history[-keep:] = old[-keep:]

    def close(self):
        if self._noise_executor is not None:
            self._noise_executor.shutdown(wait=True, cancel_futures=True)

    def process(self, frame):
        view = memoryview(frame)
        if len(view) % COMPLEX64_BYTES:
            raise ValueError(f"IQ payload has {len(view)} bytes; expected a multiple of {COMPLEX64_BYTES}")
        samples = np.frombuffer(view, dtype=np.complex64)
        if not samples.size:
            return b""
        with self._lock:
            if self._taps.size == 1:
                if self._taps[0] == 1:
                    out = samples.copy()
                else:
                    out = (samples * self._taps[0]).astype(np.complex64, copy=False)
            else:
                # FFT-based (overlap-add) convolution instead of lfilter: for a
                # pure-FIR filter (a=[1]), y = conv(taps, [history, samples])
                # in "valid" mode is exactly lfilter's zi-based streaming
                # result, but oaconvolve is ~3x faster here (measured: ~400us
                # vs ~1180us for a 33-tap filter over a ~12.7k-sample message)
                # -- lfilter's general IIR recursion doesn't exploit the pure
                # convolution structure. That gap mattered: at this bridge's
                # ~1600 msg/s per direction (~625us budget/message), lfilter
                # couldn't keep up, so the gNB's TX side built up backlog that
                # periodically flushed as an oversized burst -- overflowing
                # the UE's fixed 300000-sample ZMQ RX buffer (openair
                # radio/zmq/zmq_radio.cpp) and preventing sync entirely.
                from scipy.signal import oaconvolve

                extended = np.concatenate([self._history, samples])
                out = oaconvolve(extended, self._taps, mode="valid").astype(np.complex64, copy=False)
                tail = self._taps.size - 1
                self._history = extended[-tail:].copy() if tail else self._history
            self._add_noise(out)
        return out.astype(np.complex64, copy=False).tobytes()

    def _add_noise(self, samples):
        if self._noise_power_dbfs is None and self._snr_db is None:
            return
        if self._noise_power_dbfs is not None:
            noise_rms = 10.0 ** (self._noise_power_dbfs / 20.0)
        else:
            # Track power over the stream instead of rescanning every ZMQ
            # message. Updating every 32 messages removes a full memory pass
            # from the hot path while following normal radio power changes.
            if self._signal_rms is None or self._snr_message_count % 32 == 0:
                self._signal_rms = np.sqrt(
                    np.vdot(samples, samples).real / samples.size)
            self._snr_message_count += 1
            noise_rms = self._signal_rms * 10.0 ** (-self._snr_db / 20.0)
        if noise_rms == 0:
            return
        required = samples.size * 2
        if self._noise_buffer.size != required:
            self._noise_buffer = np.empty(required, dtype=np.float32)
        half = samples.size
        i_future = self._noise_executor.submit(
            self._noise_rngs[0].standard_normal,
            dtype=np.float32,
            out=self._noise_buffer[:half],
        )
        q_future = self._noise_executor.submit(
            self._noise_rngs[1].standard_normal,
            dtype=np.float32,
            out=self._noise_buffer[half:],
        )
        i_future.result()
        q_future.result()
        noise = self._noise_buffer.view(np.complex64)
        noise *= noise_rms / np.sqrt(2.0)
        samples += noise


@dataclass
class Direction:
    name: str
    downstream: zmq.Socket
    upstream: zmq.Socket
    channel: StreamingChannel | None
    waiting: bool = False
    request_started: float = 0.0
    messages: int = 0
    samples: int = 0
    future: concurrent.futures.Future | None = None
    pending_samples: int = 0


class PurePythonBridge:
    def __init__(self, args, initial_taps=None):
        self.args = args
        self.stop_event = threading.Event()
        self.ctx = zmq.Context()
        delay_samples = round(args.ue_delay_us * 1e-6 * args.samp_rate)
        if args.channel_mode == "static":
            taps = np.array([db_to_linear(args.path_loss_db)], dtype=np.complex64)
        elif args.channel_mode == "ideal":
            taps = np.array([1 + 0j], dtype=np.complex64)
        else:
            taps = initial_taps if initial_taps is not None else np.array([1 + 0j], dtype=np.complex64)

        def make_channel(direction, seed_offset):
            noise_enabled = args.noise_direction in ("both", direction)
            noise_power_dbfs = args.noise_power_dbfs if noise_enabled else None
            snr_db = args.snr_db if noise_enabled else None
            if (args.channel_mode == "ideal" and delay_samples == 0 and
                    noise_power_dbfs is None and snr_db is None):
                return None
            seed = None if args.noise_seed is None else args.noise_seed + seed_offset
            return StreamingChannel(taps, delay_samples, noise_power_dbfs,
                                    snr_db, seed)

        def socket(kind, address, bind):
            sock = self.ctx.socket(kind)
            sock.setsockopt(zmq.LINGER, 0)
            sock.setsockopt(zmq.SNDHWM, 2)
            sock.setsockopt(zmq.RCVHWM, 2)
            (sock.bind if bind else sock.connect)(address)
            return sock

        def wait_for_ipc_peer(address):
            if not address.startswith("ipc:///"):
                return
            path = Path(address.removeprefix("ipc://"))
            deadline = time.monotonic() + args.ipc_connect_timeout
            log.info("Waiting for IPC peer to bind %s", path)
            while not path.exists():
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"timed out waiting for IPC peer {path}")
                time.sleep(0.05)

        connect = lambda endpoint, port: endpoint or f"tcp://{args.connect_host}:{port}"
        bind = lambda endpoint, port: endpoint or f"tcp://{args.bind_host}:{port}"
        self.gnb_tx_address = connect(args.gnb_tx_endpoint, args.gnb_tx_port)
        self.gnb_rx_address = bind(args.gnb_rx_endpoint, args.gnb_rx_port)
        self.ue_tx_address = connect(args.ue_tx_endpoint, args.ue_tx_port)
        self.ue_rx_address = bind(args.ue_rx_endpoint, args.ue_rx_port)
        # Bind both downstream sockets first. IPC connect does not reliably
        # recover when its filesystem endpoint did not exist at connect time,
        # so wait until the gNB/UE have bound their TX sockets before connecting.
        dl_downstream = socket(zmq.REP, self.ue_rx_address, True)
        ul_downstream = socket(zmq.REP, self.gnb_rx_address, True)
        wait_for_ipc_peer(self.gnb_tx_address)
        wait_for_ipc_peer(self.ue_tx_address)
        self.dl = Direction("DL", dl_downstream,
                            socket(zmq.REQ, self.gnb_tx_address, False),
                            make_channel("dl", 0))
        self.ul = Direction("UL", ul_downstream,
                            socket(zmq.REQ, self.ue_tx_address, False),
                            make_channel("ul", 1))
        self.directions = (self.dl, self.ul)
        dsp_workers = sum(direction.channel is not None for direction in self.directions)
        self.executor = (concurrent.futures.ThreadPoolExecutor(
            max_workers=dsp_workers, thread_name_prefix="channel-dsp")
            if dsp_workers else None)
        self.wake_read, self.wake_write = stdlib_socket.socketpair()
        self.wake_read.setblocking(False)
        self.poller = zmq.Poller()
        self.poller.register(self.wake_read.fileno(), zmq.POLLIN)
        for direction in self.directions:
            self.poller.register(direction.downstream, zmq.POLLIN)
            self.poller.register(direction.upstream, zmq.POLLIN)

    def set_taps(self, taps):
        for direction in self.directions:
            if direction.channel is not None:
                direction.channel.set_taps(taps)

    def stop(self):
        self.stop_event.set()
        try:
            self.wake_write.send(b"\0")
        except OSError:
            pass

    def _wake_when_done(self, _future):
        try:
            self.wake_write.send(b"\0")
        except OSError:
            pass

    def _send_reply(self, direction, reply, samples):
        direction.downstream.send(reply, copy=False)
        direction.waiting = False
        direction.messages += 1
        direction.samples += samples

    def _finish_dsp(self):
        for direction in self.directions:
            if direction.future is None or not direction.future.done():
                continue
            try:
                reply = direction.future.result()
            except Exception as exc:
                log.error("%s: %s; forwarding an empty response", direction.name, exc)
                reply = b""
            self._send_reply(direction, reply, direction.pending_samples)
            direction.future = None
            direction.pending_samples = 0

    def run(self):
        args = self.args
        log.info("Topology (pure Python/pyzmq backend, single UE):")
        log.info("  DL: gNB REP %s -> UE REP(bind) %s", self.gnb_tx_address, self.ue_rx_address)
        log.info("  UL: UE REP %s -> gNB REP(bind) %s", self.ue_tx_address, self.gnb_rx_address)
        dsp_directions = [direction.name for direction in self.directions
                          if direction.channel is not None]
        if not dsp_directions:
            channel_detail = "zero-copy payload forwarding"
        else:
            channel_detail = f"streaming complex64 DSP on {','.join(dsp_directions)}"
        log.info("  Channel: %s (%s)", args.channel_mode, channel_detail)
        if args.noise_power_dbfs is not None:
            log.info("  AWGN: %.1f dBFS complex RMS, direction=%s, seed=%s",
                     args.noise_power_dbfs, args.noise_direction, args.noise_seed)
        elif args.snr_db is not None:
            log.info("  AWGN: target SNR %.1f dB, direction=%s, seed=%s",
                     args.snr_db, args.noise_direction, args.noise_seed)
        last_report = time.monotonic()
        try:
            while not self.stop_event.is_set():
                events = dict(self.poller.poll(100))
                if events.get(self.wake_read.fileno(), 0) & zmq.POLLIN:
                    try:
                        while self.wake_read.recv(64):
                            pass
                    except BlockingIOError:
                        pass
                self._finish_dsp()
                for direction in self.directions:
                    if not direction.waiting and events.get(direction.downstream, 0) & zmq.POLLIN:
                        request = direction.downstream.recv(copy=False)
                        direction.upstream.send(request, copy=False)
                        direction.waiting = True
                        direction.request_started = time.monotonic()
                    if direction.waiting and events.get(direction.upstream, 0) & zmq.POLLIN:
                        payload = direction.upstream.recv(copy=False)
                        samples = len(payload) // COMPLEX64_BYTES
                        if direction.channel is None:
                            self._send_reply(direction, payload, samples)
                        else:
                            direction.pending_samples = samples
                            direction.future = self.executor.submit(
                                direction.channel.process, payload)
                            direction.future.add_done_callback(self._wake_when_done)
                now = time.monotonic()
                if now - last_report >= args.stats_interval:
                    text = ", ".join(f"{d.name}={d.messages} msgs/{d.samples} samples" for d in self.directions)
                    pending = [f"{d.name}:{now-d.request_started:.1f}s" for d in self.directions if d.waiting]
                    log.info("Traffic: %s%s", text, f"; pending {','.join(pending)}" if pending else "")
                    last_report = now
        finally:
            if self.executor is not None:
                self.executor.shutdown(wait=True, cancel_futures=True)
            for direction in self.directions:
                if direction.channel is not None:
                    direction.channel.close()
            for direction in self.directions:
                direction.downstream.close(0)
                direction.upstream.close(0)
            self.ctx.destroy(linger=0)
            self.wake_read.close()
            self.wake_write.close()


def _lookup(mapping, key):
    if not isinstance(mapping, dict) or key is None:
        return None
    for candidate in (key, str(key)):
        if candidate in mapping:
            return mapping[candidate]
    try:
        wanted = int(key)
        return next(value for map_key, value in mapping.items() if int(map_key) == wanted)
    except (TypeError, ValueError, StopIteration):
        return None


def _infer_step(path):
    match = re.search(r"[tT](\d+)", Path(path).name) or re.search(r"-(\d{4,9})(?:\D|$)", Path(path).name)
    return int(match.group(1)) if match else None


def _iter_saved_records(obj, args, default_step=None):
    if isinstance(obj, (tuple, list)) and len(obj) == 2:
        data, delays = obj
        if isinstance(data, dict) and isinstance(delays, dict):
            for step_key, by_ru in data.items():
                d_by_ru = _lookup(delays, step_key)
                by_ue, d_by_ue = _lookup(by_ru, args.ru_id), _lookup(d_by_ru, args.ru_id)
                values, dly = _lookup(by_ue, args.ue_id), _lookup(d_by_ue, args.ue_id)
                if values is not None and dly is not None:
                    yield int(step_key), values, dly
        elif default_step is not None:
            yield default_step, data, delays
    elif isinstance(obj, dict):
        if "data" in obj and "delays" in obj:
            yield from _iter_saved_records((obj["data"], obj["delays"]), args, default_step)
        else:
            value_key = "values" if "values" in obj else "cir" if "cir" in obj else None
            delay_key = "delays" if "delays" in obj else "delay" if "delay" in obj else None
            step = obj.get("time_idx", obj.get("step", default_step))
            if value_key and delay_key and step is not None and int(obj.get("ue_id", args.ue_id)) == args.ue_id:
                yield int(step), obj[value_key], obj[delay_key]


def _taps_from_cir(values, delays, args, normalize_power=None):
    if normalize_power is None:
        normalize_power = args.normalize_power
    return cir_to_taps(values, delays, args.samp_rate, args.keep_absolute_delay,
                       normalize_power, args.path_loss_db, args.cir_path_axis,
                       args.antenna_mode, args.rx_ant, args.tx_ant, args.max_channel_taps)


def _apply_fixed_dataset_gain(taps, args):
    """Scale every CIR step by one gain, preserving inter-step path loss."""
    energies = np.asarray([
        np.sqrt(np.vdot(step.astype(np.complex128), step.astype(np.complex128)).real)
        for step in taps
    ])
    nonzero = energies[energies > 0]
    if not nonzero.size:
        return taps
    reference = float(nonzero.max())
    gain = 1.0 / reference if args.normalize_power else 1.0
    scaled = [(step * gain).astype(np.complex64, copy=False) for step in taps]
    variation_db = 20.0 * np.log10(float(nonzero.max() / nonzero.min()))
    log.info(
        "CIR dataset gain=%.6g (strongest raw energy=%.6g); preserving %.2f dB inter-step amplitude variation",
        gain, reference, variation_db)
    return scaled


def load_local_cirs(args):
    patterns = args.cir_file or [str(Path(args.cir_dir).expanduser() / pattern) for pattern in
        ("CIR.dat", "CIR.pkl", "CIR.pickle", f"sample-cir-ue{args.ue_id}-t*.*")]
    files, seen = [], set()
    for pattern in patterns:
        for name in glob.glob(str(Path(pattern).expanduser())):
            resolved = Path(name).resolve()
            if resolved not in seen:
                files.append(Path(name)); seen.add(resolved)
    if not files:
        raise FileNotFoundError(f"no CIR files matched: {patterns}")
    records = {}
    for path in files:
        if path.suffix.lower() == ".npz":
            with np.load(path, allow_pickle=False) as npz:
                obj = {key: npz[key] for key in npz.files}
        else:
            if not args.allow_pickle:
                continue
            _install_numpy_pickle_compat()
            with path.open("rb") as stream:
                obj = pickle.load(stream)
        for step, values, delays in _iter_saved_records(obj, args, _infer_step(path)):
            # Load raw taps first. If requested, normalization is applied once
            # to the complete dataset below, never independently per step.
            records[step] = _taps_from_cir(values, delays, args, normalize_power=False)
    if not records:
        raise ValueError("CIR files contained no matching records")
    steps = sorted(records)
    return _apply_fixed_dataset_gain([records[step] for step in steps], args), steps


def load_taps_file(args):
    path = Path(args.cir_taps_file).expanduser()
    with np.load(path, allow_pickle=False) as npz:
        re_part, im_part, lengths = npz["taps_re"], npz["taps_im"], npz["tap_lengths"]
        steps = npz["time_steps"].astype(int).tolist() if "time_steps" in npz else list(range(re_part.shape[0]))
    if re_part.shape != im_part.shape or re_part.ndim != 3 or lengths.shape != re_part.shape[:2]:
        raise ValueError(f"invalid bridge taps arrays in {path}")
    col = min(args.ue_taps_column, re_part.shape[1] - 1)
    taps = []
    for idx in range(re_part.shape[0]):
        count = int(lengths[idx, col])
        taps.append((re_part[idx, col, :count] + 1j * im_part[idx, col, :count]).astype(np.complex64)
                    if count > 0 else np.array([1 + 0j], dtype=np.complex64))
    return _apply_fixed_dataset_gain(taps, args), steps


class TapUpdater(threading.Thread):
    def __init__(self, bridge, args, taps, steps):
        super().__init__(name="cir-updater", daemon=True)
        self.bridge, self.args, self.taps, self.steps = bridge, args, taps, steps

    def run(self):
        trigger = getattr(self.args, "step_trigger_file", "")
        if trigger:
            trigger_path = Path(trigger)
            log.info("Holding startup CIR until trigger file appears: %s", trigger_path)
            while not trigger_path.exists():
                if self.bridge.stop_event.wait(0.1):
                    return
            # A weak first route point may be unsuitable for initial UE access.
            # Startup can therefore use the dataset's strongest CIR, but the
            # measurement replay must always restart at the real first point.
            self.bridge.set_taps(self.taps[0])
            energy = np.sqrt(np.vdot(self.taps[0].astype(np.complex128), self.taps[0].astype(np.complex128)).real)
            log.info("Trigger received; reset to CIR time_step=%s, tap_energy=%.6g (%.2f dB)",
                     self.steps[0], energy,
                     20.0 * np.log10(energy) if energy > 0 else float("-inf"))
        index = repeat = loops = 0
        while not self.bridge.stop_event.wait(self.args.cir_interval):
            repeat += 1
            if repeat < self.args.repeat_per_step:
                continue
            repeat = 0
            index += 1
            if index == len(self.taps):
                if self.args.stop_when_cir_exhausted:
                    log.info("All %d CIR steps applied; stopping the bridge", len(self.taps))
                    self.bridge.stop()
                    return
                if not self.args.loop_scenario:
                    log.info("All %d CIR steps applied; holding the final CIR", len(self.taps))
                    return
                index = 0; loops += 1
            current = self.taps[index]
            self.bridge.set_taps(current)
            energy = np.sqrt(np.vdot(current.astype(np.complex128), current.astype(np.complex128)).real)
            log.info("CIR time_step=%s (loop %d), tap_energy=%.6g (%.2f dB)",
                     self.steps[index], loops, energy,
                     20.0 * np.log10(energy) if energy > 0 else float("-inf"))


class LiveCirUpdater(threading.Thread):
    def __init__(self, client, allocation, bridge, args, max_step):
        super().__init__(name="aodt-cir-updater", daemon=True)
        self.client = client
        self.allocation = allocation
        self.bridge = bridge
        self.args = args
        self.max_step = max_step
        self.applied_taps = {}

    def _save_applied_taps(self):
        """Atomically save all applied taps once the final live step is ready."""
        if not self.args.save_applied_taps or not self.applied_taps:
            return
        output = Path(self.args.save_applied_taps).expanduser()
        output.parent.mkdir(parents=True, exist_ok=True)
        steps = sorted(self.applied_taps)
        max_taps = max(len(self.applied_taps[item]) for item in steps)
        matrix = np.zeros((len(steps), max_taps), dtype=np.complex64)
        lengths = np.empty(len(steps), dtype=np.int32)
        for row, item in enumerate(steps):
            current = self.applied_taps[item]
            matrix[row, :len(current)] = current
            lengths[row] = len(current)
        temporary = output.with_suffix(output.suffix + ".tmp")
        with temporary.open("wb") as handle:
            np.savez(handle, time_steps=np.asarray(steps, dtype=np.int32),
                     tap_lengths=lengths, taps=matrix,
                     sample_rate_hz=float(self.args.samp_rate))
        temporary.replace(output)
        log.info("Saved %d applied AODT CIR steps to %s", len(steps), output)

    def _apply_and_record(self, step, values, delays):
        """Apply exactly the channel used by the bridge and retain it for plots."""
        taps = _taps_from_cir(values[0], delays[0], self.args)
        self.bridge.set_taps(taps)
        if not self.args.save_applied_taps:
            return
        self.applied_taps[int(step)] = np.asarray(taps, dtype=np.complex64)
        if int(step) == self.max_step:
            self._save_applied_taps()

    def run(self):
        from dt_client import SlotIndex, TimeStepIndex

        temporal_cls = SlotIndex if self.args.is_slot_symbol_mode else TimeStepIndex
        trigger = self.args.step_trigger_file
        if trigger:
            trigger_path = Path(trigger)
            try:
                self.client.get_cirs(
                    self.allocation, batch_index=0, temporal_index=temporal_cls(0))
                values = self.client.to_numpy(
                    self.allocation, 0, self.args.ru_index, "values")
                delays = self.client.to_numpy(
                    self.allocation, 0, self.args.ru_index, "delays")
                self._apply_and_record(0, values, delays)
                log.info(
                    "Applied live AODT CIR step 0 for startup; waiting for traffic trigger %s",
                    trigger_path)
            except RuntimeError as exc:
                log.warning(
                    "Initial AODT CIR step 0 failed: %s; waiting with passthrough taps",
                    exc)
            while not trigger_path.exists():
                if self.bridge.stop_event.wait(0.05):
                    return
            log.info("Traffic trigger received; CIR step 0 clock starts now")

        started = time.monotonic()
        last_step = -1
        while not self.bridge.stop_event.is_set():
            step = int((time.monotonic() - started) / self.args.cir_interval)
            if step > self.max_step:
                if self.args.loop_scenario:
                    started = time.monotonic()
                    step = 0
                elif self.args.stop_when_cir_exhausted:
                    log.info("AODT scenario complete (%d CIR steps); stopping the bridge", self.max_step + 1)
                    self.bridge.stop()
                    return
                else:
                    log.info("AODT scenario complete; holding the last CIR")
                    return
            if step == last_step:
                self.bridge.stop_event.wait(0.01)
                continue
            try:
                self.client.get_cirs(self.allocation, batch_index=0,
                                     temporal_index=temporal_cls(step))
                values = self.client.to_numpy(self.allocation, step, self.args.ru_index, "values")
                delays = self.client.to_numpy(self.allocation, step, self.args.ru_index, "delays")
                self._apply_and_record(step, values, delays)
                last_step = step
                log.info("Applied live AODT CIR step %d", step)
            except RuntimeError as exc:
                log.warning("AODT CIR step %d failed: %s; retaining previous taps", step, exc)
                self.bridge.stop_event.wait(0.2)


def run_aodt(args):
    try:
        import dt_client
    except ImportError as exc:
        log.error("--channel-mode aodt requires dt_client: %s", exc)
        return 1
    yaml_path = Path(args.yaml_file).expanduser()
    if not yaml_path.is_file():
        log.error("--yaml-file must name an existing AODT scenario")
        return 1
    client = dt_client.DigitalTwinClient(args.server_address, force=args.force_connect)
    client.start_server_log_streaming("dt_server.log", "INFO")
    allocation = bridge = updater = None
    try:
        client.start(yaml_path.read_text())
        status = client.get_status()
        if not status["scenario_loaded"]:
            raise RuntimeError("AODT server did not load the scenario")
        args.is_slot_symbol_mode = status["is_slot_symbol_mode"]
        max_step = max(0, status["num_slots_or_timesteps_per_batch"] - 1)
        allocation = client.allocate_cirs_memory(
            [args.ru_index], [[args.ue_aodt_index]], num_time_steps=1)
        bridge = PurePythonBridge(args)
        updater = LiveCirUpdater(client, allocation, bridge, args, max_step)
        signal.signal(signal.SIGINT, lambda *_: bridge.stop())
        signal.signal(signal.SIGTERM, lambda *_: bridge.stop())
        updater.start()
        bridge.run()
        updater.join(timeout=2)
        return 0
    except RuntimeError as exc:
        log.error("AODT mode failed: %s", exc)
        return 1
    finally:
        if bridge is not None:
            bridge.stop()
        if allocation is not None:
            client.deallocate_cirs_memory(allocation)
        client.stop_server_log_streaming()


def run_bridge(args, taps=None, steps=None, update_taps=True):
    startup_taps = taps[0] if taps else None
    if taps and args.step_trigger_file and args.startup_strongest_cir:
        startup_index = int(np.argmax([
            np.vdot(step.astype(np.complex128), step.astype(np.complex128)).real
            for step in taps
        ]))
        startup_taps = taps[startup_index]
        log.info("Using strongest CIR time_step=%s for startup; replay will reset to step 0 on trigger",
                 steps[startup_index])
    bridge = PurePythonBridge(args, startup_taps)
    updater = TapUpdater(bridge, args, taps, steps) if taps and update_taps else None
    signal.signal(signal.SIGINT, lambda *_: bridge.stop())
    signal.signal(signal.SIGTERM, lambda *_: bridge.stop())
    if updater:
        updater.start()
    bridge.run()
    if updater:
        updater.join(timeout=2)
    return 0


def build_parser():
    p = argparse.ArgumentParser(description="Pure-Python single-UE OCUDU/OAI ZMQ channel bridge")
    p.add_argument("--connect-host", default="127.0.0.1")
    p.add_argument("--bind-host", default="*")
    p.add_argument("--host", default=None, help="set both connect and bind host")
    p.add_argument("--channel-mode", choices=("ideal", "static", "aodt", "precomputed", "cir"), default="ideal")
    p.add_argument("--gnb-tx-port", type=int, default=2000)
    p.add_argument("--gnb-rx-port", type=int, default=2001)
    p.add_argument("--ue-tx-port", type=int, default=2101)
    p.add_argument("--ue-rx-port", type=int, default=2100)
    p.add_argument("--gnb-tx-endpoint", default=None,
                   help="full gNB TX endpoint; overrides --connect-host/--gnb-tx-port")
    p.add_argument("--gnb-rx-endpoint", default=None,
                   help="full bridge-bound gNB RX endpoint; overrides --bind-host/--gnb-rx-port")
    p.add_argument("--ue-tx-endpoint", default=None,
                   help="full UE TX endpoint; overrides --connect-host/--ue-tx-port")
    p.add_argument("--ue-rx-endpoint", default=None,
                   help="full bridge-bound UE RX endpoint; overrides --bind-host/--ue-rx-port")
    p.add_argument("--ipc-connect-timeout", type=float, default=300.0,
                   help="seconds to wait for gNB/UE IPC TX endpoints after binding bridge endpoints")
    p.add_argument("--samp-rate", type=float, default=23_040_000.0)
    p.add_argument("--slow-down-ratio", type=float, default=1.0,
                   help="accepted for CLI compatibility; endpoint backpressure clocks this bridge")
    p.add_argument("--zmq-timeout-ms", type=int, default=100, help="accepted for CLI compatibility")
    p.add_argument("--stats-interval", type=float, default=5.0)
    p.add_argument("--path-loss-db", type=float, default=0.0)
    p.add_argument("--ue-delay-us", type=float, default=0.0)
    noise = p.add_mutually_exclusive_group()
    noise.add_argument("--noise-power-dbfs", type=float, default=None,
                       help="complex AWGN RMS power in dBFS; for example -40")
    noise.add_argument("--snr-db", type=float, default=None,
                       help="set AWGN per message for the requested signal-to-noise ratio")
    p.add_argument("--noise-direction", choices=("both", "dl", "ul"), default="both")
    p.add_argument("--noise-seed", type=int, default=None,
                   help="reproducible base RNG seed; UL uses seed+1")
    p.add_argument("--server-address", default="localhost:50051")
    p.add_argument("--force-connect", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--yaml-file", default="")
    p.add_argument("--ru-index", type=int, default=0)
    p.add_argument("--ue-aodt-index", type=int, default=0)
    p.add_argument("--cir-interval", type=float, default=1.0)
    p.add_argument("--save-applied-taps", default="",
                   help="write the exact per-step complex taps applied by live AODT to an NPZ file")
    p.add_argument("--step-trigger-file", default="",
                   help="hold CIR step 0 until this file exists")
    p.add_argument("--startup-strongest-cir", action="store_true",
                   help="before a step trigger, use the dataset's strongest CIR for reliable UE access; replay resets to step 0")
    p.add_argument("--loop-scenario", action="store_true")
    p.add_argument("--stop-when-cir-exhausted", action=argparse.BooleanOptionalAction, default=True,
                   help="--channel-mode aodt/cir/precomputed only: once every CIR step has been applied, "
                        "stop the whole bridge (closing its ZMQ endpoints) instead of holding the last CIR "
                        "forever / looping. Ignored if --loop-scenario is set. On by default; pass "
                        "--no-stop-when-cir-exhausted to hold/loop instead.")
    p.add_argument("--keep-absolute-delay", action="store_true")
    p.add_argument("--normalize-power", action="store_true", default=False,
                   help="apply one fixed gain derived from the strongest CIR step; preserves relative "
                        "path loss and amplitude variation between steps")
    p.add_argument("--cir-path-axis", choices=("auto", "first", "last"), default="auto")
    p.add_argument("--antenna-mode", choices=("sum", "first", "strongest"), default="sum")
    p.add_argument("--rx-ant", type=int, default=0)
    p.add_argument("--tx-ant", type=int, default=0)
    p.add_argument("--max-channel-taps", type=int, default=4096)
    p.add_argument("--freeze-first-cir", action="store_true")
    p.add_argument("--fixed-cir-step", type=int, default=None,
                   help="--channel-mode cir/precomputed only: permanently use this CIR time_step; "
                        "the channel remains frequency-selective but does not change over time")
    p.add_argument("--cir-source", choices=("taps", "local"), default="local")
    p.add_argument("--cir-taps-file", default=str(DEFAULT_TAPS_FILE))
    p.add_argument("--ue-taps-column", type=int, default=0)
    p.add_argument("--cir-dir", default=str(DEFAULT_CIR_DIR))
    p.add_argument("--cir-file", action="append", default=None)
    p.add_argument("--ue-id", type=int, default=1)
    p.add_argument("--ru-id", type=int, default=1)
    p.add_argument("--no-pickle", dest="allow_pickle", action="store_false", default=True)
    p.add_argument("--repeat-per-step", type=int, default=1,
                   help="--channel-mode cir/precomputed only: hold each CIR step for "
                        "cir-interval * repeat-per-step seconds (default 1, so --cir-interval alone "
                        "is the per-step dwell time)")
    return p


def main():
    args = build_parser().parse_args()
    if args.noise_power_dbfs is not None and not np.isfinite(args.noise_power_dbfs):
        log.error("--noise-power-dbfs must be finite")
        return 2
    if args.snr_db is not None and not np.isfinite(args.snr_db):
        log.error("--snr-db must be finite")
        return 2
    if args.host is not None:
        args.connect_host = args.bind_host = args.host
    if args.channel_mode == "aodt":
        return run_aodt(args)
    if args.channel_mode in ("precomputed", "cir"):
        try:
            taps, steps = load_taps_file(args) if args.cir_source == "taps" else load_local_cirs(args)
        except (OSError, ValueError, KeyError, pickle.UnpicklingError) as exc:
            log.error("Failed to load CIR: %s", exc)
            return 1
        fixed_cir = args.freeze_first_cir or args.fixed_cir_step is not None
        if args.fixed_cir_step is not None:
            try:
                fixed_index = steps.index(args.fixed_cir_step)
            except ValueError:
                log.error("Fixed CIR time_step %s not found; available steps: %s",
                          args.fixed_cir_step, steps)
                return 2
            taps, steps = [taps[fixed_index]], [steps[fixed_index]]
            log.info("Permanently fixed to CIR time_step=%s", steps[0])
        elif args.freeze_first_cir:
            taps, steps = taps[:1], steps[:1]
        first_energy = np.sqrt(np.vdot(taps[0].astype(np.complex128), taps[0].astype(np.complex128)).real)
        log.info("Loaded %d CIR steps; first step=%s, first tap count=%d, first tap energy=%.6g (%.2f dB)",
                 len(steps), steps[0], len(taps[0]), first_energy,
                 20.0 * np.log10(first_energy) if first_energy > 0 else float("-inf"))
        return run_bridge(args, taps, steps, update_taps=not fixed_cir)
    return run_bridge(args)


if __name__ == "__main__":
    sys.exit(main())
