#!/usr/bin/env python3
"""Sample-synchronous OCUDU gNB <-> OAI UE bridge for live AODT CIRs.

Unlike ``zmq_bridge_ocudu_oai_bridge.py``, this bridge never uses host wall
clock time to select a CIR.  Each direction counts every IQ sample it forwards,
starting with the first ZMQ message.  Both radios' ZMQ drivers zero-fill
timestamp gaps and drop late bursts without advancing their sample counters,
and neither receiver discards samples, so stream sample ``c`` is the
transmitter's baseband timestamp minus its stream start time.  DL sample ``c``
and UL sample ``c`` therefore both correspond to gNB timestamp
``gnb_start + c``; OCUDU starts its ZMQ streams on a 10 ms frame boundary.

The optional trigger file selects one common origin ``S``: the first frame
boundary that neither direction has reached.  AODT step ``k`` occupies stream
samples ``[S + k * N, S + (k + 1) * N)`` in both directions, and a ZMQ message
which straddles a boundary is split before FIR processing.  Step 0 is held
until the trigger appears.

The ZMQ radio protocol carries IQ only, so this time base holds only if the
bridge has forwarded both streams from their first sample: restart the gNB and
UE whenever the bridge restarts.
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from pathlib import Path

import numpy as np
import zmq

import zmq_bridge_ocudu_oai_bridge as legacy


log = logging.getLogger("ocudu_oai_sample_sync_bridge")
WAIT_WARNING_SECONDS = 5.0


class ScenarioExhausted(RuntimeError):
    """The sample clock advanced past the final non-looping AODT step."""


def sample_segments(start_sample: int, sample_count: int, step_samples: int,
                    origin: int | None = 0):
    """Yield ``(logical_step, offset, count)`` split exactly at sample edges.

    Samples before ``origin`` hold step 0; ``origin=None`` holds every sample.
    """
    if start_sample < 0 or sample_count < 0 or step_samples <= 0:
        raise ValueError("sample positions/counts must be non-negative and step_samples positive")
    if origin is not None and origin < 0:
        raise ValueError("origin must be non-negative")
    offset = 0
    while offset < sample_count:
        position = start_sample + offset
        if origin is None:
            logical_step, boundary = 0, start_sample + sample_count
        elif position < origin:
            logical_step, boundary = 0, origin
        else:
            logical_step = (position - origin) // step_samples
            boundary = origin + (logical_step + 1) * step_samples
        count = min(sample_count - offset, boundary - position)
        yield logical_step, offset, count
        offset += count


def scenario_interval(yaml_file) -> float | None:
    """Return the scenario YAML's ``sim_interval`` in seconds, if it has one."""
    path = Path(yaml_file).expanduser()
    if not path.is_file():
        return None
    try:
        import yaml
        stack = [yaml.safe_load(path.read_text())]
    except Exception as exc:
        log.warning("Could not read sim_interval from %s: %s", path, exc)
        return None
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            if "sim_interval" in node:
                return float(node["sim_interval"])
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)
    return None


def save_applied_taps(path, taps_by_step, samp_rate):
    """Atomically write applied taps in the legacy ``--save-applied-taps`` layout."""
    if not taps_by_step:
        return
    output = Path(path).expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    steps = sorted(taps_by_step)
    matrix = np.zeros((len(steps), max(len(taps_by_step[step]) for step in steps)),
                      dtype=np.complex64)
    lengths = np.empty(len(steps), dtype=np.int32)
    for row, step in enumerate(steps):
        matrix[row, :len(taps_by_step[step])] = taps_by_step[step]
        lengths[row] = len(taps_by_step[step])
    temporary = output.with_suffix(output.suffix + ".tmp")
    with temporary.open("wb") as handle:
        np.savez(handle, time_steps=np.asarray(steps, dtype=np.int32),
                 tap_lengths=lengths, taps=matrix, sample_rate_hz=float(samp_rate))
    temporary.replace(output)
    log.info("Saved %d applied AODT CIR steps to %s", len(steps), output)


class ContinuousHistoryChannel(legacy.StreamingChannel):
    """StreamingChannel whose FIR input history survives a tap-length increase.

    The base class keeps only ``len(taps) - 1`` input samples, so switching to
    a longer CIR would convolve its late taps with zeros instead of the real
    preceding IQ.  This keeps ``history_samples`` real input samples at all
    times and rebuilds the base-class history from them on every switch.
    """

    def __init__(self, taps, history_samples, **kwargs):
        self._input_tail = np.zeros(max(0, int(history_samples)), dtype=np.complex64)
        super().__init__(taps, **kwargs)

    def set_taps(self, taps):
        super().set_taps(taps)
        with self._lock:
            need = self._taps.size - 1
            if need > self._input_tail.size:
                self._input_tail = np.concatenate([
                    np.zeros(need - self._input_tail.size, dtype=np.complex64), self._input_tail])
            self._history = self._input_tail[self._input_tail.size - need:].copy()

    def process(self, frame):
        out = super().process(frame)
        samples = np.frombuffer(memoryview(frame), dtype=np.complex64)
        with self._lock:
            keep = self._input_tail.size
            if samples.size >= keep:
                self._input_tail = samples[samples.size - keep:].copy()
            elif samples.size:
                self._input_tail = np.concatenate([self._input_tail[samples.size:], samples])
        return out


class AodtTapTimeline:
    """Thread-safe, sample-clock-indexed AODT tap cache.

    The worker is allowed to run ahead, but a bridge never crosses a sample
    boundary until the matching CIR is cached.  This turns slow AODT service
    into normal ZMQ backpressure instead of a silent old-tap interval.
    """

    def __init__(self, client, allocation, args, temporal_cls, max_step):
        self.client = client
        self.allocation = allocation
        self.args = args
        self.temporal_cls = temporal_cls
        self.max_step = max_step
        self._taps: dict[int, np.ndarray] = {}
        self._target = 0
        self._next = 0
        self._closed = False
        self._error: Exception | None = None
        self._condition = threading.Condition()
        self._worker = threading.Thread(
            target=self._run, name="aodt-cir-prefetch", daemon=True)

    def _physical_step(self, logical_step: int) -> int:
        if logical_step <= self.max_step:
            return logical_step
        if self.args.loop_scenario:
            return logical_step % (self.max_step + 1)
        if self.args.stop_when_cir_exhausted:
            raise ScenarioExhausted(
                f"sample clock reached AODT step {logical_step}, beyond final step {self.max_step}")
        return self.max_step

    def _fetch(self, step: int) -> np.ndarray:
        self.client.get_cirs(
            self.allocation, batch_index=0, temporal_index=self.temporal_cls(step))
        values = self.client.to_numpy(self.allocation, step, self.args.ru_index, "values")
        delays = self.client.to_numpy(self.allocation, step, self.args.ru_index, "delays")
        return legacy._taps_from_cir(values[0], delays[0], self.args)

    def prime(self) -> np.ndarray:
        """Load step 0 before accepting radio traffic; failure is explicit."""
        taps = self._fetch(0)
        with self._condition:
            self._taps[0] = taps
            self._next = 1
        return taps

    def start(self):
        self.request(0)
        self._worker.start()

    def close(self):
        with self._condition:
            self._closed = True
            self._condition.notify_all()
        if self._worker.is_alive():
            self._worker.join(timeout=2)

    def snapshot(self, steps) -> dict[int, np.ndarray]:
        with self._condition:
            return {step: self._taps[step] for step in steps if step in self._taps}

    def request(self, logical_step: int):
        try:
            physical = self._physical_step(logical_step)
        except ScenarioExhausted:
            return
        target = min(self.max_step, physical + self.args.aodt_prefetch_steps)
        with self._condition:
            if target > self._target:
                self._target = target
                self._condition.notify_all()

    def get(self, logical_step: int, stop_event: threading.Event,
            label: str = "bridge") -> tuple[int, np.ndarray]:
        physical = self._physical_step(logical_step)
        self.request(logical_step)
        with self._condition:
            started = time.monotonic()
            next_warning = started + WAIT_WARNING_SECONDS
            while physical not in self._taps:
                if self._error is not None:
                    raise RuntimeError(f"AODT CIR prefetch stopped: {self._error}") from self._error
                if stop_event.is_set() or self._closed:
                    raise InterruptedError("bridge stopped while waiting for an AODT CIR")
                self._condition.wait(timeout=0.1)
                now = time.monotonic()
                if now >= next_warning:
                    log.warning("%s has waited %.0f s for AODT CIR step %d",
                                label, now - started, physical)
                    next_warning = now + WAIT_WARNING_SECONDS
            return physical, self._taps[physical]

    def _run(self):
        last_warning = 0.0
        while True:
            with self._condition:
                while self._next > self._target and not self._closed:
                    self._condition.wait()
                if self._closed:
                    return
                step = self._next
            try:
                taps = self._fetch(step)
            except RuntimeError as exc:
                # A retry holds the exact sample boundary; it never substitutes
                # CIR[k-1] for CIR[k].
                now = time.monotonic()
                if now - last_warning >= WAIT_WARNING_SECONDS:
                    log.warning("AODT CIR step %d fetch failed: %s; retrying", step, exc)
                    last_warning = now
                with self._condition:
                    self._condition.wait(timeout=self.args.aodt_fetch_retry_seconds)
                continue
            except Exception as exc:
                # Anything else will not succeed on retry; fail the waiting
                # bridge instead of leaving it blocked on a dead worker.
                log.exception("AODT CIR step %d fetch failed permanently", step)
                with self._condition:
                    self._error = exc
                    self._condition.notify_all()
                return
            last_warning = 0.0
            with self._condition:
                self._taps[step] = taps
                self._next += 1
                self._condition.notify_all()
            log.info("Prefetched AODT CIR step %d", step)


class SampleSynchronousBridge(legacy.PurePythonBridge):
    """The existing socket topology with a shared stream-sample CIR clock.

    The poll thread assigns each payload its stream position; DSP runs on the
    inherited per-direction executor, one message in flight per direction.
    """

    def __init__(self, args, timeline, initial_taps):
        super().__init__(args, initial_taps)
        self.timeline = timeline
        self.step_samples = args.aodt_step_samples
        self.origin_align = args.aodt_origin_align_samples
        self.frame_samples = max(1, int(round(args.samp_rate / 100)))
        self._trigger_path = Path(args.step_trigger_file) if args.step_trigger_file else None
        self._origin = None if self._trigger_path else 0
        self._stats_lock = threading.Lock()
        self.applied_steps = {0}
        self._pending_pairs: dict[int, dict[str, tuple[int, float]]] = {}
        self._pair_stats: list[tuple[int, float]] = []
        self._switch_count = 0
        self._max_deviation = 0
        self._replace_channels(args, initial_taps)
        for direction in self.directions:
            direction.stream_samples = 0
            direction.applied_step = 0
            direction.cir_wait_seconds = 0.0
        if self._trigger_path:
            if self._trigger_path.exists():
                log.warning("Trigger file %s already exists; AODT step 0 starts with the first IQ sample",
                            self._trigger_path)
            log.info("Holding AODT step 0 until trigger file appears: %s", self._trigger_path)

    def _replace_channels(self, args, taps):
        """Swap in FIR channels whose input history survives tap-length changes."""
        delay_samples = round(args.ue_delay_us * 1e-6 * args.samp_rate)
        history = max(args.max_channel_taps, len(taps)) + delay_samples
        for seed_offset, direction in enumerate(self.directions):
            noise_enabled = args.noise_direction in ("both", direction.name.lower())
            if direction.channel is not None:
                direction.channel.close()
            direction.channel = ContinuousHistoryChannel(
                taps, history, delay_samples=delay_samples,
                noise_power_dbfs=args.noise_power_dbfs if noise_enabled else None,
                snr_db=args.snr_db if noise_enabled else None,
                noise_seed=None if args.noise_seed is None else args.noise_seed + seed_offset)

    def _start_clock_if_triggered(self):
        if not self._trigger_path.exists():
            return
        # Every dispatched message ends at or before ``reached``, so the origin
        # is still ahead of both directions and neither has to rewind.
        reached = max(direction.stream_samples for direction in self.directions)
        self._origin = -(-reached // self.origin_align) * self.origin_align
        self.timeline.request(0)
        log.info("Trigger received; AODT sample origin is stream sample %d (frame %d); "
                 "DL had forwarded %d and UL %d samples",
                 self._origin, self._origin // self.frame_samples,
                 self.dl.stream_samples, self.ul.stream_samples)

    def _dispatch(self, direction, payload):
        if self._origin is None:
            self._start_clock_if_triggered()
        sample_count = len(payload) // legacy.COMPLEX64_BYTES
        start = direction.stream_samples
        direction.stream_samples += sample_count
        direction.pending_samples = sample_count
        direction.future = self.executor.submit(
            self._process_payload, direction, payload, start, self._origin)
        direction.future.add_done_callback(self._wake_when_done)

    def _process_payload(self, direction, payload, start, origin):
        if len(payload) % legacy.COMPLEX64_BYTES:
            raise ValueError(
                f"IQ payload has {len(payload)} bytes; expected a multiple of "
                f"{legacy.COMPLEX64_BYTES}")
        sample_count = len(payload) // legacy.COMPLEX64_BYTES
        parts = []
        payload_view = memoryview(payload)
        for logical_step, offset, count in sample_segments(
                start, sample_count, self.step_samples, origin):
            started = time.monotonic()
            physical_step, taps = self.timeline.get(logical_step, self.stop_event, direction.name)
            direction.cir_wait_seconds += time.monotonic() - started
            if physical_step != direction.applied_step:
                direction.channel.set_taps(taps)
                direction.applied_step = physical_step
                self._record_switch(direction, logical_step, physical_step, start + offset, origin)
            begin = offset * legacy.COMPLEX64_BYTES
            end = (offset + count) * legacy.COMPLEX64_BYTES
            parts.append(direction.channel.process(payload_view[begin:end]))
        return parts[0] if len(parts) == 1 else b"".join(parts)

    def _record_switch(self, direction, logical_step, physical_step, actual, origin):
        planned = origin + logical_step * self.step_samples
        now = time.monotonic()
        log.info("%s switched to AODT CIR step %d at stream sample %d "
                 "(planned %d, deviation %+d; frame %d + %d samples)",
                 direction.name, physical_step, actual, planned, actual - planned,
                 actual // self.frame_samples, actual % self.frame_samples)
        with self._stats_lock:
            self.applied_steps.add(physical_step)
            self._switch_count += 1
            self._max_deviation = max(self._max_deviation, abs(actual - planned))
            pair = self._pending_pairs.setdefault(logical_step, {})
            pair[direction.name] = (actual, now)
            if len(pair) < len(self.directions):
                return
            del self._pending_pairs[logical_step]
            (dl_sample, dl_time), (ul_sample, ul_time) = pair["DL"], pair["UL"]
            skew, gap_ms = dl_sample - ul_sample, (dl_time - ul_time) * 1e3
            self._pair_stats.append((skew, gap_ms))
        log.info("AODT CIR step %d active on DL and UL: sample skew %+d, wall-clock gap %+.1f ms",
                 physical_step, skew, gap_ms)

    def _log_sync_summary(self):
        with self._stats_lock:
            pairs = list(self._pair_stats)
            switches, max_deviation = self._switch_count, self._max_deviation
        if not switches:
            return
        waits = ", ".join(f"{d.name} {d.cir_wait_seconds:.1f} s" for d in self.directions)
        if not pairs:
            log.info("Sync summary: %d tap switches, max |actual-planned| %d samples; CIR wait %s",
                     switches, max_deviation, waits)
            return
        skew = np.array([pair[0] for pair in pairs])
        gap = np.array([pair[1] for pair in pairs])
        log.info("Sync summary: %d tap switches, max |actual-planned| %d samples; "
                 "DL/UL sample skew max |%d| over %d steps; DL-UL wall-clock gap "
                 "mean %+.1f ms, max |%.1f| ms, std %.1f ms; CIR wait %s",
                 switches, max_deviation, int(np.abs(skew).max()), len(pairs),
                 gap.mean(), np.abs(gap).max(), gap.std(), waits)

    def _finish_dsp(self):
        for direction in self.directions:
            future = direction.future
            if future is None or not future.done():
                continue
            direction.future = None
            try:
                reply = future.result()
            except ScenarioExhausted as exc:
                log.info("%s; stopping bridge", exc)
                self.stop()
                return
            except InterruptedError:
                return
            except Exception as exc:
                # An empty or short reply would shift this stream against the
                # radios' timestamps for the rest of the run, so stop instead.
                log.error("%s channel processing failed: %s; stopping bridge",
                          direction.name, exc, exc_info=exc)
                self.stop()
                return
            self._send_reply(direction, reply, direction.pending_samples)
            direction.pending_samples = 0

    def run(self):
        log.info("Sample-synchronous AODT bridge: %d samples/CIR step (%.9g s at %.9g sps), "
                 "origin aligned to %d samples",
                 self.step_samples, self.step_samples / self.args.samp_rate,
                 self.args.samp_rate, self.origin_align)
        log.info("Topology: DL gNB %s -> UE %s; UL UE %s -> gNB %s",
                 self.gnb_tx_address, self.ue_rx_address,
                 self.ue_tx_address, self.gnb_rx_address)
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
                if self.stop_event.is_set():
                    break
                for direction in self.directions:
                    if not direction.waiting and events.get(direction.downstream, 0) & zmq.POLLIN:
                        request = direction.downstream.recv(copy=False)
                        direction.upstream.send(request, copy=False)
                        direction.waiting = True
                        direction.request_started = time.monotonic()
                    if (direction.waiting and direction.future is None
                            and events.get(direction.upstream, 0) & zmq.POLLIN):
                        self._dispatch(direction, direction.upstream.recv(copy=False))
                now = time.monotonic()
                if now - last_report >= self.args.stats_interval:
                    text = ", ".join(
                        f"{d.name}={d.messages} msgs/{d.stream_samples} samples, step={d.applied_step}"
                        for d in self.directions)
                    origin = ("waiting for trigger" if self._origin is None
                              else f"origin={self._origin}")
                    log.info("Traffic: %s; %s", text, origin)
                    last_report = now
        finally:
            if self.executor is not None:
                self.executor.shutdown(wait=True, cancel_futures=True)
            self._log_sync_summary()
            for direction in self.directions:
                if direction.channel is not None:
                    direction.channel.close()
                direction.downstream.close(0)
                direction.upstream.close(0)
            self.ctx.destroy(linger=0)
            self.wake_read.close()
            self.wake_write.close()


def run_aodt_sample_sync(args):
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
    allocation = timeline = bridge = None
    try:
        client.start(yaml_path.read_text())
        status = client.get_status()
        if not status["scenario_loaded"]:
            raise RuntimeError("AODT server did not load the scenario")
        args.is_slot_symbol_mode = status["is_slot_symbol_mode"]
        if args.is_slot_symbol_mode:
            log.warning("AODT scenario is in slot/symbol mode; check that --aodt-step-samples "
                        "(%d) is the duration of one AODT slot", args.aodt_step_samples)
        temporal_cls = dt_client.SlotIndex if args.is_slot_symbol_mode else dt_client.TimeStepIndex
        max_step = max(0, status["num_slots_or_timesteps_per_batch"] - 1)
        allocation = client.allocate_cirs_memory(
            [args.ru_index], [[args.ue_aodt_index]], num_time_steps=1)
        timeline = AodtTapTimeline(client, allocation, args, temporal_cls, max_step)
        initial_taps = timeline.prime()
        log.info("Loaded AODT CIR step 0 before accepting IQ traffic")
        bridge = SampleSynchronousBridge(args, timeline, initial_taps)
        signal.signal(signal.SIGINT, lambda *_: bridge.stop())
        signal.signal(signal.SIGTERM, lambda *_: bridge.stop())
        timeline.start()
        bridge.run()
        return 0
    except RuntimeError as exc:
        log.error("AODT sample-synchronous mode failed: %s", exc)
        return 1
    finally:
        if bridge is not None:
            bridge.stop()
        if timeline is not None:
            timeline.close()
            if bridge is not None and args.save_applied_taps:
                try:
                    save_applied_taps(args.save_applied_taps,
                                      timeline.snapshot(bridge.applied_steps), args.samp_rate)
                except OSError as exc:
                    log.error("Could not save applied taps: %s", exc)
        if allocation is not None:
            client.deallocate_cirs_memory(allocation)
        client.stop_server_log_streaming()


def resolve_step_samples(args) -> int:
    """Return samples per AODT step, warning when it disagrees with the scenario."""
    yaml_interval = scenario_interval(args.yaml_file) if args.yaml_file else None
    if args.aodt_step_samples is not None:
        step_samples = args.aodt_step_samples
    else:
        interval = args.cir_interval if args.cir_interval is not None else yaml_interval
        if interval is None:
            raise ValueError("pass --cir-interval or --aodt-step-samples; "
                             "the scenario YAML has no sim_interval")
        step_samples = int(round(interval * args.samp_rate))
    if yaml_interval is not None and step_samples > 0:
        expected = int(round(yaml_interval * args.samp_rate))
        if step_samples != expected:
            log.warning("%d samples/CIR step is %.9g s of radio time, but the scenario sim_interval "
                        "is %.9g s (%d samples); AODT scenario time will advance %.3gx as fast "
                        "as radio time", step_samples, step_samples / args.samp_rate,
                        yaml_interval, expected, expected / step_samples)
    return step_samples


def build_parser():
    parser = legacy.build_parser()
    parser.description = "Sample-synchronous single-UE OCUDU/OAI AODT ZMQ channel bridge"
    parser.set_defaults(cir_interval=None)
    parser.add_argument("--aodt-step-samples", type=int, default=None,
                        help="exact IQ samples per AODT time step; default is rounded "
                             "--cir-interval * --samp-rate, where --cir-interval defaults to "
                             "the scenario YAML's sim_interval")
    parser.add_argument("--aodt-origin-align-samples", type=int, default=None,
                        help="round the trigger origin up to a multiple of this many stream "
                             "samples; default is one 10 ms NR frame (--samp-rate / 100)")
    parser.add_argument("--aodt-prefetch-steps", type=int, default=2,
                        help="number of future AODT CIR steps to request before their sample boundary")
    parser.add_argument("--aodt-fetch-retry-seconds", type=float, default=0.1,
                        help="retry delay when AODT has not produced a requested CIR")
    return parser


def main():
    args = build_parser().parse_args()
    if args.host is not None:
        args.connect_host = args.bind_host = args.host
    if args.channel_mode != "aodt":
        log.error("This bridge is dedicated to --channel-mode aodt")
        return 2
    try:
        args.aodt_step_samples = resolve_step_samples(args)
    except ValueError as exc:
        log.error("%s", exc)
        return 2
    if args.aodt_step_samples <= 0:
        log.error("--aodt-step-samples must be a positive integer")
        return 2
    if args.aodt_origin_align_samples is None:
        args.aodt_origin_align_samples = max(1, int(round(args.samp_rate / 100)))
    if args.aodt_origin_align_samples <= 0:
        log.error("--aodt-origin-align-samples must be a positive integer")
        return 2
    if args.aodt_step_samples % args.aodt_origin_align_samples:
        log.warning("--aodt-step-samples %d is not a multiple of the %d-sample origin alignment; "
                    "only the first AODT boundary is frame-aligned",
                    args.aodt_step_samples, args.aodt_origin_align_samples)
    if args.aodt_prefetch_steps < 0:
        log.error("--aodt-prefetch-steps must be non-negative")
        return 2
    if args.aodt_fetch_retry_seconds <= 0:
        log.error("--aodt-fetch-retry-seconds must be positive")
        return 2
    return run_aodt_sample_sync(args)


if __name__ == "__main__":
    raise SystemExit(main())
