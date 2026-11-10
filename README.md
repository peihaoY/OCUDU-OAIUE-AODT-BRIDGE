# OCUDU ↔ OAI UE ↔ AODT Bridge

A pure-Python ZMQ channel bridge between an **OCUDU gNB** and an **OAI `nr-uesoftmodem` UE**. It applies channel impulse responses (CIRs) from **NVIDIA Aerial Omniverse Digital Twin (AODT)** to the IQ streams in both directions (DL and UL).

```
 OCUDU gNB  --TX-->  [ bridge: AODT CIR FIR + AWGN ]  --RX-->  OAI UE
 OCUDU gNB  <--RX--  [ bridge: AODT CIR FIR + AWGN ]  <--TX--  OAI UE
```

## Files

| File | Description |
|------|-------------|
| `zmq_bridge_ocudu_oai_bridge.py` | Base bridge. It is a single pyzmq poll loop. Channel modes: `ideal`, `static`, `aodt`, `precomputed`, `cir`. It also handles optional AWGN, path loss and power normalization. |
| `zmq_bridge_ocudu_oai_sample_sync_bridge.py` | Sample-synchronous bridge for live AODT. It switches CIR steps by cumulative IQ sample count instead of wall-clock time. DL and UL share one time origin, aligned to a 10 ms frame boundary. It imports the base bridge as a library. |
| `bridge_sync` | Launcher script for the sample-synchronous bridge. It is configured through environment variables. |
| `msu_1ru_1ue.yaml` | Example AODT scenario: 1 RU and 1 moving UE at MSU Engineering, 3489.42 MHz, `sim_interval` = 0.3 s. |

## How sample synchronization works

- Each direction counts the IQ samples it forwards, starting from the first ZMQ message. Stream sample `c` maps to gNB timestamp `gnb_start + c` in both DL and UL.
- AODT step `k` covers samples `[S + k·N, S + (k+1)·N)`. `N = sim_interval × sample_rate`, and `S` is the common origin chosen when the trigger fires. If a ZMQ message crosses a step boundary, the bridge splits it before FIR filtering.
- Step 0 is held until the trigger file appears. This gives the UE time to scan, run random access and set up its PDU session.
- **Restart the gNB and UE whenever the bridge restarts.** The time base is only valid when the bridge has forwarded both streams from their first sample.

## Requirements

- Python 3 with `numpy`, `pyzmq`, `scipy`, `PyYAML`
- AODT `dt_client` Python bindings (built under `aodt_source/client/build`)
- A running AODT server (default `localhost:50051`)
- OCUDU gNB and OAI UE built with ZMQ radio support

## Layout

`bridge_sync` expects this directory layout:

```
<workdir>/
├── bridge_sync
└── aodt_source/
    ├── .venv/bin/python3
    └── client/
        ├── build/                 # dt_client bindings
        └── examples/
            ├── zmq_bridge_ocudu_oai_bridge.py
            ├── zmq_bridge_ocudu_oai_sample_sync_bridge.py
            └── msu_1ru_1ue.yaml
```

## Usage

1. Start the AODT server.
2. Start the bridge:
   ```bash
   ./bridge_sync
   ```
3. Start the gNB and the UE.
4. When the UE is attached, start the channel variation:
   ```bash
   touch /tmp/phyan100_start_channel_variation
   ```
   To trigger it automatically instead, set `BRIDGE_INITIAL_HOLD_SECONDS=<seconds>`.

`bridge_sync` passes any extra arguments through to the bridge, for example `./bridge_sync --ue-delay-us 1.0`.

### Environment variables

| Variable | Default | Meaning |
|----------|---------|---------|
| `BRIDGE_CPUS` | `8,10,12,14,16,18` | CPU set for `taskset` |
| `BRIDGE_SERVER_ADDRESS` | `localhost:50051` | AODT server |
| `BRIDGE_YAML_FILE` | `aodt_source/client/examples/msu_1ru_1ue.yaml` | Scenario YAML |
| `BRIDGE_SAMPLE_RATE_HZ` | `122880000` | IQ sample rate |
| `BRIDGE_CIR_INTERVAL` / `BRIDGE_AODT_STEP_SAMPLES` | *(from YAML)* | Overrides the step length, in seconds or in samples |
| `BRIDGE_ORIGIN_ALIGN_SAMPLES` | *(one 10 ms frame)* | Alignment of the common origin |
| `BRIDGE_PREFETCH_STEPS` | `2` | Number of AODT steps fetched ahead |
| `BRIDGE_NOISE_POWER_DBFS` | `-65` | Absolute noise floor. Set it to empty to use `BRIDGE_SNR_DB` instead. |
| `BRIDGE_SNR_DB` | `30` | Relative SNR. Used only when the noise floor is empty. `off` disables noise. |
| `BRIDGE_NOISE_DIRECTION` | `both` | `both`, `dl` or `ul` |
| `BRIDGE_PATH_LOSS_DB` | `0.0` | Extra path loss |
| `BRIDGE_NORMALIZE_POWER` | `off` | Normalizes each step to unit energy. This erases path loss. |
| `BRIDGE_STOP_WHEN_CIR_EXHAUSTED` | `on` | Stops the bridge after the last step |
| `BRIDGE_LOOP_SCENARIO` | `off` | Loops back to step 0 instead of stopping |
| `BRIDGE_STEP_TRIGGER_FILE` | `/tmp/phyan100_start_channel_variation` | Trigger file |
| `BRIDGE_INITIAL_HOLD_SECONDS` | `off` | Fires the trigger automatically after this many seconds |
| `BRIDGE_SAVE_APPLIED_TAPS` | *(empty)* | Path where the applied taps are saved |
| `GNB_TX/RX_ENDPOINT`, `UE_TX/RX_ENDPOINT` | `ipc:///tmp/phyan100_*.ipc` | ZMQ endpoints |
