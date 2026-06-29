import asyncio
import websockets
import sys
import io
import json
import time
import csv
import numpy as np
from collections import deque
from scipy.signal import butter, filtfilt, iirnotch

# ── stdout UTF-8 (keeps emoji / box-drawing chars safe on Windows too) ────────
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# ── Server config ─────────────────────────────────────────────────────────────
HOST = "0.0.0.0"
PORT = 1234
TELEMETRY_PORT = 1236  # Port that Telemetry Viewer will listen on

# ── EEG / FFT config ──────────────────────────────────────────────────────────
EEG_FS     = 250          # sample rate set on the ESP (Hz)
FFT_WINDOW = 250          # 1 second of data for FFT

# We keep a 2-second rolling buffer of raw data to apply filters cleanly
# without edge artifacts on the newest data.
raw_buffer = deque(maxlen=EEG_FS * 2) 

# ── Globals (must be module-level so handler() can write to them) ─────────────
csv_file   = None         # file handle  (assigned in main before server starts)
csv_writer = None         # csv.writer   (assigned in main before server starts)

connected_clients = set()
telemetry_writer = None   # Will hold the TCP connection to Telemetry Viewer

# Per-second rate stats
sample_count      = 0
last_sample_count = 0
packet_count      = 0
last_print        = time.time()
last_fft_time     = 0.0

# ── DSP helpers ───────────────────────────────────────────────────────────────
def apply_realtime_filters(raw_data_array: np.ndarray) -> np.ndarray:
    """Applies a 50Hz Notch + 1-40Hz Bandpass filter."""
    if len(raw_data_array) < 50: 
        return raw_data_array  # Too short to filter

    # 1. 50Hz Notch Filter
    b_notch, a_notch = iirnotch(50.0, 30.0, EEG_FS)
    notched = filtfilt(b_notch, a_notch, raw_data_array)
    
    # 2. 1-40Hz Bandpass Filter (Butterworth)
    # This acts much better than a Moving Average filter! A moving average 
    # smears out important high-frequency brainwaves like Beta/Gamma.
    b_band, a_band = butter(N=4, Wn=[1, 40], btype="bandpass", fs=EEG_FS)
    filtered = filtfilt(b_band, a_band, notched)
    
    return filtered

def compute_eeg_bands(filtered_eeg: np.ndarray):
    # FFT calculation
    fft  = np.fft.rfft(filtered_eeg)
    freqs = np.fft.rfftfreq(len(filtered_eeg), d=1.0 / EEG_FS)
    power = np.abs(fft) ** 2
    
    bands = {
        "delta": (0.5, 4),
        "theta": (4,   8),
        "alpha": (8,  13),
        "beta":  (13, 30),
        "gamma": (30, 40),
    }
    
    band_powers = {
        name: float(np.sum(power[(freqs >= f1) & (freqs < f2)]))
        for name, (f1, f2) in bands.items()
    }
    
    return freqs.tolist(), power.tolist(), band_powers


# ── Telemetry Viewer TCP Client ───────────────────────────────────────────────
async def telemetry_viewer_client():
    global telemetry_writer
    while True:
        try:
            # Connect TO Telemetry Viewer (which is acting as the server)
            reader, writer = await asyncio.open_connection('127.0.0.1', TELEMETRY_PORT)
            print(f"[+] Connected to Telemetry Viewer on port {TELEMETRY_PORT}!", flush=True)
            telemetry_writer = writer
            
            # Wait until connection is lost
            while not writer.is_closing():
                await asyncio.sleep(1)
                
        except (ConnectionRefusedError, OSError):
            # Telemetry Viewer not running or not listening yet
            if telemetry_writer is not None:
                print("[-] Disconnected from Telemetry Viewer.", flush=True)
            telemetry_writer = None
            await asyncio.sleep(2) # Retry every 2 seconds
        except Exception as e:
            telemetry_writer = None
            await asyncio.sleep(2)


# ── WebSocket handler ─────────────────────────────────────────────────────────
async def handler(websocket):
    global sample_count, last_sample_count, packet_count
    global last_print, last_fft_time
    global csv_writer  # referenced (not assigned) – must be declared global
    global telemetry_writer

    client_ip = websocket.remote_address
    connected_clients.add(websocket)
    print(f"[+] Client connected: {client_ip}", flush=True)

    try:
        async for raw_message in websocket:

            try:
                data = json.loads(raw_message)
                messages = data if isinstance(data, list) else [data]
            except (json.JSONDecodeError, AttributeError):
                continue

            new_raw_samples = []

            for msg in messages:
                if not isinstance(msg, dict): continue
                dev = msg.get("dev", "")

                # ── EXG raw data from ESP32 ───────────────────────────────────
                if dev == "EXG":
                    raw_val = msg.get("v")
                    esp_ts  = msg.get("t", 0)

                    if raw_val is None: continue

                    sample_count += 1
                    packet_count += 1
                    
                    # Store raw data for CSV and filtering
                    raw_buffer.append(raw_val)
                    new_raw_samples.append(raw_val)

                    if csv_writer is not None:
                        csv_writer.writerow([sample_count, time.time(), esp_ts, raw_val])

                # ── Forward IMU or other sensors directly ─────────────────────
                else:
                    for client in connected_clients.copy():
                        if client != websocket:
                            try: await client.send(json.dumps(msg))
                            except Exception: pass

            # ── Real-Time Filtering & Dashboard Broadcast ─────────────────────
            # If we just received new samples, filter the whole buffer and 
            # send ONLY the newly filtered samples to the dashboard.
            if len(new_raw_samples) > 0 and len(raw_buffer) >= 50:
                raw_array = np.array(raw_buffer, dtype=np.float32)
                raw_array -= raw_array.mean() # Center around 0
                
                # Apply Notch + Bandpass filter
                filtered_array = apply_realtime_filters(raw_array)
                
                # Get the filtered versions of the newly arrived samples
                num_new = len(new_raw_samples)
                recent_filtered = filtered_array[-num_new:].tolist()
                
                # Broadcast the CLEAN signal to the dashboard.html 
                # (Dashboard expects "dev": "EEG", "samples": [...])
                eeg_msg = json.dumps({"dev": "EEG", "samples": recent_filtered})
                for client in connected_clients.copy():
                    if client != websocket:
                        try: await client.send(eeg_msg)
                        except Exception: pass

                # ── Broadcast to Telemetry Viewer via TCP ─────────────────────
                if telemetry_writer is not None:
                    # Telemetry Viewer expects comma-separated values ending in newline
                    tcp_lines = []
                    for r_val, f_val in zip(new_raw_samples, recent_filtered):
                        tcp_lines.append(f"{r_val},{f_val:.2f}\n")
                    
                    tcp_payload = "".join(tcp_lines).encode("utf-8")
                    
                    try:
                        telemetry_writer.write(tcp_payload)
                    except Exception:
                        telemetry_writer = None # Will trigger reconnect loop

            # ── FFT (Once per second) ─────────────────────────────────────────
            if len(raw_buffer) >= FFT_WINDOW and (time.time() - last_fft_time > 1.0):
                last_fft_time = time.time()
                
                # Take the last 1 second (250 samples) of the FILTERED buffer
                raw_array = np.array(raw_buffer, dtype=np.float32)
                raw_array -= raw_array.mean()
                filtered_array = apply_realtime_filters(raw_array)
                
                # Grab just the last second for FFT
                fft_window_data = filtered_array[-FFT_WINDOW:]
                
                freqs, power, bands = compute_eeg_bands(fft_window_data)
                
                total = sum(bands.values())
                alpha_ratio = bands["alpha"] / total if total > 0 else 0.0
                beta_alpha_ratio = bands["beta"] / bands["alpha"] if bands["alpha"] > 0 else 0.0
                
                print(
                    f"[FFT] delta={bands['delta']:.0f}  theta={bands['theta']:.0f}  "
                    f"alpha={bands['alpha']:.0f}  beta={bands['beta']:.0f}  "
                    f"gamma={bands['gamma']:.0f}  |  focus(b/a)={beta_alpha_ratio:.2f}",
                    flush=True,
                )
                
                # 1. Send FFT spectrum data for the line chart
                fft_msg = json.dumps({
                    "dev": "FFT", 
                    "freq": freqs[:len(freqs)//2], # Only send up to Nyquist
                    "power": power[:len(power)//2]
                })
                
                # 2. Send Band Power data for the bar chart and focus score
                band_msg = json.dumps({
                    "dev": "EEG_BANDS", 
                    **bands,
                    "alpha_ratio": alpha_ratio,
                    "beta_alpha_ratio": beta_alpha_ratio
                })
                
                for client in connected_clients.copy():
                    if client != websocket:
                        try:
                            await client.send(fft_msg)
                            await client.send(band_msg)
                        except Exception:
                            pass

            # ── Per-second rate report ────────────────────────────────────────
            now = time.time()
            if now - last_print >= 1.0:
                hz = sample_count - last_sample_count
                print(
                    f"[RATE] {hz:3d} samples/sec  |  total={sample_count}  "
                    f"packets={packet_count}",
                    flush=True,
                )
                last_print        = now
                last_sample_count = sample_count

                # Flush CSV every second so data survives a Ctrl-C
                if csv_file is not None:
                    csv_file.flush()

    except websockets.exceptions.ConnectionClosed:
        print(f"[-] Client disconnected: {client_ip}", flush=True)
    finally:
        connected_clients.discard(websocket)


# ── Main ──────────────────────────────────────────────────────────────────────
async def serve():
    print(f"[Server] WebSocket Dashboard listening on ws://{HOST}:{PORT}", flush=True)
    print(f"[Server] Will auto-connect to Telemetry Viewer on port {TELEMETRY_PORT} when you start it...", flush=True)
    
    # Start WebSocket server
    ws_server = websockets.serve(handler, HOST, PORT, ping_interval=20, ping_timeout=10)
    
    # Start Telemetry Viewer auto-connect client loop in background
    client_task = asyncio.create_task(telemetry_viewer_client())
    
    await ws_server
    await client_task


if __name__ == "__main__":
    # Filename: CLI arg or interactive prompt
    if len(sys.argv) > 1:
        base = sys.argv[1]
    else:
        base = input("CSV filename (without .csv): ").strip() or "eeg_data"

    csv_filename = base + ".csv"

    # Open CSV and assign to GLOBALS before the server starts –
    # this is what was broken in server.py (it used locals).
    csv_file   = open(csv_filename, "w", newline="", encoding="utf-8")
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(["sample_index", "host_timestamp", "esp_millis", "raw_adc"])

    print(f"[Server] Saving data to: {csv_filename}", flush=True)
    print(f"[Server] Expecting  {{\"dev\":\"EXG\",\"v\":<int>,\"t\":<millis>}}", flush=True)

    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        print("\n[Server] Stopped.", flush=True)
    finally:
        if csv_file:
            csv_file.close()
            print(f"[Server] CSV closed: {csv_filename}", flush=True)
