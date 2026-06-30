/**
 * esp32-eeg-standalone-ap-logger.ino
 *
 * STANDALONE ESP32 firmware for BioAmp EXG Pill — no router, no laptop server.
 *
 * What it does
 * ------------
 *  1. The moment it powers on, the ESP32 broadcasts its OWN WiFi network
 *     (a WiFi Access Point). You'll see it in your phone/laptop's WiFi list.
 *  2. You join it and enter the password set below (AP_PASS).
 *  3. Because there's no internet on this network, your phone/laptop OS
 *     will automatically pop up a "Sign in to network" / captive portal
 *     browser window — this loads the live dashboard automatically.
 *     (If it doesn't auto-pop on your device, just open a browser and go
 *     to http://192.168.4.1 — it works identically.)
 *  4. The dashboard shows the live EXG/EEG reading as a number AND a
 *     real-time scrolling graph (custom canvas plot, no internet/CDN
 *     needed — works fully offline on the AP).
 *  5. You type how long you want to log (seconds or minutes) and hit
 *     "Start Logging". The ESP32 timestamps and saves every sample to its
 *     internal flash (LittleFS). When the timer ends, your browser
 *     automatically downloads the CSV file — named with the sensor name,
 *     a run counter, and the duration you logged.
 *
 * Hardware (same as your original)
 * ---------------------------------
 *  BioAmp EXG Pill OUT  -->  ESP32 GPIO 39 (VN)   [ADC1 channel 3, input-only]
 *  Onboard LED          -->  GPIO 2
 *
 *  NOTE: GPIO39 is on ADC1, which is unaffected by WiFi activity (WiFi only
 *  conflicts with ADC2 pins) — this is exactly why your original code chose
 *  this pin, and it's preserved here for the same reason.
 *
 * Libraries needed (Arduino Library Manager)
 * -------------------------------------------
 *  - "WebSockets" by Markus Sattler   (you already have this — we just use
 *     its WebSocketsServer class instead of WebSocketsClient this time)
 *  Everything else (WiFi, DNSServer, WebServer, LittleFS, Preferences) ships
 *  built into the ESP32 Arduino core — nothing else to install.
 *
 * IMPORTANT — Partition scheme
 * ------------------------------
 *  For logging to work you need flash space set aside for LittleFS.
 *  In Arduino IDE: Tools > Partition Scheme > "Default 4MB with spiffs
 *  (1.2MB APP/1.5MB SPIFFS)" (or any scheme with a SPIFFS/FS partition).
 *  ~1.5MB gives you roughly 18-20 minutes of continuous logging at 250 Hz.
 *
 * Output filename format
 * ------------------------
 *  /EEG_<SENSOR_NAME>_Log<counter>_<duration>s.csv
 *  e.g.  EEG_BioAmpEXG_Log003_300s.csv
 *  The counter persists across reboots (stored in NVS) so every logging
 *  run gets a unique, incrementing file.
 *
 * CSV columns: SampleNo, Timestamp_ms, ADC_Raw, Voltage_V
 */

// ---------------------------------------------------------------------------
//  Includes
// ---------------------------------------------------------------------------
#include <WiFi.h>
#include <DNSServer.h>
#include <WebServer.h>
#include <WebSocketsServer.h>
#include <LittleFS.h>
#include <Preferences.h>

// ---------------------------------------------------------------------------
//  ====================  USER-CONFIGURABLE SETTINGS  ========================
// ---------------------------------------------------------------------------
#define AP_SSID          "EEG-Sensor"     // WiFi name your phone/laptop will see
#define AP_PASS          "eeg123456"      // WiFi password (8+ chars, WPA2). Change me!
#define SENSOR_NAME      "BioAmpEXG"      // used in the log filename

#define ADC_PIN          39               // GPIO39 / VN — BioAmp EXG Pill output
#define ONBOARD_LED      2

#define SAMPLE_RATE_HZ   250              // sampling rate (250 Hz is a solid default
                                           // for EXG/EEG signals; raise cautiously —
                                           // ESP32 ADC noise increases at very high rates)

#define MAX_LOG_DURATION_S   7200         // hard safety cap: 2 hours
// ---------------------------------------------------------------------------

#define SAMPLE_INTERVAL_MS   (1000 / SAMPLE_RATE_HZ)

#define WS_PORT          81
#define HTTP_PORT        80
#define DNS_PORT         53

#define QUEUE_DEPTH          (SAMPLE_RATE_HZ * 2)   // ~2 seconds of buffering
#define MAX_SAMPLE_JSON_LEN  32
#define WS_BATCH_BUFFER_SIZE 4096
#define WS_BATCH_THRESHOLD   10           // flush to browser every ~40 ms @250Hz

#define LOG_LINE_LEN         40
#define LOG_BUFFER_SIZE      4096
#define LOG_FLUSH_LINES       100         // write to flash every ~100 samples

// ---------------------------------------------------------------------------
//  Globals
// ---------------------------------------------------------------------------
WebServer        server(HTTP_PORT);
WebSocketsServer webSocket(WS_PORT);
DNSServer        dnsServer;
Preferences      prefs;

typedef struct {
  uint16_t value;
  uint32_t timestamp;
} Sample_t;

QueueHandle_t sampleQueue = NULL;
TaskHandle_t  adcTaskHandle = NULL;

bool     anyClientConnected = false;

bool     isLogging        = false;
File     logFile;
String   currentLogFilename;
uint32_t logStartMillis   = 0;
uint32_t logDurationMs    = 0;
uint32_t logSampleCount   = 0;

char wsBatchBuffer[WS_BATCH_BUFFER_SIZE];
int  wsBatchPos   = 0;
int  wsBatchCount = 0;
uint32_t lastWsFlush = 0;

char logLineBuffer[LOG_BUFFER_SIZE];
int  logBufferPos  = 0;
int  logBufferLines = 0;

// ---------------------------------------------------------------------------
//  Dashboard HTML (fully self-contained — no external CDN, works offline)
// ---------------------------------------------------------------------------
static const char* DASHBOARD_HTML = R"HTMLPAGE(
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>EEG Live Dashboard</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',Roboto,sans-serif;background:#0f172a;color:#e2e8f0;padding:1rem;min-height:100vh}
.wrap{max-width:760px;margin:0 auto}
h1{font-size:1.3rem;color:#38bdf8;margin-bottom:.2rem}
.sub{font-size:.78rem;color:#64748b;margin-bottom:1rem}
.statusRow{display:flex;align-items:center;gap:.5rem;margin-bottom:1rem;font-size:.8rem;color:#94a3b8}
.dot{width:9px;height:9px;border-radius:50%;background:#ef4444;display:inline-block}
.dot.on{background:#4ade80;box-shadow:0 0 8px #4ade80}
.card{background:#1e293b;border:1px solid #334155;border-radius:14px;padding:1.2rem;margin-bottom:1rem}
.bigval{font-size:2.6rem;font-weight:700;color:#38bdf8;line-height:1}
.bigval span{font-size:1rem;color:#64748b;font-weight:400}
.metrics{display:flex;gap:1.5rem;margin-top:.5rem;font-size:.8rem;color:#94a3b8}
canvas{width:100%;height:240px;background:#0f172a;border-radius:10px;border:1px solid #334155;display:block}
.logRow{display:flex;gap:.6rem;flex-wrap:wrap;align-items:flex-end;margin-top:.8rem}
.field{display:flex;flex-direction:column;gap:.3rem}
label{font-size:.72rem;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:.04em}
input,select{padding:.55rem .7rem;background:#0f172a;border:1px solid #475569;border-radius:8px;color:#e2e8f0;font-size:.9rem;outline:none}
input:focus,select:focus{border-color:#38bdf8}
input[type=number]{width:110px}
button{padding:.6rem 1.1rem;border:none;border-radius:8px;font-weight:700;cursor:pointer;font-size:.88rem;letter-spacing:.02em;transition:opacity .15s}
button:hover{opacity:.85}
button:disabled{opacity:.4;cursor:not-allowed}
.btnStart{background:linear-gradient(135deg,#0ea5e9,#6366f1);color:#fff}
.btnStop{background:#ef4444;color:#fff}
.logStatus{margin-top:.9rem;font-size:.85rem;color:#94a3b8}
.logStatus b{color:#38bdf8}
.progressTrack{height:8px;background:#0f172a;border-radius:99px;margin-top:.5rem;overflow:hidden;border:1px solid #334155}
.progressFill{height:100%;background:linear-gradient(90deg,#0ea5e9,#6366f1);width:0%;transition:width .3s linear}
.footnote{font-size:.7rem;color:#475569;margin-top:.4rem}
</style>
</head>
<body>
<div class="wrap">
  <h1>&#9889; EEG / EXG Live Dashboard</h1>
  <div class="sub" id="sensorLabel">Sensor: ...</div>

  <div class="statusRow">
    <span class="dot" id="connDot"></span>
    <span id="connText">Connecting...</span>
    <span style="margin-left:auto" id="rateText"></span>
  </div>

  <div class="card">
    <div class="bigval"><span id="bigValNum">--</span> <span>raw ADC</span></div>
    <div class="metrics">
      <div>Voltage: <b id="voltVal" style="color:#e2e8f0">-- V</b></div>
      <div>Samples received: <b id="sampleCountVal" style="color:#e2e8f0">0</b></div>
    </div>
  </div>

  <div class="card">
    <canvas id="graph"></canvas>
  </div>

  <div class="card">
    <label style="display:block;margin-bottom:.5rem">Data Logging</label>
    <div class="logRow">
      <div class="field">
        <label>Duration</label>
        <input id="durVal" type="number" min="1" max="7200" value="60">
      </div>
      <div class="field">
        <label>Unit</label>
        <select id="durUnit">
          <option value="1">Seconds</option>
          <option value="60">Minutes</option>
        </select>
      </div>
      <button class="btnStart" id="startBtn" onclick="startLogging()">Start Logging</button>
      <button class="btnStop" id="stopBtn" onclick="stopLogging()" disabled>Stop</button>
    </div>
    <div class="logStatus" id="logStatusText">Not logging.</div>
    <div class="progressTrack"><div class="progressFill" id="progressFill"></div></div>
    <div class="footnote">When logging finishes, the CSV file downloads to this device automatically.</div>
  </div>
</div>

<script>
let ws;
let connected = false;
let buffer = [];                 // {v, t}
const MAX_POINTS = 750;          // ~3s of graph history at 250Hz
let lastRateCheck = Date.now();
let samplesSinceRateCheck = 0;
let totalSamples = 0;

let logging = false;
let logEndsAt = 0;
let logTotalMs = 0;
let progressTimer = null;

function connectWS() {
  ws = new WebSocket('ws://' + location.hostname + ':81/');
  ws.onopen = () => {
    connected = true;
    document.getElementById('connDot').classList.add('on');
    document.getElementById('connText').innerText = 'Connected';
  };
  ws.onclose = () => {
    connected = false;
    document.getElementById('connDot').classList.remove('on');
    document.getElementById('connText').innerText = 'Disconnected — retrying...';
    setTimeout(connectWS, 1500);
  };
  ws.onerror = () => ws.close();
  ws.onmessage = (evt) => {
    const msg = evt.data;
    if (msg.length === 0) return;
    if (msg[0] === 'D') {
      // Data batch: D[{"v":..,"t":..},...]
      try {
        const arr = JSON.parse(msg.substring(1));
        for (const s of arr) {
          buffer.push(s);
          totalSamples++;
          samplesSinceRateCheck++;
        }
        if (buffer.length > MAX_POINTS) buffer.splice(0, buffer.length - MAX_POINTS);
        if (arr.length > 0) {
          const last = arr[arr.length - 1];
          document.getElementById('bigValNum').innerText = last.v;
          document.getElementById('voltVal').innerText = (last.v * 3.3 / 4095).toFixed(3) + ' V';
        }
        document.getElementById('sampleCountVal').innerText = totalSamples;
      } catch (e) {}
    } else if (msg[0] === 'S') {
      handleStatus(msg.substring(2)); // strip "S:"
    }
  };
}

function handleStatus(payload) {
  const parts = payload.split(':');
  const type = parts[0];
  if (type === 'LOG_STARTED') {
    const filename = parts[1];
    const durationS = parseInt(parts[2]);
    logging = true;
    logTotalMs = durationS * 1000;
    logEndsAt = Date.now() + logTotalMs;
    document.getElementById('startBtn').disabled = true;
    document.getElementById('stopBtn').disabled = false;
    document.getElementById('logStatusText').innerHTML =
      'Logging to <b>' + filename + '</b>...';
    if (progressTimer) clearInterval(progressTimer);
    progressTimer = setInterval(updateProgress, 250);
  } else if (type === 'LOG_COMPLETE' || type === 'LOG_STOPPED') {
    const filename = parts[1];
    const sampleCount = parts[2];
    logging = false;
    if (progressTimer) clearInterval(progressTimer);
    document.getElementById('progressFill').style.width = '100%';
    document.getElementById('startBtn').disabled = false;
    document.getElementById('stopBtn').disabled = true;
    document.getElementById('logStatusText').innerHTML =
      (type === 'LOG_COMPLETE' ? 'Finished: ' : 'Stopped: ') +
      '<b>' + filename + '</b> (' + sampleCount + ' samples). Downloading...';
    triggerDownload(filename);
    setTimeout(() => { document.getElementById('progressFill').style.width = '0%'; }, 1500);
  } else if (type === 'LOG_ERROR') {
    logging = false;
    if (progressTimer) clearInterval(progressTimer);
    document.getElementById('startBtn').disabled = false;
    document.getElementById('stopBtn').disabled = true;
    document.getElementById('logStatusText').innerText = 'Error: ' + parts.slice(1).join(':');
  }
}

function updateProgress() {
  const remainMs = Math.max(0, logEndsAt - Date.now());
  const pct = Math.min(100, 100 * (1 - remainMs / logTotalMs));
  document.getElementById('progressFill').style.width = pct + '%';
  document.getElementById('logStatusText').innerHTML =
    document.getElementById('logStatusText').innerHTML.split(' — ')[0] +
    ' — ' + Math.ceil(remainMs / 1000) + 's remaining';
}

function triggerDownload(filename) {
  const a = document.createElement('a');
  a.href = '/download?file=' + encodeURIComponent(filename);
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
}

function startLogging() {
  const val = parseInt(document.getElementById('durVal').value);
  const unit = parseInt(document.getElementById('durUnit').value);
  if (!val || val <= 0) { alert('Enter a valid duration'); return; }
  const seconds = val * unit;
  ws.send('C:START:' + seconds);
}

function stopLogging() {
  ws.send('C:STOP');
}

// ---- Canvas graph ----
const canvas = document.getElementById('graph');
const ctx = canvas.getContext('2d');
function resizeCanvas() {
  canvas.width = canvas.clientWidth * devicePixelRatio;
  canvas.height = canvas.clientHeight * devicePixelRatio;
}
window.addEventListener('resize', resizeCanvas);
resizeCanvas();

function drawGraph() {
  requestAnimationFrame(drawGraph);
  const w = canvas.width, h = canvas.height;
  ctx.clearRect(0, 0, w, h);
  if (buffer.length < 2) return;

  let min = Infinity, max = -Infinity;
  for (const s of buffer) { if (s.v < min) min = s.v; if (s.v > max) max = s.v; }
  if (min === max) { min -= 10; max += 10; }
  const pad = (max - min) * 0.1;
  min -= pad; max += pad;

  // gridlines
  ctx.strokeStyle = '#1e293b';
  ctx.lineWidth = 1;
  for (let i = 1; i < 4; i++) {
    const y = (h / 4) * i;
    ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(w, y); ctx.stroke();
  }

  // line
  ctx.strokeStyle = '#38bdf8';
  ctx.lineWidth = 2 * devicePixelRatio;
  ctx.beginPath();
  const n = buffer.length;
  for (let i = 0; i < n; i++) {
    const x = (i / (MAX_POINTS - 1)) * w;
    const y = h - ((buffer[i].v - min) / (max - min)) * h;
    if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
  }
  ctx.stroke();
}
drawGraph();

// ---- live sample rate readout ----
setInterval(() => {
  const now = Date.now();
  const dt = (now - lastRateCheck) / 1000;
  const rate = (samplesSinceRateCheck / dt).toFixed(0);
  document.getElementById('rateText').innerText = rate + ' Hz';
  samplesSinceRateCheck = 0;
  lastRateCheck = now;
}, 1000);

document.getElementById('sensorLabel').innerText = 'Sensor: )HTMLPAGE" SENSOR_NAME R"HTMLPAGE( (GPIO39 / ADC1)';
connectWS();
</script>
</body>
</html>
)HTMLPAGE";

// ---------------------------------------------------------------------------
//  Captive portal helpers
// ---------------------------------------------------------------------------
void handleRoot() {
  server.send(200, "text/html", DASHBOARD_HTML);
}

void handleNotFound() {
  // Catches every captive-portal probe URL (Apple, Android, Windows, etc.)
  // and redirects to the dashboard, which is exactly what triggers the
  // automatic "Sign in to network" popup on most devices.
  server.sendHeader("Location", "http://192.168.4.1/", true);
  server.send(302, "text/plain", "");
}

void handleDownload() {
  if (!server.hasArg("file")) { server.send(400, "text/plain", "Missing file param"); return; }
  String fname = server.arg("file");
  if (!fname.startsWith("/")) fname = "/" + fname;
  if (!LittleFS.exists(fname)) { server.send(404, "text/plain", "File not found"); return; }
  File f = LittleFS.open(fname, "r");
  server.sendHeader("Content-Disposition", "attachment; filename=\"" + fname.substring(1) + "\"");
  server.streamFile(f, "text/csv");
  f.close();
}

// ---------------------------------------------------------------------------
//  Logging control
// ---------------------------------------------------------------------------
String makeLogFilename(uint32_t counter, uint32_t durationSec) {
  char buf[64];
  snprintf(buf, sizeof(buf), "/EEG_%s_Log%03lu_%lus.csv", SENSOR_NAME, (unsigned long)counter, (unsigned long)durationSec);
  return String(buf);
}

void startLoggingSession(uint32_t durationSec, uint8_t requesterNum) {
  if (isLogging) {
    webSocket.sendTXT(requesterNum, "S:LOG_ERROR:A logging session is already running");
    return;
  }
  if (durationSec == 0 || durationSec > MAX_LOG_DURATION_S) {
    webSocket.sendTXT(requesterNum, "S:LOG_ERROR:Duration out of allowed range");
    return;
  }

  // Rough storage check (~20 bytes/line average)
  uint64_t estimatedBytes = (uint64_t)durationSec * SAMPLE_RATE_HZ * 20;
  uint64_t freeBytes = LittleFS.totalBytes() - LittleFS.usedBytes();
  if (estimatedBytes > freeBytes) {
    webSocket.sendTXT(requesterNum, "S:LOG_ERROR:Not enough flash storage for that duration");
    return;
  }

  prefs.begin("eeglog", false);
  uint32_t counter = prefs.getUInt("counter", 0) + 1;
  prefs.putUInt("counter", counter);
  prefs.end();

  currentLogFilename = makeLogFilename(counter, durationSec);
  logFile = LittleFS.open(currentLogFilename, "w");
  if (!logFile) {
    webSocket.sendTXT(requesterNum, "S:LOG_ERROR:Could not create log file");
    return;
  }
  logFile.println("SampleNo,Timestamp_ms,ADC_Raw,Voltage_V");

  logSampleCount  = 0;
  logBufferPos    = 0;
  logBufferLines  = 0;
  logDurationMs   = durationSec * 1000UL;
  logStartMillis  = millis();
  isLogging       = true;

  char msg[80];
  snprintf(msg, sizeof(msg), "S:LOG_STARTED:%s:%lu", currentLogFilename.c_str() + 1, (unsigned long)durationSec);
  webSocket.broadcastTXT(msg);

  digitalWrite(ONBOARD_LED, HIGH);
}

void finishLoggingSession(bool stoppedManually) {
  if (!isLogging) return;
  isLogging = false;

  if (logBufferPos > 0) {
    logFile.write((const uint8_t*)logLineBuffer, logBufferPos);
    logBufferPos = 0;
  }
  logFile.close();

  char msg[80];
  snprintf(msg, sizeof(msg), "S:%s:%s:%lu",
           stoppedManually ? "LOG_STOPPED" : "LOG_COMPLETE",
           currentLogFilename.c_str() + 1,
           (unsigned long)logSampleCount);
  webSocket.broadcastTXT(msg);

  digitalWrite(ONBOARD_LED, LOW);
}

// ---------------------------------------------------------------------------
//  WebSocket event handler (control commands from browser)
// ---------------------------------------------------------------------------
void webSocketEvent(uint8_t num, WStype_t type, uint8_t* payload, size_t length) {
  switch (type) {
    case WStype_CONNECTED:
      anyClientConnected = true;
      break;
    case WStype_DISCONNECTED:
      break;
    case WStype_TEXT: {
      String msg = String((char*)payload).substring(0, length);
      if (msg.startsWith("C:START:")) {
        uint32_t durationSec = msg.substring(8).toInt();
        startLoggingSession(durationSec, num);
      } else if (msg.startsWith("C:STOP")) {
        if (isLogging) finishLoggingSession(true);
      }
      break;
    }
    default: break;
  }
}

// ---------------------------------------------------------------------------
//  ADC sampling task — pinned to core 0, fixed-rate, never blocks on network
// ---------------------------------------------------------------------------
void adcSampleTask(void* parameter) {
  TickType_t lastWake = xTaskGetTickCount();
  for (;;) {
    vTaskDelayUntil(&lastWake, pdMS_TO_TICKS(SAMPLE_INTERVAL_MS));
    Sample_t s;
    s.value = (uint16_t)analogRead(ADC_PIN);
    s.timestamp = millis();
    xQueueSend(sampleQueue, &s, 0);   // drop sample if queue is full rather than block
  }
}

// ---------------------------------------------------------------------------
//  setup()
// ---------------------------------------------------------------------------
void setup() {
  Serial.begin(115200);
  delay(300);

  pinMode(ONBOARD_LED, OUTPUT);
  digitalWrite(ONBOARD_LED, LOW);

  // ADC config (matches original: 12-bit, 0-3.3V range)
  analogReadResolution(12);
  analogSetPinAttenuation(ADC_PIN, ADC_11db);
  pinMode(ADC_PIN, INPUT);

  // Filesystem for logging
  if (!LittleFS.begin(true)) {
    Serial.println("[FS] LittleFS mount failed even after format!");
  } else {
    Serial.printf("[FS] LittleFS ready. Total: %u bytes, Used: %u bytes\n",
                  LittleFS.totalBytes(), LittleFS.usedBytes());
  }

  // Start the Access Point — this is what shows up in WiFi lists immediately
  WiFi.mode(WIFI_AP);
  WiFi.softAP(AP_SSID, AP_PASS);
  IPAddress apIP = WiFi.softAPIP();
  Serial.print("[AP] Started. Connect to \""); Serial.print(AP_SSID);
  Serial.print("\" and browse to http://"); Serial.println(apIP);

  // Captive portal DNS: redirect ALL domain lookups to our own IP
  dnsServer.start(DNS_PORT, "*", apIP);

  // HTTP routes
  server.on("/", HTTP_GET, handleRoot);
  server.on("/download", HTTP_GET, handleDownload);
  server.onNotFound(handleNotFound);   // captures captive-portal probe URLs
  server.begin();

  // WebSocket server for live data + logging control
  webSocket.begin();
  webSocket.onEvent(webSocketEvent);

  // Sample queue + dedicated sampling task
  sampleQueue = xQueueCreate(QUEUE_DEPTH, sizeof(Sample_t));
  xTaskCreatePinnedToCore(adcSampleTask, "ADCSample", 4096, NULL, 3, &adcTaskHandle, 0);

  wsBatchBuffer[wsBatchPos++] = '[';

  Serial.printf("[Setup] Sampling GPIO%d at %d Hz. Ready.\n", ADC_PIN, SAMPLE_RATE_HZ);
}

// ---------------------------------------------------------------------------
//  loop()  — runs on core 1: services DNS/HTTP/WS, drains the sample queue,
//  broadcasts live data, and writes the log file when active.
// ---------------------------------------------------------------------------
void loop() {
  dnsServer.processNextRequest();
  server.handleClient();
  webSocket.loop();

  Sample_t s;
  while (xQueueReceive(sampleQueue, &s, 0) == pdTRUE) {

    // ---- build live WebSocket batch ----
    char sampleStr[MAX_SAMPLE_JSON_LEN];
    int len = snprintf(sampleStr, sizeof(sampleStr), "%s{\"v\":%u,\"t\":%lu}",
                        wsBatchCount > 0 ? "," : "", s.value, (unsigned long)s.timestamp);
    if (len > 0 && wsBatchPos + len < WS_BATCH_BUFFER_SIZE - 2) {
      memcpy(wsBatchBuffer + wsBatchPos, sampleStr, len);
      wsBatchPos += len;
      wsBatchCount++;
    }

    // ---- write to log file if logging ----
    if (isLogging) {
      logSampleCount++;
      float volts = s.value * 3.3f / 4095.0f;
      int llen = snprintf(logLineBuffer + logBufferPos, LOG_BUFFER_SIZE - logBufferPos,
                           "%lu,%lu,%u,%.4f\n",
                           (unsigned long)logSampleCount, (unsigned long)s.timestamp, s.value, volts);
      if (llen > 0) {
        logBufferPos += llen;
        logBufferLines++;
      }
      if (logBufferLines >= LOG_FLUSH_LINES || logBufferPos > LOG_BUFFER_SIZE - LOG_LINE_LEN) {
        logFile.write((const uint8_t*)logLineBuffer, logBufferPos);
        logBufferPos = 0;
        logBufferLines = 0;
      }
    }
  }

  // ---- flush WS batch to all connected browsers ----
  if (wsBatchCount > 0 && (millis() - lastWsFlush >= 40 || wsBatchCount >= WS_BATCH_THRESHOLD)) {
    wsBatchBuffer[wsBatchPos++] = ']';
    wsBatchBuffer[wsBatchPos] = '\0';

    // Prefix "D" identifies this as a data message to the browser JS
    static char outMsg[WS_BATCH_BUFFER_SIZE + 1];
    outMsg[0] = 'D';
    memcpy(outMsg + 1, wsBatchBuffer, wsBatchPos + 1);
    webSocket.broadcastTXT(outMsg);

    wsBatchPos = 0;
    wsBatchBuffer[wsBatchPos++] = '[';
    wsBatchCount = 0;
    lastWsFlush = millis();
  }

  // ---- check if logging duration has elapsed ----
  if (isLogging && (millis() - logStartMillis >= logDurationMs)) {
    finishLoggingSession(false);
  }
}
