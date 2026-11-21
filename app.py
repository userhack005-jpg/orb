import os
import json
import time
import threading
import requests
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify, render_template_string

try:
    import fcntl
except ImportError:
    fcntl = None

app = Flask(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
CONFIG_FILE = "config.json"
STATUS_FILE = "status.json"
LOCK_FILE = "/tmp/orb_bot.lock"
config_lock = threading.Lock()

default_status = {
    "live_price": None,
    "m5_high": None,
    "m5_low": None,
    "active_range_title": "Waiting...",
    "active_range_time": "-",
    "state": "INITIALIZING",
    "range_high": None,
    "range_low": None,
    "retest_count": 0,
    "max_retests": 4,
    "last_updated": "-",
    "logs": []
}

bot_status = dict(default_status)

def save_status():
    try:
        tmp_file = STATUS_FILE + ".tmp"
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(bot_status, f)
        os.replace(tmp_file, STATUS_FILE)
    except Exception:
        pass

def read_status():
    try:
        if os.path.exists(STATUS_FILE):
            with open(STATUS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return bot_status

def add_log(msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    print(entry, flush=True)
    bot_status["logs"].insert(0, entry)
    bot_status["logs"] = bot_status["logs"][:30]
    save_status()

def load_config():
    with config_lock:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        else:
            cfg = {
                "symbol": "BINANCE:BTCUSDT",
                "timezone": "Asia/Kolkata",
                "telegram_bot_token": "",
                "telegram_chat_id": "",
                "max_retests": 4,
                "alerts": {"range_marked": True, "breakout_close": True, "retest_entry": True},
                "ranges": [{"title": "Asian Open", "time": "01:00"}]
            }
        env_token = os.environ.get("TELEGRAM_BOT_TOKEN")
        env_chat = os.environ.get("TELEGRAM_CHAT_ID")
        if env_token and (not cfg.get("telegram_bot_token") or cfg["telegram_bot_token"] == "YOUR_TELEGRAM_BOT_TOKEN"):
            cfg["telegram_bot_token"] = env_token
        if env_chat and (not cfg.get("telegram_chat_id") or cfg["telegram_chat_id"] == "YOUR_TELEGRAM_CHAT_ID"):
            cfg["telegram_chat_id"] = env_chat
        return cfg

def save_config(new_cfg):
    with config_lock:
        valid_ranges = []
        for r in new_cfg.get("ranges", []):
            t = r.get("time", "").strip()
            title = r.get("title", "Range").strip() or "Range"
            if len(t) == 5 and ":" in t:
                valid_ranges.append({"title": title, "time": t})
        valid_ranges.sort(key=lambda x: x["time"])
        new_cfg["ranges"] = valid_ranges
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(new_cfg, f, indent=2)

def send_telegram(msg, alert_type=None):
    cfg = load_config()
    if alert_type and not cfg.get("alerts", {}).get(alert_type, True):
        add_log(f"(Muted '{alert_type}') {msg}")
        return
    token = cfg.get("telegram_bot_token", "").strip()
    chat_id = cfg.get("telegram_chat_id", "").strip()
    add_log(f"ALERT: {msg}")
    if not token or not chat_id or token == "YOUR_TELEGRAM_BOT_TOKEN":
        add_log("⚠️ Telegram Token/Chat ID not set.")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        requests.post(url, json={"chat_id": chat_id, "text": msg}, timeout=8)
    except Exception as e:
        add_log(f"Telegram Error: {e}")

def get_tv_data(symbol):
    url = "https://scanner.tradingview.com/global/scan"
    payload = {
        "symbols": {"tickers": [symbol]},
        "columns": ["close", "bid", "ask", "open|5", "high|5", "low|5", "close|5"]
    }
    try:
        res = requests.post(url, json=payload, timeout=5).json()
        if not res.get("data"):
            add_log(f"⚠️ Symbol not found on TradingView: {symbol}")
            return None
        d = res["data"][0]["d"]
        live_tick = d[1] if d[1] is not None else d[0]
        return {
            "live": live_tick,
            "m5_open": d[3],
            "m5_high": d[4],
            "m5_low": d[5],
            "m5_close": d[6]
        }
    except Exception as e:
        add_log(f"TV Fetch Error: {e}")
        return None

def get_current_schedule(now, ranges):
    if not ranges:
        return None
    sorted_ranges = sorted(ranges, key=lambda x: x["time"])
    candidates = []
    for day_offset in (-1, 0):
        base_date = (now + timedelta(days=day_offset)).date()
        for r in sorted_ranges:
            hh, mm = map(int, r["time"].split(":"))
            start_dt = datetime(base_date.year, base_date.month, base_date.day, hh, mm, 0, tzinfo=IST)
            if start_dt <= now:
                candidates.append((start_dt, r["title"], r["time"]))
    if not candidates:
        return None
    start_dt, title, time_str = candidates[-1]
    end_dt = start_dt + timedelta(minutes=5)
    instance_id = f"{start_dt.strftime('%Y-%m-%d')}_{time_str}"
    return instance_id, title, time_str, start_dt, end_dt

def run_bot():
    # Ensure only ONE bot loop runs even if Gunicorn spawns multiple workers
    if fcntl:
        try:
            lock_fp = open(LOCK_FILE, "w")
            fcntl.flock(lock_fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            return

    active_instance_id = None
    state = "WAITING_FOR_RANGE"
    range_high = None
    range_low = None
    retest_count = 0

    active_slot = None
    last_slot_close_price = None
    breakout_slot = None

    add_log("🚀 Multi-Range ORB Bot Started.")

    while True:
        cfg = load_config()
        now = datetime.now(IST)
        symbol = cfg.get("symbol", "BINANCE:BTCUSDT")
        max_retests = int(cfg.get("max_retests", 4))
        ranges = cfg.get("ranges", [])

        bot_status["max_retests"] = max_retests
        bot_status["last_updated"] = now.strftime("%H:%M:%S IST")

        # Always fetch live price & 5m high/low so Dashboard stays live 24/7
        data = get_tv_data(symbol)
        if data:
            bot_status["live_price"] = data["live"]
            bot_status["m5_high"] = data["m5_high"]
            bot_status["m5_low"] = data["m5_low"]

        sched = get_current_schedule(now, ranges)
        if not sched:
            bot_status["state"] = "NO_RANGES_CONFIGURED"
            save_status()
            time.sleep(5)
            continue

        instance_id, title, time_str, target_start, target_end = sched

        if active_instance_id != instance_id:
            active_instance_id = instance_id
            range_high = None
            range_low = None
            retest_count = 0
            active_slot = None
            last_slot_close_price = None
            breakout_slot = None

            if target_start <= now < target_end:
                state = "MARKING_RANGE"
                add_log(f"⏱️ New Range Active: [{title} - {time_str}]. Marking 5m High/Low...")
            else:
                state = "MISSED_WINDOW_WAITING_NEXT"
                add_log(f"⏭️ [{title} - {time_str}] window passed. Waiting for next scheduled range.")

        bot_status["active_range_title"] = title
        bot_status["active_range_time"] = time_str
        bot_status["state"] = state
        bot_status["range_high"] = range_high
        bot_status["range_low"] = range_low
        bot_status["retest_count"] = retest_count
        save_status()

        if state in ("DONE_FOR_RANGE", "MISSED_WINDOW_WAITING_NEXT"):
            time.sleep(3)
            continue

        if not data:
            time.sleep(3)
            continue

        # 1. Marking the 5-minute range
        if state == "MARKING_RANGE":
            if now < target_end:
                range_high = data["m5_high"]
                range_low = data["m5_low"]
                bot_status["range_high"] = range_high
                bot_status["range_low"] = range_low
                save_status()
                time.sleep(2)
                continue
            else:
                if range_high is not None and range_low is not None:
                    state = "SEEKING_INITIAL_BREAK"
                    active_slot = (now.hour, now.minute // 5)
                    bot_status["state"] = state
                    save_status()
                    send_telegram(
                        f"📌 [{title} | {time_str}] Range Marked ({symbol})\n"
                        f"High: {range_high}\nLow: {range_low}",
                        alert_type="range_marked"
                    )
                else:
                    state = "MISSED_WINDOW_WAITING_NEXT"
                continue

        current_slot = (now.hour, now.minute // 5)

        # 2. 5-Minute Candle Close Detection
        if active_slot is not None and current_slot != active_slot and last_slot_close_price is not None:
            closed_price = last_slot_close_price
            breakout_triggered = False

            if state == "SEEKING_INITIAL_BREAK":
                if closed_price < range_low:
                    state = "WAIT_SELL_RETEST"
                    breakout_slot = current_slot
                    breakout_triggered = True
                    send_telegram(
                        f"🔻 [{title}] Close Below Range Low ({closed_price} < {range_low}). Ready for SELL...",
                        alert_type="breakout_close"
                    )
                elif closed_price > range_high:
                    state = "WAIT_BUY_RETEST"
                    breakout_slot = current_slot
                    breakout_triggered = True
                    send_telegram(
                        f"🔺 [{title}] Close Above Range High ({closed_price} > {range_high}). Ready for BUY...",
                        alert_type="breakout_close"
                    )

            elif state == "IN_SELL_SEEKING_HIGH_BREAK":
                if closed_price > range_high:
                    state = "WAIT_BUY_RETEST"
                    breakout_slot = current_slot
                    breakout_triggered = True
                    send_telegram(
                        f"🔄 [{title}] Range High Broken Now! ({closed_price} > {range_high}). Ready for BUY...",
                        alert_type="breakout_close"
                    )

            elif state == "IN_BUY_SEEKING_LOW_BREAK":
                if closed_price < range_low:
                    state = "WAIT_SELL_RETEST"
                    breakout_slot = current_slot
                    breakout_triggered = True
                    send_telegram(
                        f"🔄 [{title}] Range Low Broken Now! ({closed_price} < {range_low}). Ready for SELL...",
                        alert_type="breakout_close"
                    )

            if breakout_triggered:
                active_slot = current_slot
                last_slot_close_price = data["m5_close"]
                bot_status["state"] = state
                save_status()
                time.sleep(6)
                continue

        active_slot = current_slot
        last_slot_close_price = data["m5_close"]

        # 3. Live Retest Entry Check (Immediate Tick + Wick after 10s)
        if state == "WAIT_SELL_RETEST":
            wick_touch = (now.second >= 10 and data["m5_high"] is not None and data["m5_high"] >= range_low)
            if data["live"] >= range_low or wick_touch:
                retest_count += 1
                send_telegram(
                    f"✅ [{title}] Successful SELL Entered! ({retest_count}/{max_retests})\n"
                    f"Live: {data['live']} | Range Low: {range_low}",
                    alert_type="retest_entry"
                )
                if retest_count >= max_retests:
                    state = "DONE_FOR_RANGE"
                    add_log(f"🛑 [{title}] Max {max_retests} retests reached. Waiting for next range.")
                else:
                    state = "IN_SELL_SEEKING_HIGH_BREAK"

        elif state == "WAIT_BUY_RETEST":
            wick_touch = (now.second >= 10 and data["m5_low"] is not None and data["m5_low"] <= range_high)
            if data["live"] <= range_high or wick_touch:
                retest_count += 1
                send_telegram(
                    f"✅ [{title}] Entered in BUY! ({retest_count}/{max_retests})\n"
                    f"Live: {data['live']} | Range High: {range_high}",
                    alert_type="retest_entry"
                )
                if retest_count >= max_retests:
                    state = "DONE_FOR_RANGE"
                    add_log(f"🛑 [{title}] Max {max_retests} retests reached. Waiting for next range.")
                else:
                    state = "IN_BUY_SEEKING_LOW_BREAK"

        bot_status["state"] = state
        bot_status["retest_count"] = retest_count
        save_status()
        time.sleep(2.5)

# ================= WEB DASHBOARD =================
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ORB Multi-Range Bot Dashboard</title>
<style>
  body { font-family: system-ui, -apple-system, sans-serif; background: #0f172a; color: #e2e8f0; margin: 0; padding: 16px; }
  .container { max-width: 740px; margin: auto; }
  .card { background: #1e293b; border: 1px solid #334155; border-radius: 12px; padding: 16px; margin-bottom: 16px; }
  h2, h3 { margin-top: 0; color: #38bdf8; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; }
  .stat { background: #0f172a; padding: 10px; border-radius: 8px; border: 1px solid #334155; }
  .stat small { color: #94a3b8; display: block; font-size: 12px; }
  .stat strong { font-size: 15px; color: #f8fafc; word-break: break-word; }
  label { display: block; margin-top: 10px; font-size: 13px; color: #cbd5e1; }
  input[type="text"], input[type="number"], input[type="time"] {
    width: 100%; box-sizing: border-box; padding: 8px 10px; margin-top: 4px;
    background: #0f172a; border: 1px solid #475569; color: #fff; border-radius: 6px;
  }
  .row { display: flex; gap: 8px; align-items: center; margin-top: 8px; }
  .row input { margin-top: 0; }
  .checkbox-row { display: flex; gap: 16px; flex-wrap: wrap; margin-top: 10px; }
  .checkbox-row label { display: flex; align-items: center; gap: 6px; margin: 0; cursor: pointer; }
  button { background: #0284c7; color: #fff; border: none; padding: 10px 14px; border-radius: 6px; cursor: pointer; font-weight: 600; margin-top: 12px; }
  button:hover { background: #0369a1; }
  .btn-danger { background: #dc2626; padding: 8px 10px; margin-top: 0; }
  .btn-secondary { background: #475569; }
  .logs { background: #020617; font-family: monospace; font-size: 12px; padding: 10px; border-radius: 8px; max-height: 220px; overflow-y: auto; line-height: 1.5; }
</style>
</head>
<body>
<div class="container">
  <h2>⚡ ORB Multi-Range Bot Dashboard</h2>

  <div class="card">
    <h3>Live Status <small id="updated" style="font-weight:normal; color:#94a3b8; float:right;"></small></h3>
    <div class="grid">
      <div class="stat"><small>Active Range</small><strong id="st_range">-</strong></div>
      <div class="stat"><small>State</small><strong id="st_state">-</strong></div>
      <div class="stat"><small>Live Price</small><strong id="st_live" style="color:#4ade80;">-</strong></div>
      <div class="stat"><small>Range High / Low</small><strong id="st_hl" style="color:#fbbf24;">-</strong></div>
      <div class="stat"><small>Current 5m High / Low</small><strong id="st_m5hl">-</strong></div>
      <div class="stat"><small>Retests</small><strong id="st_retests">-</strong></div>
    </div>
  </div>

  <div class="card">
    <h3>Settings & Alert Toggles</h3>
    <div class="grid">
      <div><label>Symbol (e.g. OANDA:XAUUSD, BINANCE:BTCUSDT)</label><input type="text" id="symbol"></div>
      <div><label>Max Retests / Range</label><input type="number" id="max_retests" min="1" max="20"></div>
    </div>
    <label>Telegram Bot Token</label><input type="text" id="token">
    <label>Telegram Chat ID</label><input type="text" id="chat_id">

    <div class="checkbox-row">
      <label><input type="checkbox" id="al_range"> 1. Range Marked Alert</label>
      <label><input type="checkbox" id="al_break"> 2. Breakout Close Alert</label>
      <label><input type="checkbox" id="al_retest"> 3. Retest Entry Alert</label>
    </div>

    <h3 style="margin-top:20px;">Sequential Range Timings (IST 24-Hr)</h3>
    <div id="ranges_list"></div>
    <button type="button" class="btn-secondary" onclick="addRangeRow('', '')">+ Add Range Time</button>
    <div style="display:flex; gap:10px; flex-wrap:wrap;">
      <button type="button" onclick="saveSettings()">💾 Save All Settings</button>
      <button type="button" class="btn-secondary" onclick="testTelegram()">🔔 Test Telegram</button>
    </div>
    <p id="save_msg" style="color:#4ade80; font-size:13px;"></p>
  </div>

  <div class="card">
    <h3>Recent Logs</h3>
    <div class="logs" id="logs">Loading...</div>
  </div>
</div>

<script>
function addRangeRow(title, timeVal) {
  const div = document.createElement('div');
  div.className = 'row';
  div.innerHTML = `
    <input type="text" class="r-title" placeholder="Title (e.g. London Open)" value="${title}">
    <input type="time" class="r-time" value="${timeVal}" style="max-width:130px;">
    <button type="button" class="btn-danger" onclick="this.parentElement.remove()">✕</button>
  `;
  document.getElementById('ranges_list').appendChild(div);
}

async function loadAll() {
  const res = await fetch('/api/config');
  const cfg = await res.json();
  document.getElementById('symbol').value = cfg.symbol || 'BINANCE:BTCUSDT';
  document.getElementById('max_retests').value = cfg.max_retests || 4;
  document.getElementById('token').value = cfg.telegram_bot_token || '';
  document.getElementById('chat_id').value = cfg.telegram_chat_id || '';
  document.getElementById('al_range').checked = cfg.alerts?.range_marked ?? true;
  document.getElementById('al_break').checked = cfg.alerts?.breakout_close ?? true;
  document.getElementById('al_retest').checked = cfg.alerts?.retest_entry ?? true;

  const list = document.getElementById('ranges_list');
  list.innerHTML = '';
  (cfg.ranges || []).forEach(r => addRangeRow(r.title, r.time));
}

async function saveSettings() {
  const ranges = [];
  document.querySelectorAll('#ranges_list .row').forEach(row => {
    const title = row.querySelector('.r-title').value.trim();
    const time = row.querySelector('.r-time').value.trim();
    if (time) ranges.push({ title: title || 'Range', time });
  });
  const payload = {
    symbol: document.getElementById('symbol').value.trim(),
    timezone: 'Asia/Kolkata',
    max_retests: parseInt(document.getElementById('max_retests').value) || 4,
    telegram_bot_token: document.getElementById('token').value.trim(),
    telegram_chat_id: document.getElementById('chat_id').value.trim(),
    alerts: {
      range_marked: document.getElementById('al_range').checked,
      breakout_close: document.getElementById('al_break').checked,
      retest_entry: document.getElementById('al_retest').checked
    },
    ranges
  };
  const res = await fetch('/api/config', {
    method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)
  });
  if (res.ok) {
    document.getElementById('save_msg').innerText = '✅ Settings Saved! Applied live.';
    setTimeout(() => document.getElementById('save_msg').innerText = '', 3000);
    loadAll();
  }
}

async function testTelegram() {
  await fetch('/api/test_telegram', { method: 'POST' });
  alert('Test alert triggered! Check Telegram & Logs.');
}

async function refreshStatus() {
  const res = await fetch('/api/status');
  const s = await res.json();
  document.getElementById('updated').innerText = s.last_updated || '-';
  document.getElementById('st_range').innerText = `${s.active_range_title} (${s.active_range_time})`;
  document.getElementById('st_state').innerText = s.state || '-';
  document.getElementById('st_live').innerText = s.live_price ?? '-';
  document.getElementById('st_hl').innerText = (s.range_high && s.range_low) ? `${s.range_high} / ${s.range_low}` : '-';
  document.getElementById('st_m5hl').innerText = (s.m5_high && s.m5_low) ? `${s.m5_high} / ${s.m5_low}` : '-';
  document.getElementById('st_retests').innerText = `${s.retest_count ?? 0} / ${s.max_retests ?? 4}`;
  document.getElementById('logs').innerHTML = (s.logs || []).map(l => `<div>${l}</div>`).join('');
}

loadAll();
refreshStatus();
setInterval(refreshStatus, 3000);
</script>
</body>
</html>
"""

@app.route("/")
def index():
    return render_template_string(DASHBOARD_HTML)

@app.route("/health")
def health():
    return "OK", 200

@app.route("/api/status")
def api_status():
    return jsonify(read_status())

@app.route("/api/config", methods=["GET", "POST"])
def api_config():
    if request.method == "POST":
        save_config(request.json)
        return jsonify({"ok": True})
    return jsonify(load_config())

@app.route("/api/test_telegram", methods=["POST"])
def api_test_telegram():
    send_telegram("🔔 Test Alert from ORB Multi-Range Dashboard!")
    return jsonify({"ok": True})

bot_thread = threading.Thread(target=run_bot, daemon=True)
bot_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
