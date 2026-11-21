import os
import re
import json
import time
import threading
import requests
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify

try:
    import fcntl
except ImportError:
    fcntl = None

app = Flask(__name__)

# ================= CONFIGURATION =================
IST = timezone(timedelta(hours=5, minutes=30))
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
UPSTASH_REDIS_REST_URL = os.environ.get("UPSTASH_REDIS_REST_URL", "").strip().rstrip("/")
UPSTASH_REDIS_REST_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "").strip()
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "").strip().rstrip("/")

LOCAL_DB_FILE = "users_db.json"
LOCK_FILE = "/tmp/orb_multiuser_bot.lock"
REDIS_KEY = "orb_multiuser_bot:users_v1"

db_lock = threading.Lock()
price_cache_lock = threading.Lock()
latest_prices = {}  # {symbol: {"live": ..., "m5_open": ..., "m5_high": ..., "m5_low": ..., "m5_close": ...}}

# ================= STORAGE (UPSTASH REDIS + LOCAL FALLBACK) =================
def _redis_headers():
    return {
        "Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}",
        "Content-Type": "application/json"
    }

def load_all_users():
    with db_lock:
        if UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN:
            try:
                resp = requests.get(
                    f"{UPSTASH_REDIS_REST_URL}/get/{REDIS_KEY}",
                    headers=_redis_headers(),
                    timeout=8
                )
                if resp.status_code == 200:
                    result = resp.json().get("result")
                    if result:
                        return json.loads(result)
                    return {}
            except Exception as e:
                print(f"[DB] Redis read error: {e}", flush=True)
        # Fallback to local JSON file
        if os.path.exists(LOCAL_DB_FILE):
            try:
                with open(LOCAL_DB_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

def save_all_users(users_dict):
    with db_lock:
        raw = json.dumps(users_dict)
        if UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN:
            try:
                requests.post(
                    f"{UPSTASH_REDIS_REST_URL}/set/{REDIS_KEY}",
                    headers=_redis_headers(),
                    data=raw.encode("utf-8"),
                    timeout=8
                )
            except Exception as e:
                print(f"[DB] Redis write error: {e}", flush=True)
        # Also mirror locally for fast reads / fallback
        try:
            tmp = LOCAL_DB_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(raw)
            os.replace(tmp, LOCAL_DB_FILE)
        except Exception as e:
            print(f"[DB] Local write error: {e}", flush=True)

def default_user_profile(chat_id):
    return {
        "chat_id": str(chat_id),
        "symbol": "OANDA:XAUUSD",
        "active": True,
        "max_retests": 4,
        "alerts": {
            "range_marked": True,
            "breakout_close": True,
            "retest_entry": True
        },
        "ranges": [
            {"title": "Asian Open", "time": "01:00"},
            {"title": "London Open", "time": "12:00"}
        ],
        # Interactive UI flow state
        "ui_step": None,
        "ui_temp": {},
        # Runtime ORB tracking state
        "runtime": {
            "active_instance_id": None,
            "active_range_title": "-",
            "active_range_time": "-",
            "state": "WAITING_FOR_RANGE",
            "range_high": None,
            "range_low": None,
            "retest_count": 0,
            "active_slot": None,
            "last_slot_close_price": None,
            "retest_cooldown_until": 0.0
        }
    }

def get_or_create_user(users, chat_id):
    cid = str(chat_id)
    if cid not in users:
        users[cid] = default_user_profile(cid)
    else:
        # Ensure all keys exist for older records
        base = default_user_profile(cid)
        for k, v in base.items():
            if k not in users[cid]:
                users[cid][k] = v
        for rk, rv in base["runtime"].items():
            if rk not in users[cid]["runtime"]:
                users[cid]["runtime"][rk] = rv
    return users[cid]

# ================= TELEGRAM API HELPERS =================
def tg_api(method, payload):
    if not TELEGRAM_BOT_TOKEN:
        print("[TG] Missing TELEGRAM_BOT_TOKEN", flush=True)
        return None
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    try:
        resp = requests.post(url, json=payload, timeout=10)
        return resp.json()
    except Exception as e:
        print(f"[TG] {method} error: {e}", flush=True)
        return None

def send_message(chat_id, text, reply_markup=None, parse_mode="HTML"):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return tg_api("sendMessage", payload)

def edit_message(chat_id, message_id, text, reply_markup=None, parse_mode="HTML"):
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return tg_api("editMessageText", payload)

def answer_callback(callback_query_id, text=""):
    payload = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
    tg_api("answerCallbackQuery", payload)

def setup_bot_commands_and_webhook():
    if not TELEGRAM_BOT_TOKEN:
        return
    # Register Menu Tab Commands
    commands = [
        {"command": "start", "description": "Open Main Menu & Welcome"},
        {"command": "ranges", "description": "Add, Edit, or Delete Range Timings"},
        {"command": "status", "description": "View Live Price & Active Range Status"},
        {"command": "help", "description": "How to use this ORB Trading Bot"}
    ]
    tg_api("setMyCommands", {"commands": commands})

    # Auto-register Webhook if RENDER_EXTERNAL_URL is set
    if RENDER_EXTERNAL_URL:
        webhook_url = f"{RENDER_EXTERNAL_URL}/webhook"
        res = tg_api("setWebhook", {"url": webhook_url, "drop_pending_updates": False})
        print(f"[TG] Webhook setup ({webhook_url}): {res}", flush=True)

# ================= TRADINGVIEW BATCH FETCHER =================
TV_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Origin": "https://www.tradingview.com",
    "Referer": "https://www.tradingview.com/",
    "Content-Type": "application/json"
}

def fetch_tv_batch(symbols):
    """Fetch multiple unique symbols in ONE single TradingView API call."""
    unique_syms = sorted(list(set(s.strip().upper() for s in symbols if s and s.strip())))
    if not unique_syms:
        return {}
    url = "https://scanner.tradingview.com/global/scan"
    payload = {
        "symbols": {"tickers": unique_syms},
        "columns": ["close", "bid", "ask", "open|5", "high|5", "low|5", "close|5"]
    }
    try:
        resp = requests.post(url, json=payload, headers=TV_HEADERS, timeout=10)
        if resp.status_code != 200 or not resp.text.strip():
            print(f"[TV] HTTP {resp.status_code} (rate limit/empty)", flush=True)
            return {}
        data = resp.json().get("data", [])
        out = {}
        for item in data:
            sym = item.get("s", "").upper()
            d = item.get("d", [])
            if len(d) >= 7:
                live_tick = d[1] if d[1] is not None else d[0]
                out[sym] = {
                    "live": live_tick,
                    "m5_open": d[3],
                    "m5_high": d[4],
                    "m5_low": d[5],
                    "m5_close": d[6]
                }
        return out
    except Exception as e:
        print(f"[TV] Batch fetch error: {e}", flush=True)
        return {}

def validate_tv_symbol(symbol):
    sym = symbol.strip().upper()
    res = fetch_tv_batch([sym])
    return sym in res, res.get(sym)

# ================= INLINE KEYBOARD BUILDERS =================
def main_menu_keyboard(user):
    toggle_text = "⏸️ Pause Alerts" if user.get("active", True) else "▶️ Resume Alerts"
    return {
        "inline_keyboard": [
            [{"text": f"📈 Set Symbol ({user['symbol']})", "callback_data": "menu:symbol"}],
            [{"text": f"⏰ Manage Ranges ({len(user['ranges'])})", "callback_data": "menu:ranges"}],
            [{"text": "🔔 Alerts & Max Retests", "callback_data": "menu:alerts"}],
            [{"text": "📊 Live Status", "callback_data": "menu:status"}],
            [{"text": toggle_text, "callback_data": "action:toggle_active"}],
            [{"text": "❓ Help & Guide", "callback_data": "menu:help"}]
        ]
    }

def main_menu_text(user):
    status_badge = "🟢 Active" if user.get("active", True) else "🔴 Paused"
    return (
        f"⚡ <b>ORB Multi-Range Alert Bot</b>\n\n"
        f"Welcome! Track Opening Range Breakouts (5m candle close) and instant live Retest Entries directly on Telegram.\n\n"
        f"<b>Current Setup:</b>\n"
        f"• Status: {status_badge}\n"
        f"• Symbol: <code>{user['symbol']}</code>\n"
        f"• Ranges Configured: <b>{len(user['ranges'])}</b>\n"
        f"• Max Retests / Range: <b>{user['max_retests']}</b>\n\n"
        f"Choose an option below to configure your bot:"
    )

def ranges_menu_view(user):
    ranges = sorted(user.get("ranges", []), key=lambda x: x["time"])
    user["ranges"] = ranges
    lines = ["⏰ <b>Manage Sequential Ranges (IST 24-Hr)</b>\n"]
    if not ranges:
        lines.append("<i>No ranges configured yet. Tap below to add your first range.</i>")
    else:
        lines.append("Ranges run sequentially. When the next range time starts, the previous range automatically disables.\n")
        for idx, r in enumerate(ranges):
            lines.append(f"{idx+1}. <b>{r['title']}</b> — <code>{r['time']} IST</code>")

    keyboard = []
    for idx, r in enumerate(ranges):
        keyboard.append([
            {"text": f"✏️ Edit {r['time']} ({r['title'][:14]})", "callback_data": f"range:edit:{idx}"},
            {"text": "🗑️ Delete", "callback_data": f"range:del:{idx}"}
        ])
    keyboard.append([{"text": "➕ Add New Range", "callback_data": "range:add"}])
    keyboard.append([{"text": "⬅️ Back to Main Menu", "callback_data": "menu:main"}])
    return "\n".join(lines), {"inline_keyboard": keyboard}

def alerts_menu_view(user):
    al = user.get("alerts", {})
    r_icon = "✅" if al.get("range_marked", True) else "❌"
    b_icon = "✅" if al.get("breakout_close", True) else "❌"
    e_icon = "✅" if al.get("retest_entry", True) else "❌"
    mx = user.get("max_retests", 4)

    text = (
        "🔔 <b>Alert Preferences & Max Retests</b>\n\n"
        "Tap any alert below to turn it ON or OFF:\n"
        f"• <b>1. Range Marked:</b> {r_icon}\n"
        f"• <b>2. Breakout Close:</b> {b_icon}\n"
        f"• <b>3. Retest Entry:</b> {e_icon}\n\n"
        f"<b>Max Retests per Range:</b> <code>{mx}</code>"
    )
    keyboard = {
        "inline_keyboard": [
            [{"text": f"{r_icon} 1. Range Marked Alert", "callback_data": "alert:toggle:range_marked"}],
            [{"text": f"{b_icon} 2. Breakout Close Alert", "callback_data": "alert:toggle:breakout_close"}],
            [{"text": f"{e_icon} 3. Retest Entry Alert", "callback_data": "alert:toggle:retest_entry"}],
            [
                {"text": "➖ Decrease", "callback_data": "retest:dec"},
                {"text": f"Max: {mx}", "callback_data": "noop"},
                {"text": "➕ Increase", "callback_data": "retest:inc"}
            ],
            [{"text": "⬅️ Back to Main Menu", "callback_data": "menu:main"}]
        ]
    }
    return text, keyboard

def status_view(user):
    sym = user["symbol"].upper()
    with price_cache_lock:
        pdata = latest_prices.get(sym)
    if not pdata:
        fetched = fetch_tv_batch([sym])
        pdata = fetched.get(sym)

    rt = user.get("runtime", {})
    live_str = f"<code>{pdata['live']}</code>" if pdata and pdata.get("live") is not None else "<i>Fetching...</i>"
    hl_str = (
        f"<code>{rt['range_high']}</code> / <code>{rt['range_low']}</code>"
        if rt.get("range_high") is not None and rt.get("range_low") is not None
        else "<i>Not marked yet</i>"
    )
    now_str = datetime.now(IST).strftime("%H:%M:%S IST")
    status_badge = "🟢 Active" if user.get("active", True) else "🔴 Paused"

    text = (
        f"📊 <b>Live Bot Status</b>\n"
        f"<i>Updated: {now_str}</i>\n\n"
        f"• Alerts: {status_badge}\n"
        f"• Symbol: <code>{sym}</code>\n"
        f"• Live Price: {live_str}\n"
        f"• Active Range: <b>{rt.get('active_range_title', '-')}</b> ({rt.get('active_range_time', '-')})\n"
        f"• State: <code>{rt.get('state', 'WAITING')}</code>\n"
        f"• Range High / Low: {hl_str}\n"
        f"• Retests Completed: <b>{rt.get('retest_count', 0)} / {user.get('max_retests', 4)}</b>"
    )
    keyboard = {
        "inline_keyboard": [
            [{"text": "🔄 Refresh Status", "callback_data": "menu:status"}],
            [{"text": "⬅️ Back to Main Menu", "callback_data": "menu:main"}]
        ]
    }
    return text, keyboard

def help_view():
    text = (
        "❓ <b>How This ORB Bot Works</b>\n\n"
        "<b>1. Set Your Symbol</b>\n"
        "Use TradingView format with exchange prefix, e.g.:\n"
        "• Gold: <code>OANDA:XAUUSD</code>\n"
        "• Silver: <code>OANDA:XAGUSD</code>\n"
        "• Bitcoin: <code>BINANCE:BTCUSDT</code>\n"
        "• EUR/USD: <code>FX:EURUSD</code>\n\n"
        "<b>2. Configure Ranges (IST 24-Hr)</b>\n"
        "Add one or more 5-minute range start times (e.g., <code>01:00</code>, <code>12:00</code>, <code>15:05</code>). "
        "The bot marks the 5-minute candle High & Low.\n"
        "• Multiple ranges switch automatically in sequence.\n"
        "• A single range stays active for 24 hours until Max Retests is reached.\n\n"
        "<b>3. Alert Cycle</b>\n"
        "• <b>Range Marked:</b> Sent when the 5m range candle closes.\n"
        "• <b>Breakout Close:</b> Sent when a subsequent 5m candle closes outside the range.\n"
        "• <b>Retest Entry:</b> Sent immediately on live price touch back to the breakout level."
    )
    keyboard = {"inline_keyboard": [[{"text": "⬅️ Back to Main Menu", "callback_data": "menu:main"}]]}
    return text, keyboard

# ================= SCHEDULE & ORB ENGINE =================
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

def send_user_alert(user, msg, alert_type):
    if not user.get("alerts", {}).get(alert_type, True):
        return
    send_message(user["chat_id"], msg)

def process_user_orb(user, data, now):
    """Runs the ORB state machine for a single user. Returns True if state changed."""
    changed = False
    ranges = user.get("ranges", [])
    rt = user["runtime"]
    max_retests = int(user.get("max_retests", 4))
    symbol = user["symbol"].upper()

    sched = get_current_schedule(now, ranges)
    if not sched:
        if rt.get("state") != "NO_RANGES":
            rt["state"] = "NO_RANGES"
            return True
        return False

    instance_id, title, time_str, target_start, target_end = sched

    # Automatic sequential handoff to new range
    if rt.get("active_instance_id") != instance_id:
        rt["active_instance_id"] = instance_id
        rt["active_range_title"] = title
        rt["active_range_time"] = time_str
        rt["range_high"] = None
        rt["range_low"] = None
        rt["retest_count"] = 0
        rt["active_slot"] = None
        rt["last_slot_close_price"] = None
        rt["retest_cooldown_until"] = 0.0
        if target_start <= now < target_end:
            rt["state"] = "MARKING_RANGE"
        else:
            rt["state"] = "MISSED_WINDOW_WAITING_NEXT"
        changed = True

    state = rt["state"]
    if state in ("DONE_FOR_RANGE", "MISSED_WINDOW_WAITING_NEXT"):
        return changed

    if not data:
        return changed

    # 1. Mark 5-minute range
    if state == "MARKING_RANGE":
        if now < target_end:
            if rt["range_high"] != data["m5_high"] or rt["range_low"] != data["m5_low"]:
                rt["range_high"] = data["m5_high"]
                rt["range_low"] = data["m5_low"]
                changed = True
            return changed
        else:
            if rt["range_high"] is not None and rt["range_low"] is not None:
                rt["state"] = "SEEKING_INITIAL_BREAK"
                rt["active_slot"] = [now.hour, now.minute // 5]
                rt["last_slot_close_price"] = data["m5_close"]
                send_user_alert(
                    user,
                    f"📌 <b>[{title} | {time_str} IST] Range Marked</b>\n"
                    f"Symbol: <code>{symbol}</code>\n"
                    f"High: <code>{rt['range_high']}</code>\n"
                    f"Low: <code>{rt['range_low']}</code>",
                    "range_marked"
                )
            else:
                rt["state"] = "MISSED_WINDOW_WAITING_NEXT"
            return True

    current_slot = [now.hour, now.minute // 5]
    active_slot = rt.get("active_slot")
    last_close = rt.get("last_slot_close_price")
    r_high = rt["range_high"]
    r_low = rt["range_low"]

    # 2. 5-Minute Candle Close Detection (Zero Missed Closes)
    if active_slot is not None and current_slot != active_slot and last_close is not None:
        breakout_triggered = False

        if state == "SEEKING_INITIAL_BREAK":
            if last_close < r_low:
                rt["state"] = "WAIT_SELL_RETEST"
                breakout_triggered = True
                send_user_alert(
                    user,
                    f"🔻 <b>[{title}] Close Below Range Low</b>\n"
                    f"Symbol: <code>{symbol}</code>\n"
                    f"Close: <code>{last_close}</code> &lt; Low: <code>{r_low}</code>\n"
                    f"Status: Waiting for SELL Retest...",
                    "breakout_close"
                )
            elif last_close > r_high:
                rt["state"] = "WAIT_BUY_RETEST"
                breakout_triggered = True
                send_user_alert(
                    user,
                    f"🔺 <b>[{title}] Close Above Range High</b>\n"
                    f"Symbol: <code>{symbol}</code>\n"
                    f"Close: <code>{last_close}</code> &gt; High: <code>{r_high}</code>\n"
                    f"Status: Waiting for BUY Retest...",
                    "breakout_close"
                )

        elif state == "IN_SELL_SEEKING_HIGH_BREAK":
            if last_close > r_high:
                rt["state"] = "WAIT_BUY_RETEST"
                breakout_triggered = True
                send_user_alert(
                    user,
                    f"🔄 <b>[{title}] Range High Broken!</b>\n"
                    f"Symbol: <code>{symbol}</code>\n"
                    f"Close: <code>{last_close}</code> &gt; High: <code>{r_high}</code>\n"
                    f"Status: Reversal — Waiting for BUY Retest...",
                    "breakout_close"
                )

        elif state == "IN_BUY_SEEKING_LOW_BREAK":
            if last_close < r_low:
                rt["state"] = "WAIT_SELL_RETEST"
                breakout_triggered = True
                send_user_alert(
                    user,
                    f"🔄 <b>[{title}] Range Low Broken!</b>\n"
                    f"Symbol: <code>{symbol}</code>\n"
                    f"Close: <code>{last_close}</code> &lt; Low: <code>{r_low}</code>\n"
                    f"Status: Reversal — Waiting for SELL Retest...",
                    "breakout_close"
                )

        rt["active_slot"] = current_slot
        rt["last_slot_close_price"] = data["m5_close"]
        if breakout_triggered:
            # Non-blocking 8-second cooldown so stale previous-candle wick never triggers false retest
            rt["retest_cooldown_until"] = time.time() + 8.0
        return True

    if rt.get("active_slot") != current_slot or rt.get("last_slot_close_price") != data["m5_close"]:
        rt["active_slot"] = current_slot
        rt["last_slot_close_price"] = data["m5_close"]
        changed = True

    # 3. Immediate Live Retest Entry Check (after cooldown)
    if time.time() < rt.get("retest_cooldown_until", 0.0):
        return changed

    state = rt["state"]
    if state == "WAIT_SELL_RETEST":
        wick_touch = (now.second >= 10 and data["m5_high"] is not None and data["m5_high"] >= r_low)
        if (data["live"] is not None and data["live"] >= r_low) or wick_touch:
            rt["retest_count"] = rt.get("retest_count", 0) + 1
            cnt = rt["retest_count"]
            send_user_alert(
                user,
                f"✅ <b>[{title}] SELL Entry Triggered! ({cnt}/{max_retests})</b>\n"
                f"Symbol: <code>{symbol}</code>\n"
                f"Live Price: <code>{data['live']}</code> | Range Low: <code>{r_low}</code>",
                "retest_entry"
            )
            rt["state"] = "DONE_FOR_RANGE" if cnt >= max_retests else "IN_SELL_SEEKING_HIGH_BREAK"
            return True

    elif state == "WAIT_BUY_RETEST":
        wick_touch = (now.second >= 10 and data["m5_low"] is not None and data["m5_low"] <= r_high)
        if (data["live"] is not None and data["live"] <= r_high) or wick_touch:
            rt["retest_count"] = rt.get("retest_count", 0) + 1
            cnt = rt["retest_count"]
            send_user_alert(
                user,
                f"✅ <b>[{title}] BUY Entry Triggered! ({cnt}/{max_retests})</b>\n"
                f"Symbol: <code>{symbol}</code>\n"
                f"Live Price: <code>{data['live']}</code> | Range High: <code>{r_high}</code>",
                "retest_entry"
            )
            rt["state"] = "DONE_FOR_RANGE" if cnt >= max_retests else "IN_BUY_SEEKING_LOW_BREAK"
            return True

    return changed

def monitor_loop():
    # Ensure only ONE background monitor runs across Gunicorn workers
    if fcntl:
        try:
            lock_fp = open(LOCK_FILE, "w")
            fcntl.flock(lock_fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            return

    print("🚀 Multi-User ORB Monitoring Engine Started.", flush=True)
    setup_bot_commands_and_webhook()

    while True:
        try:
            users = load_all_users()
            active_users = [u for u in users.values() if u.get("active", True) and u.get("ranges")]

            if not active_users:
                time.sleep(5)
                continue

            unique_symbols = list(set(u["symbol"].upper() for u in active_users))
            batch_data = fetch_tv_batch(unique_symbols)

            if batch_data:
                with price_cache_lock:
                    latest_prices.update(batch_data)

            now = datetime.now(IST)
            any_changed = False
            needs_fast_poll = False

            for u in active_users:
                sym = u["symbol"].upper()
                data = batch_data.get(sym)
                if process_user_orb(u, data, now):
                    any_changed = True
                if u["runtime"].get("state") in ("MARKING_RANGE", "WAIT_SELL_RETEST", "WAIT_BUY_RETEST"):
                    needs_fast_poll = True

            if any_changed:
                save_all_users(users)

            time.sleep(3 if needs_fast_poll else 5)
        except Exception as e:
            print(f"[LOOP] Unexpected error: {e}", flush=True)
            time.sleep(5)

# ================= TELEGRAM WEBHOOK HANDLERS =================
def handle_message(msg):
    chat_id = str(msg["chat"]["id"])
    text = (msg.get("text") or "").strip()

    users = load_all_users()
    user = get_or_create_user(users, chat_id)

    # Slash commands reset any in-progress input step
    if text.startswith("/"):
        user["ui_step"] = None
        user["ui_temp"] = {}
        save_all_users(users)

        cmd = text.split()[0].lower()
        if cmd == "/start":
            send_message(chat_id, main_menu_text(user), main_menu_keyboard(user))
        elif cmd == "/ranges":
            t, kb = ranges_menu_view(user)
            send_message(chat_id, t, kb)
        elif cmd == "/status":
            t, kb = status_view(user)
            send_message(chat_id, t, kb)
        elif cmd == "/help":
            t, kb = help_view()
            send_message(chat_id, t, kb)
        else:
            send_message(chat_id, "Unknown command. Use /start to open the Main Menu.")
        return

    # Handle interactive text steps
    step = user.get("ui_step")
    if step == "AWAIT_SYMBOL":
        sym = text.upper()
        if ":" not in sym:
            send_message(
                chat_id,
                "⚠️ Please include the exchange prefix (e.g., <code>OANDA:XAUUSD</code> or <code>BINANCE:BTCUSDT</code>).\nTry again or send /start to cancel."
            )
            return
        ok, pdata = validate_tv_symbol(sym)
        if not ok:
            send_message(
                chat_id,
                f"❌ Symbol <code>{sym}</code> was not found on TradingView.\nPlease check spelling and try again (e.g., <code>OANDA:XAUUSD</code>, <code>BINANCE:BTCUSDT</code>)."
            )
            return
        user["symbol"] = sym
        user["ui_step"] = None
        user["runtime"]["active_instance_id"] = None  # Reset runtime for new symbol
        save_all_users(users)
        send_message(
            chat_id,
            f"✅ Symbol updated to <code>{sym}</code> (Live: <code>{pdata['live']}</code>)!\n\n" + main_menu_text(user),
            main_menu_keyboard(user)
        )
        return

    elif step == "AWAIT_ADD_TITLE":
        title = text[:28] or "Range"
        user["ui_temp"] = {"title": title}
        user["ui_step"] = "AWAIT_ADD_TIME"
        save_all_users(users)
        send_message(
            chat_id,
            f"👍 Title set to <b>{title}</b>.\n\nNow send the <b>5-minute candle start time</b> in 24-hour IST format (<code>HH:MM</code>), e.g., <code>01:00</code>, <code>12:00</code>, or <code>15:05</code>:"
        )
        return

    elif step == "AWAIT_ADD_TIME":
        if not re.match(r"^([01]\d|2[0-3]):[0-5]\d$", text):
            send_message(chat_id, "⚠️ Invalid time format. Please send in 24-hour <code>HH:MM</code> format (e.g., <code>09:15</code> or <code>15:05</code>):")
            return
        title = user.get("ui_temp", {}).get("title", "Range")
        user["ranges"].append({"title": title, "time": text})
        user["ranges"].sort(key=lambda x: x["time"])
        user["ui_step"] = None
        user["ui_temp"] = {}
        save_all_users(users)
        t, kb = ranges_menu_view(user)
        send_message(chat_id, f"✅ Range <b>{title} ({text} IST)</b> added!\n\n" + t, kb)
        return

    elif step == "AWAIT_EDIT_TITLE":
        idx = user.get("ui_temp", {}).get("edit_idx")
        if idx is None or idx >= len(user["ranges"]):
            user["ui_step"] = None
            save_all_users(users)
            send_message(chat_id, "⚠️ Range not found. Returning to menu.", main_menu_keyboard(user))
            return
        if text != "-":
            user["ranges"][idx]["title"] = text[:28]
        user["ui_step"] = "AWAIT_EDIT_TIME"
        save_all_users(users)
        cur_time = user["ranges"][idx]["time"]
        send_message(
            chat_id,
            f"Now send the new start time in 24-hr IST (<code>HH:MM</code>), or send <code>-</code> to keep <code>{cur_time}</code>:"
        )
        return

    elif step == "AWAIT_EDIT_TIME":
        idx = user.get("ui_temp", {}).get("edit_idx")
        if idx is None or idx >= len(user["ranges"]):
            user["ui_step"] = None
            save_all_users(users)
            send_message(chat_id, "⚠️ Range not found. Returning to menu.", main_menu_keyboard(user))
            return
        if text != "-":
            if not re.match(r"^([01]\d|2[0-3]):[0-5]\d$", text):
                send_message(chat_id, "⚠️ Invalid time format. Send <code>HH:MM</code> (e.g., <code>14:30</code>) or <code>-</code> to keep current:")
                return
            user["ranges"][idx]["time"] = text
        user["ranges"].sort(key=lambda x: x["time"])
        user["ui_step"] = None
        user["ui_temp"] = {}
        save_all_users(users)
        t, kb = ranges_menu_view(user)
        send_message(chat_id, "✅ Range updated!\n\n" + t, kb)
        return

    # Default fallback if user types random text
    send_message(chat_id, main_menu_text(user), main_menu_keyboard(user))

def handle_callback(cb):
    cb_id = cb["id"]
    chat_id = str(cb["message"]["chat"]["id"])
    msg_id = cb["message"]["message_id"]
    data = cb.get("data", "")

    users = load_all_users()
    user = get_or_create_user(users, chat_id)

    if data == "noop":
        answer_callback(cb_id)
        return

    if data == "menu:main":
        user["ui_step"] = None
        user["ui_temp"] = {}
        save_all_users(users)
        edit_message(chat_id, msg_id, main_menu_text(user), main_menu_keyboard(user))
        answer_callback(cb_id)

    elif data == "menu:symbol":
        user["ui_step"] = "AWAIT_SYMBOL"
        save_all_users(users)
        edit_message(
            chat_id, msg_id,
            f"📈 <b>Set TradingView Symbol</b>\n\n"
            f"Current Symbol: <code>{user['symbol']}</code>\n\n"
            f"Please reply with your TradingView ticker (with exchange prefix):\n"
            f"• Gold: <code>OANDA:XAUUSD</code>\n"
            f"• Silver: <code>OANDA:XAGUSD</code>\n"
            f"• Bitcoin: <code>BINANCE:BTCUSDT</code>\n"
            f"• EUR/USD: <code>FX:EURUSD</code>",
            {"inline_keyboard": [[{"text": "❌ Cancel", "callback_data": "menu:main"}]]}
        )
        answer_callback(cb_id)

    elif data == "menu:ranges":
        user["ui_step"] = None
        save_all_users(users)
        t, kb = ranges_menu_view(user)
        edit_message(chat_id, msg_id, t, kb)
        answer_callback(cb_id)

    elif data == "range:add":
        user["ui_step"] = "AWAIT_ADD_TITLE"
        user["ui_temp"] = {}
        save_all_users(users)
        edit_message(
            chat_id, msg_id,
            "➕ <b>Add New Range</b>\n\nStep 1 of 2: Send a <b>Title</b> for this range (e.g., <code>Asian Open</code>, <code>London Open</code>, <code>NY Session</code>):",
            {"inline_keyboard": [[{"text": "❌ Cancel", "callback_data": "menu:ranges"}]]}
        )
        answer_callback(cb_id)

    elif data.startswith("range:edit:"):
        idx = int(data.split(":")[2])
        if 0 <= idx < len(user["ranges"]):
            r = user["ranges"][idx]
            user["ui_step"] = "AWAIT_EDIT_TITLE"
            user["ui_temp"] = {"edit_idx": idx}
            save_all_users(users)
            edit_message(
                chat_id, msg_id,
                f"✏️ <b>Edit Range: {r['title']} ({r['time']} IST)</b>\n\n"
                f"Send a new <b>Title</b>, or send <code>-</code> to keep <b>{r['title']}</b>:",
                {"inline_keyboard": [[{"text": "❌ Cancel", "callback_data": "menu:ranges"}]]}
            )
            answer_callback(cb_id)
        else:
            answer_callback(cb_id, "Range not found.")

    elif data.startswith("range:del:"):
        idx = int(data.split(":")[2])
        if 0 <= idx < len(user["ranges"]):
            removed = user["ranges"].pop(idx)
            save_all_users(users)
            t, kb = ranges_menu_view(user)
            edit_message(chat_id, msg_id, t, kb)
            answer_callback(cb_id, f"Deleted {removed['title']} ({removed['time']})")
        else:
            answer_callback(cb_id, "Range not found.")

    elif data == "menu:alerts":
        t, kb = alerts_menu_view(user)
        edit_message(chat_id, msg_id, t, kb)
        answer_callback(cb_id)

    elif data.startswith("alert:toggle:"):
        key = data.split(":")[2]
        if key in ("range_marked", "breakout_close", "retest_entry"):
            cur = user["alerts"].get(key, True)
            user["alerts"][key] = not cur
            save_all_users(users)
            t, kb = alerts_menu_view(user)
            edit_message(chat_id, msg_id, t, kb)
            answer_callback(cb_id, "Updated!")

    elif data in ("retest:inc", "retest:dec"):
        mx = int(user.get("max_retests", 4))
        mx = min(20, mx + 1) if data == "retest:inc" else max(1, mx - 1)
        user["max_retests"] = mx
        save_all_users(users)
        t, kb = alerts_menu_view(user)
        edit_message(chat_id, msg_id, t, kb)
        answer_callback(cb_id, f"Max Retests: {mx}")

    elif data == "menu:status":
        t, kb = status_view(user)
        edit_message(chat_id, msg_id, t, kb)
        answer_callback(cb_id, "Status refreshed")

    elif data == "action:toggle_active":
        user["active"] = not user.get("active", True)
        save_all_users(users)
        edit_message(chat_id, msg_id, main_menu_text(user), main_menu_keyboard(user))
        answer_callback(cb_id, "Alerts Resumed" if user["active"] else "Alerts Paused")

    elif data == "menu:help":
        t, kb = help_view()
        edit_message(chat_id, msg_id, t, kb)
        answer_callback(cb_id)

# ================= FLASK ROUTES =================
@app.route("/")
def home():
    users = {}
    if os.path.exists(LOCAL_DB_FILE):
        try:
            with open(LOCAL_DB_FILE, "r", encoding="utf-8") as f:
                users = json.load(f)
        except Exception:
            users = {}
    active_cnt = sum(1 for u in users.values() if u.get("active", True))
    unique_syms = len(set(u["symbol"].upper() for u in users.values() if u.get("active", True)))
    redis_ok = bool(UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN)
    return jsonify({
        "service": "ORB Multi-User Telegram Bot",
        "status": "running",
        "storage": "Upstash Redis" if redis_ok else "Local JSON",
        "total_users": len(users),
        "active_users": active_cnt,
        "tracked_unique_symbols": unique_syms,
        "server_time_ist": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    })
@app.route("/health")
def health():
    return "OK", 200

@app.route("/set_webhook")
def manual_set_webhook():
    if not TELEGRAM_BOT_TOKEN:
        return jsonify({"ok": False, "error": "TELEGRAM_BOT_TOKEN not set"}), 400
    base_url = RENDER_EXTERNAL_URL or request.url_root.rstrip("/").replace("http://", "https://")
    webhook_url = f"{base_url}/webhook"
    res = tg_api("setWebhook", {"url": webhook_url})
    setup_bot_commands_and_webhook()
    return jsonify({"webhook_url": webhook_url, "telegram_response": res})

@app.route("/webhook", methods=["POST"])
def webhook():
    update = request.get_json(silent=True) or {}
    def process_update(upd):
        try:
            if "message" in upd and "text" in upd["message"]:
                handle_message(upd["message"])
            elif "callback_query" in upd:
                handle_callback(upd["callback_query"])
        except Exception as e:
            print(f"[WEBHOOK] Error: {e}", flush=True)

    threading.Thread(target=process_update, args=(update,), daemon=True).start()
    return "OK", 200

# Start background monitoring thread
bg_thread = threading.Thread(target=monitor_loop, daemon=True)
bg_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
