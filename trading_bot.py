"""
AI-Powered Crypto Trading Bot — Enhanced Version v4
Changes from v3:
  - Minimum position size: $30 per trade (hard enforced)
  - Minimum leverage: 20x (hard enforced), max 50x
  - Full Technical Analysis:
      • Multi-timeframe: 4H + 1H + 15m
      • Market Structure: HH/HL/LH/LL (detect_market_structure_detail)
      • Break of Structure (BOS) on 1H and 15m
      • Order Blocks (bullish/bearish OB detection)
      • Liquidity Levels (equal highs/lows + sweep detection)
      • Fair Value Gap on both 4H and 15m
  - AI prompt: 8-step TA framework with minimum 3 confluences required
  - 4H candles added to build_payload for macro bias
"""

import os
import sys
import json
import time
import logging
import asyncio
import websockets
import requests
from concurrent.futures import ThreadPoolExecutor
import threading
import ctypes

# Force UTF-8 output on Windows so box-drawing characters don't crash the bot
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    try:
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv
from openai import OpenAI

# ─── LOAD .env ─────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

# ─── CONFIG ────────────────────────────────────────────────────────────────────
BINANCE_API_KEY    = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
OLLAMA_MODEL       = os.getenv("OLLAMA_MODEL", "llama-3.3-70b-versatile")
OLLAMA_BASE_URL    = os.getenv("OLLAMA_BASE_URL", "https://api.groq.com/openai/v1")
AI_API_KEY         = os.getenv("AI_API_KEY", "ollama")
SIMULATION_MODE    = os.getenv("SIMULATION_MODE", "True") == "True"
INITIAL_CAPITAL    = float(os.getenv("INITIAL_CAPITAL", "500"))
MAX_OPEN_TRADES    = int(os.getenv("MAX_OPEN_TRADES", "5"))
RISK_PER_TRADE     = float(os.getenv("RISK_PER_TRADE", "0.02"))
MIN_WIN_PROB       = float(os.getenv("MIN_WIN_PROBABILITY", "75")) / 100   # ← minimum 75%
MAX_LEVERAGE = int(os.getenv("MAX_LEVERAGE", "50"))

STATE_FILE = os.path.join(BASE_DIR, "trading_state.json")
LOCK_FILE  = os.path.join(BASE_DIR, "bot.lock")
LOG_FILE   = os.path.join(BASE_DIR, "logs", "bot.log")
os.makedirs(os.path.join(BASE_DIR, "logs"), exist_ok=True)

BINANCE_BASE = "https://fapi.binance.com"

SCAN_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "XRPUSDT", "BNBUSDT", "SOLUSDT",
    "ALGOUSDT", "RUNEUSDT", "AVAXUSDT", "NEARUSDT", "DYDXUSDT"
]

PSYCH_LEVELS = {
    "BTCUSDT":  [100000,95000,90000,85000,80000,75000,70000,65000,60000,50000],
    "ETHUSDT":  [5000,4500,4000,3500,3000,2500,2000,1800,1500],
    "SOLUSDT":  [300,250,200,180,150,120,100,80],
    "XRPUSDT":  [5,4,3,2,1.5,1,0.5],
    "BNBUSDT":  [1000,800,700,600,500,400,300],
    "ALGOUSDT": [2,1.5,1,0.5,0.25],
    "RUNEUSDT": [20,15,10,8,5,3],
    "AVAXUSDT": [100,80,60,50,40,30,20],
    "NEARUSDT": [10,8,6,5,4,3,2],
    "DYDXUSDT": [5,4,3,2,1.5,1],
}

# High-impact economic events 2026 (month, day, name)
HIGH_IMPACT_EVENTS = [
    (1,15,"US CPI"),(2,12,"US CPI"),(3,12,"US CPI"),(4,10,"US CPI"),
    (5,13,"US CPI"),(6,11,"US CPI"),(7,11,"US CPI"),(8,13,"US CPI"),
    (9,10,"US CPI"),(10,15,"US CPI"),(11,12,"US CPI"),(12,10,"US CPI"),
    (1,10,"NFP"),(2,7,"NFP"),(3,7,"NFP"),(4,4,"NFP"),
    (5,2,"NFP"),(6,6,"NFP"),(7,4,"NFP"),(8,7,"NFP"),
    (9,5,"NFP"),(10,3,"NFP"),(11,6,"NFP"),(12,4,"NFP"),
    (1,29,"FOMC"),(3,19,"FOMC"),(5,7,"FOMC"),(6,18,"FOMC"),
    (7,30,"FOMC"),(9,17,"FOMC"),(10,29,"FOMC"),(12,10,"FOMC"),
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─── STATE ─────────────────────────────────────────────────────────────────────
# ─── SINGLE-INSTANCE LOCK ──────────────────────────────────────────────────────
def acquire_lock() -> bool:
    """
    Write our PID to bot.lock. If another PID already owns the lock and that
    process is still alive → refuse to start (return False).
    Cleans up stale locks from crashed processes automatically.
    """
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE, "r") as f:
                old_pid = int(f.read().strip())
            # Check if that process is still running
            import subprocess
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {old_pid}", "/FO", "CSV"],
                capture_output=True, text=True
            )
            if str(old_pid) in result.stdout:
                print(f"[ERROR] Bot already running (PID {old_pid}). Exiting to prevent duplicate trades.")
                return False
            else:
                # Stale lock from a crashed process — clean it up
                os.remove(LOCK_FILE)
                log.warning(f"Removed stale lock (PID {old_pid} is no longer running)")
        except Exception:
            os.remove(LOCK_FILE)
    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))
    return True

def release_lock():
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
    except Exception:
        pass

# ─── SLEEP PREVENTION (Windows) ────────────────────────────────────────────────
# Keeps the system awake so the bot runs continuously even on battery/idle.
# ES_CONTINUOUS      = tell Windows this state change is permanent until reset
# ES_SYSTEM_REQUIRED = prevent the system from sleeping
# ES_AWAYMODE_REQUIRED = allow background processing in away mode (lid closed)
_ES_CONTINUOUS        = 0x80000000
_ES_SYSTEM_REQUIRED   = 0x00000001
_ES_AWAYMODE_REQUIRED = 0x00000040

def prevent_sleep():
    """Prevent Windows from sleeping while the bot is running.
    Works even with the laptop lid closed or on battery.
    Call once at startup; Windows will honour it until allow_sleep() is called.
    """
    if sys.platform == "win32":
        try:
            ctypes.windll.kernel32.SetThreadExecutionState(
                _ES_CONTINUOUS | _ES_SYSTEM_REQUIRED | _ES_AWAYMODE_REQUIRED
            )
            log.info("[SLEEP] Windows sleep prevention ENABLED — system will stay awake until bot stops")
            print("  [SLEEP] Windows sleep prevention ENABLED (lid-close safe, runs until Ctrl+C)")
        except Exception as e:
            log.warning(f"[SLEEP] Could not enable sleep prevention: {e}")

def allow_sleep():
    """Re-enable normal Windows sleep behaviour on bot shutdown."""
    if sys.platform == "win32":
        try:
            ctypes.windll.kernel32.SetThreadExecutionState(_ES_CONTINUOUS)
            log.info("[SLEEP] Windows sleep prevention RELEASED — normal sleep restored")
        except Exception:
            pass

# ─── STATE ─────────────────────────────────────────────────────────────────────
def load_state() -> dict:
    default = {
        "balance": INITIAL_CAPITAL, "initial_balance": INITIAL_CAPITAL,
        "open_trades": [], "closed_trades": [],
        "total_profit": 0.0, "win_count": 0, "loss_count": 0,
        "last_scan": None, "session_start": datetime.now().isoformat(),
        "sl_cooldowns": {}   # {"SUIUSDT_SHORT": "2026-03-30T10:45:00", ...}
    }
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)
            # Backfill any keys added in newer versions so the bot never crashes
            # on an old state file that predates a key.
            for k, v in default.items():
                saved.setdefault(k, v)
            return saved
    except Exception:
        pass
    return default

def save_state(state: dict):
    """Atomic write: save to .tmp first, then rename — prevents partial/corrupt writes."""
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, default=str)
    # Atomic replace (works on Windows too)
    if os.path.exists(STATE_FILE):
        os.replace(tmp, STATE_FILE)
    else:
        os.rename(tmp, STATE_FILE)

# ─── BINANCE ───────────────────────────────────────────────────────────────────
_binance_fail_count  = 0
_binance_blocked_until: Optional[datetime] = None
BINANCE_CIRCUIT_BREAKER_THRESHOLD = 5    # consecutive failures before pausing
BINANCE_CIRCUIT_BREAKER_PAUSE_S   = 60  # seconds to pause when circuit breaks

def binance_get(endpoint: str, params: dict = {}) -> dict:
    global _binance_fail_count, _binance_blocked_until
    # Circuit breaker: if Binance is unreachable, stop hammering immediately
    if _binance_blocked_until and datetime.now() < _binance_blocked_until:
        return {}
    try:
        r = requests.get(BINANCE_BASE + endpoint, params=params, timeout=5)
        r.raise_for_status()
        _binance_fail_count = 0   # reset on success
        _binance_blocked_until = None
        return r.json()
    except Exception as e:
        _binance_fail_count += 1
        err_str = str(e)
        # Only log first occurrence to avoid log spam
        if _binance_fail_count == 1 or _binance_fail_count % 10 == 0:
            log.error(f"Binance {endpoint}: {err_str[:120]}")
        # Trip the circuit breaker after threshold failures
        if _binance_fail_count >= BINANCE_CIRCUIT_BREAKER_THRESHOLD:
            _binance_blocked_until = datetime.now() + timedelta(seconds=BINANCE_CIRCUIT_BREAKER_PAUSE_S)
            log.warning(
                f"[CIRCUIT BREAKER] Binance unreachable ({_binance_fail_count} failures). "
                f"Pausing all Binance calls for {BINANCE_CIRCUIT_BREAKER_PAUSE_S}s. "
                f"Check: Windows Firewall blocking Python, or Binance geo-restriction."
            )
            print(f"\n  [CIRCUIT BREAKER] Binance blocked for {BINANCE_CIRCUIT_BREAKER_PAUSE_S}s — "
                  f"fix Windows Firewall (allow python.exe outbound) then restart bot.\n")
        return {}

def get_klines(symbol: str, interval: str = "15m", limit: int = 100) -> list:
    data = binance_get("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit})
    if not data: return []
    return [{"open_time":k[0],"open":float(k[1]),"high":float(k[2]),
             "low":float(k[3]),"close":float(k[4]),"volume":float(k[5])} for k in data]

def get_orderbook(symbol: str, limit: int = 50) -> dict:
    return binance_get("/fapi/v1/depth", {"symbol": symbol, "limit": limit})

def analyze_orderbook(symbol: str) -> dict:
    """
    Deep order book analysis — tells you exactly what buyers and sellers are doing RIGHT NOW.

    Calculates:
    - Total bid/ask volume at 5, 10, 20 levels
    - Buy vs Sell pressure ratio
    - Large wall detection (single order > 5x average = institutional wall)
    - Spread tightness (tight spread = active market)
    - Clear BUY / SELL / NEUTRAL signal with confidence
    """
    ob = get_orderbook(symbol, 50)
    bids = ob.get("bids", [])   # [[price, qty], ...]
    asks = ob.get("asks", [])

    if not bids or not asks:
        return {"signal": "NEUTRAL", "confidence": 0, "reason": "No orderbook data"}

    # Convert to floats
    bids = [(float(p), float(q)) for p, q in bids]
    asks = [(float(p), float(q)) for p, q in asks]

    best_bid = bids[0][0]
    best_ask = asks[0][0]
    spread_pct = (best_ask - best_bid) / best_bid * 100

    # Volume at different depths
    bid_vol_5  = sum(q for _, q in bids[:5])
    ask_vol_5  = sum(q for _, q in asks[:5])
    bid_vol_10 = sum(q for _, q in bids[:10])
    ask_vol_10 = sum(q for _, q in asks[:10])
    bid_vol_20 = sum(q for _, q in bids[:20])
    ask_vol_20 = sum(q for _, q in asks[:20])

    # Imbalance ratios (>1 = more buyers, <1 = more sellers)
    imb_5  = bid_vol_5  / ask_vol_5  if ask_vol_5  > 0 else 1.0
    imb_10 = bid_vol_10 / ask_vol_10 if ask_vol_10 > 0 else 1.0
    imb_20 = bid_vol_20 / ask_vol_20 if ask_vol_20 > 0 else 1.0

    # Weighted imbalance (closer levels matter more)
    weighted_imb = (imb_5 * 0.5) + (imb_10 * 0.3) + (imb_20 * 0.2)

    # Large wall detection
    avg_bid_qty = bid_vol_20 / 20 if bid_vol_20 > 0 else 0
    avg_ask_qty = ask_vol_20 / 20 if ask_vol_20 > 0 else 0
    bid_walls = [(p, q) for p, q in bids[:20] if avg_bid_qty > 0 and q > avg_bid_qty * 5]
    ask_walls = [(p, q) for p, q in asks[:20] if avg_ask_qty > 0 and q > avg_ask_qty * 5]

    # Determine signal
    if weighted_imb >= 1.5:
        signal = "BUY"
        confidence = min(95, int(50 + (weighted_imb - 1.0) * 30))
        reason = f"Heavy buy pressure: {weighted_imb:.2f}x more bids than asks"
    elif weighted_imb <= 0.67:
        signal = "SELL"
        confidence = min(95, int(50 + (1.0 / weighted_imb - 1.0) * 30))
        reason = f"Heavy sell pressure: {1/weighted_imb:.2f}x more asks than bids"
    elif weighted_imb >= 1.2:
        signal = "BUY"
        confidence = int(40 + (weighted_imb - 1.0) * 25)
        reason = f"Moderate buy pressure: {weighted_imb:.2f}x bids vs asks"
    elif weighted_imb <= 0.83:
        signal = "SELL"
        confidence = int(40 + (1.0 / weighted_imb - 1.0) * 25)
        reason = f"Moderate sell pressure: {1/weighted_imb:.2f}x asks vs bids"
    else:
        signal = "NEUTRAL"
        confidence = 20
        reason = f"Balanced orderbook: {weighted_imb:.2f} imbalance ratio"

    # Wall adjustments
    if bid_walls and signal == "SELL":
        reason += f" | WARNING: Large bid wall at {bid_walls[0][0]} may stop drop"
    if ask_walls and signal == "BUY":
        reason += f" | WARNING: Large ask wall at {ask_walls[0][0]} may stop rise"
    if bid_walls and signal != "SELL":
        reason += f" | Strong bid support wall at {bid_walls[0][0]}"
    if ask_walls and signal != "BUY":
        reason += f" | Strong ask resistance wall at {ask_walls[0][0]}"

    return {
        "signal":        signal,
        "confidence":    confidence,
        "weighted_imb":  round(weighted_imb, 3),
        "imb_top5":      round(imb_5,  3),
        "imb_top10":     round(imb_10, 3),
        "imb_top20":     round(imb_20, 3),
        "bid_vol_10":    round(bid_vol_10, 2),
        "ask_vol_10":    round(ask_vol_10, 2),
        "spread_pct":    round(spread_pct, 4),
        "bid_walls":     [(round(p,6), round(q,2)) for p,q in bid_walls[:3]],
        "ask_walls":     [(round(p,6), round(q,2)) for p,q in ask_walls[:3]],
        "reason":        reason,
    }

def get_volume_delta(candles: list) -> dict:
    """
    Volume Delta = Buying Volume - Selling Volume per candle.
    Bullish candle (close > open) → buying volume.
    Bearish candle (close < open) → selling volume.

    Tells you WHO is in control right now — buyers or sellers.
    """
    if len(candles) < 5:
        return {"delta_signal": "NEUTRAL", "cvd_trend": "FLAT", "last_delta": 0}

    recent = candles[-10:]
    deltas = []
    for c in recent:
        vol = c["volume"]
        if c["close"] >= c["open"]:
            deltas.append(vol)    # buying candle
        else:
            deltas.append(-vol)   # selling candle

    cvd = sum(deltas)             # cumulative volume delta
    last_5_delta = sum(deltas[-5:])
    last_3_delta = sum(deltas[-3:])

    # Trend in delta
    first_half = sum(deltas[:5])
    second_half = sum(deltas[5:])
    if second_half > first_half * 1.2:   cvd_trend = "ACCELERATING_BUY"
    elif second_half < first_half * 0.8: cvd_trend = "ACCELERATING_SELL"
    elif cvd > 0:                        cvd_trend = "BUY_DOMINANT"
    elif cvd < 0:                        cvd_trend = "SELL_DOMINANT"
    else:                                cvd_trend = "FLAT"

    if last_3_delta > 0:   delta_signal = "BUY"
    elif last_3_delta < 0: delta_signal = "SELL"
    else:                  delta_signal = "NEUTRAL"

    return {
        "delta_signal":  delta_signal,
        "cvd_trend":     cvd_trend,
        "last_3_delta":  round(last_3_delta, 2),
        "last_5_delta":  round(last_5_delta, 2),
        "cvd_10":        round(cvd, 2),
        "buyers_in_control": last_3_delta > 0,
    }

def get_ticker_24h(symbol: str) -> dict:
    return binance_get("/fapi/v1/ticker/24hr", {"symbol": symbol})

def get_open_interest(symbol: str) -> dict:
    return binance_get("/fapi/v1/openInterest", {"symbol": symbol})

def get_funding_rate(symbol: str) -> dict:
    data = binance_get("/fapi/v1/fundingRate", {"symbol": symbol, "limit": 1})
    return data[0] if data else {}

def get_current_price(symbol: str) -> float:
    data = binance_get("/fapi/v1/ticker/price", {"symbol": symbol})
    return float(data.get("price", 0))

# ─── FEAR & GREED ─────────────────────────────────────────────────────────────
def get_fear_and_greed() -> dict:
    """
    Fetches Fear & Greed index from alternative.me with cache-busting headers
    and a single retry. Logs the raw response so stale/wrong values are visible.
    """
    _default = {
        "value": 50, "label": "Neutral", "signal": "NEUTRAL",
        "soft_avoid_longs": False, "soft_avoid_shorts": False,
        "avoid_longs": False, "avoid_shorts": False,
        "breakout_override_note": "Neutral: both directions allowed freely"
    }
    headers = {"Cache-Control": "no-cache", "Pragma": "no-cache"}
    url = f"https://api.alternative.me/fng/?limit=1&format=json&t={int(time.time())}"

    for attempt in range(2):
        try:
            r = requests.get(url, headers=headers, timeout=10)
            r.raise_for_status()
            raw_json = r.json()
            log.info(f"[F&G] raw response: {raw_json}")
            item = raw_json["data"][0]
            val  = int(item["value"])
            lbl  = item["value_classification"]
            log.info(f"[F&G] parsed: value={val} label={lbl}")
            if val <= 25:
                sig = "EXTREME_FEAR"
                soft_avoid_longs, soft_avoid_shorts = True, False
                breakout_note = "Extreme Fear: SHORT preferred. LONG only on confirmed resistance breakout with volume"
            elif val <= 45:
                sig = "FEAR"
                soft_avoid_longs, soft_avoid_shorts = False, False
                breakout_note = "Fear market: prefer shorts, longs only on strong breakout confirmation"
            elif val <= 55:
                sig = "NEUTRAL"
                soft_avoid_longs, soft_avoid_shorts = False, False
                breakout_note = "Neutral: both directions allowed freely"
            elif val <= 75:
                sig = "GREED"
                soft_avoid_longs, soft_avoid_shorts = False, False
                breakout_note = "Greed market: prefer longs, shorts only on strong support breakdown with volume"
            else:
                sig = "EXTREME_GREED"
                soft_avoid_longs, soft_avoid_shorts = False, True
                breakout_note = "Extreme Greed: LONG preferred. SHORT only on confirmed support breakdown with volume"
            return {
                "value": val, "label": lbl, "signal": sig,
                "soft_avoid_longs": soft_avoid_longs,
                "soft_avoid_shorts": soft_avoid_shorts,
                "avoid_longs": False, "avoid_shorts": False,
                "breakout_override_note": breakout_note
            }
        except Exception as e:
            log.warning(f"[F&G] attempt {attempt + 1}/2 failed: {e}")
            if attempt == 0:
                time.sleep(2)

    log.error("[F&G] both attempts failed — using neutral default")
    return _default

# ─── BTC DOMINANCE ────────────────────────────────────────────────────────────
def get_btc_dominance() -> dict:
    """
    Fetches BTC dominance from CoinGecko with cache-busting headers and retry.
    Logs the raw value before rounding and warns when the data is stale.
    """
    _default = {"btc_dominance": 55.0, "signal": "BALANCED", "avoid_alts": False}
    headers = {
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "User-Agent": "Mozilla/5.0 TradingBot/1.0",
    }
    url = f"https://api.coingecko.com/api/v3/global?t={int(time.time())}"

    for attempt in range(2):
        try:
            r = requests.get(url, headers=headers, timeout=10)
            r.raise_for_status()
            body = r.json()
            dom = body["data"]["market_cap_percentage"]["btc"]
            log.info(f"[BTC_DOM] raw value from CoinGecko: {dom}")

            # Warn if CoinGecko data is stale (updated_at is a Unix timestamp)
            updated_at = body["data"].get("updated_at", 0)
            if updated_at:
                age_secs = int(time.time()) - updated_at
                if age_secs > 300:
                    log.warning(f"[BTC_DOM] data is {age_secs}s old — CoinGecko may be serving cached response")
                else:
                    log.debug(f"[BTC_DOM] data age: {age_secs}s — fresh")

            if dom >= 58:   sig, avoid_alts = "BTC_DOMINANT", True
            elif dom >= 52: sig, avoid_alts = "BTC_STRONG",   False
            elif dom >= 47: sig, avoid_alts = "BALANCED",     False
            else:           sig, avoid_alts = "ALTSEASON",    False
            # "value" key matches what the dashboard and api_server expect.
            # "btc_dominance" key kept for internal bot code (print_context etc.).
            return {
                "value": round(dom, 2),
                "btc_dominance": round(dom, 2),
                "signal": sig,
                "avoid_alts": avoid_alts,
            }
        except Exception as e:
            log.warning(f"[BTC_DOM] attempt {attempt + 1}/2 failed: {e}")
            if attempt == 0:
                time.sleep(2)

    log.error("[BTC_DOM] both attempts failed — using default")
    return _default

# ─── ECONOMIC CALENDAR ────────────────────────────────────────────────────────
def check_high_impact_event() -> dict:
    now      = datetime.now()
    tomorrow = now + timedelta(days=1)
    for month, day, name in HIGH_IMPACT_EVENTS:
        if now.month == month and now.day == day:
            return {"has_event": True, "event_name": name,
                    "advice": f"HIGH IMPACT: {name} today — skipping all trades",
                    "should_skip": True}
        if tomorrow.month == month and tomorrow.day == day and now.hour >= 22:
            return {"has_event": True, "event_name": name,
                    "advice": f"{name} tomorrow — caution with overnight trades",
                    "should_skip": False}
    return {"has_event": False, "event_name": None,
            "advice": "No major events today", "should_skip": False}

# ─── TECHNICAL ANALYSIS ───────────────────────────────────────────────────────
def compute_ema(prices: list, period: int) -> list:
    if len(prices) < period: return []
    k   = 2 / (period + 1)
    ema = [sum(prices[:period]) / period]
    for p in prices[period:]: ema.append(p * k + ema[-1] * (1 - k))
    return ema

def compute_rsi(closes: list, period: int = 14) -> float:
    if len(closes) < period + 1: return 50.0
    d = [closes[i+1]-closes[i] for i in range(len(closes)-1)]
    ag = sum(x for x in d[-period:] if x > 0) / period
    al = sum(-x for x in d[-period:] if x < 0) / period
    if al == 0: return 100.0
    return 100 - (100 / (1 + ag/al))

def compute_macd(closes: list):
    e12 = compute_ema(closes, 12)
    e26 = compute_ema(closes, 26)
    if not e12 or not e26: return 0, 0, 0
    n    = min(len(e12), len(e26))
    ml   = [e12[-(n-i)] - e26[-(n-i)] for i in range(n)]
    sig  = compute_ema(ml, 9)
    if not sig: return ml[-1], 0, 0
    return ml[-1], sig[-1], ml[-1] - sig[-1]

def compute_atr(candles: list, period: int = 14) -> float:
    if len(candles) < period + 1: return 0.0
    trs = [max(c["high"]-c["low"],
               abs(c["high"]-candles[i-1]["close"]),
               abs(c["low"] -candles[i-1]["close"]))
           for i, c in enumerate(candles) if i > 0]
    return sum(trs[-period:]) / period

def find_sr(candles: list, lookback: int = 20) -> tuple:
    if len(candles) < lookback: return 0, 0
    r = candles[-lookback:]
    return min(c["low"] for c in r), max(c["high"] for c in r)

def detect_structure(candles: list) -> str:
    if len(candles) < 10: return "UNKNOWN"
    highs = [c["high"] for c in candles[-10:]]
    lows  = [c["low"]  for c in candles[-10:]]
    if highs[-1] > max(highs[:-1]) and lows[-1] > min(lows[:-1]): return "BULLISH"
    if highs[-1] < max(highs[:-1]) and lows[-1] < min(lows[:-1]): return "BEARISH"
    return "RANGING"

def detect_fvg(candles: list) -> Optional[dict]:
    for i in range(len(candles)-3, max(0,len(candles)-15), -1):
        c1,c2,c3 = candles[i],candles[i+1],candles[i+2]
        if c3["low"]  > c1["high"]: return {"type":"BULLISH","low":c1["high"],"high":c3["low"]}
        if c3["high"] < c1["low"]:  return {"type":"BEARISH","low":c3["high"],"high":c1["low"]}
    return None

def detect_market_structure_detail(candles: list, lookback: int = 40) -> dict:
    """Identify HH/HL/LH/LL swing sequence for proper market structure analysis."""
    if len(candles) < 10:
        return {"structure": "UNKNOWN", "sequence": [], "last_swing_high": None, "last_swing_low": None}
    recent = candles[-lookback:]
    swing_highs, swing_lows = [], []
    for i in range(2, len(recent) - 2):
        if (recent[i]["high"] > recent[i-1]["high"] and recent[i]["high"] > recent[i-2]["high"] and
                recent[i]["high"] > recent[i+1]["high"] and recent[i]["high"] > recent[i+2]["high"]):
            swing_highs.append(recent[i]["high"])
        if (recent[i]["low"] < recent[i-1]["low"] and recent[i]["low"] < recent[i-2]["low"] and
                recent[i]["low"] < recent[i+1]["low"] and recent[i]["low"] < recent[i+2]["low"]):
            swing_lows.append(recent[i]["low"])
    sequence = []
    if len(swing_highs) >= 2:
        sequence.append("HH" if swing_highs[-1] > swing_highs[-2] else "LH")
    if len(swing_lows) >= 2:
        sequence.append("HL" if swing_lows[-1] > swing_lows[-2] else "LL")
    if "HH" in sequence and "HL" in sequence:   structure = "BULLISH_TREND"
    elif "LH" in sequence and "LL" in sequence: structure = "BEARISH_TREND"
    elif "HH" in sequence and "LL" in sequence: structure = "MIXED_VOLATILE"
    elif "LH" in sequence and "HL" in sequence: structure = "RANGING_COMPRESSED"
    else:                                        structure = "DEVELOPING"
    return {
        "structure": structure, "sequence": sequence,
        "last_swing_high": round(swing_highs[-1], 4) if swing_highs else None,
        "last_swing_low":  round(swing_lows[-1], 4)  if swing_lows  else None,
    }

def detect_order_blocks(candles: list) -> dict:
    """
    Bullish OB: last bearish candle before a strong bullish impulse.
    Bearish OB: last bullish candle before a strong bearish impulse.
    """
    if len(candles) < 10:
        return {"bullish_ob": None, "bearish_ob": None}
    recent   = candles[-40:]
    bull_ob  = None
    bear_ob  = None
    for i in range(1, len(recent) - 1):
        c    = recent[i]
        prev = recent[i - 1]
        body      = abs(c["close"] - c["open"])
        prev_body = abs(prev["close"] - prev["open"]) or 0.0001
        # Bullish impulse candle
        if c["close"] > c["open"] and body > prev_body * 1.3 and prev["close"] < prev["open"]:
            bull_ob = {"type": "BULLISH", "high": round(prev["high"], 4),
                       "low": round(prev["low"], 4),
                       "mid": round((prev["high"] + prev["low"]) / 2, 4)}
        # Bearish impulse candle
        if c["close"] < c["open"] and body > prev_body * 1.3 and prev["close"] > prev["open"]:
            bear_ob = {"type": "BEARISH", "high": round(prev["high"], 4),
                       "low": round(prev["low"], 4),
                       "mid": round((prev["high"] + prev["low"]) / 2, 4)}
    return {"bullish_ob": bull_ob, "bearish_ob": bear_ob}

def detect_liquidity_levels(candles: list, tolerance_pct: float = 0.12) -> dict:
    """
    Equal highs → buy-side liquidity (retail stops above).
    Equal lows  → sell-side liquidity (retail stops below).
    Liquidity sweep: price wick pierced the level but closed back inside.
    """
    if len(candles) < 20:
        return {"equal_highs_level": None, "equal_lows_level": None,
                "liquidity_sweep_above": False, "liquidity_sweep_below": False,
                "buy_side_liquidity": False, "sell_side_liquidity": False}
    recent  = candles[-30:]
    highs   = [c["high"] for c in recent]
    lows    = [c["low"]  for c in recent]
    max_h   = max(highs)
    min_l   = min(lows)
    eq_h    = [h for h in highs if abs(h - max_h) / max_h * 100 <= tolerance_pct]
    eq_l    = [l for l in lows  if abs(l - min_l) / min_l * 100 <= tolerance_pct]
    last    = candles[-1]
    sweep_above = last["high"] >= max_h and last["close"] < max_h   # wick above → possible SHORT
    sweep_below = last["low"]  <= min_l and last["close"] > min_l   # wick below → possible LONG
    return {
        "equal_highs_level":   round(max_h, 4) if len(eq_h) >= 2 else None,
        "equal_lows_level":    round(min_l, 4) if len(eq_l) >= 2 else None,
        "buy_side_liquidity":  len(eq_h) >= 2,
        "sell_side_liquidity": len(eq_l) >= 2,
        "liquidity_sweep_above": sweep_above,
        "liquidity_sweep_below": sweep_below,
    }

def detect_bos(candles: list) -> dict:
    """Break of Structure: close above prev swing high (bullish BOS) or below prev swing low (bearish BOS)."""
    if len(candles) < 15:
        return {"bos_detected": False, "type": None}
    analysis  = candles[-20:-3]
    if not analysis:
        return {"bos_detected": False, "type": None}
    prev_high = max(c["high"] for c in analysis)
    prev_low  = min(c["low"]  for c in analysis)
    cur_close = candles[-1]["close"]
    prv_close = candles[-2]["close"]
    if cur_close > prev_high and prv_close <= prev_high:
        return {"bos_detected": True, "type": "BULLISH_BOS",
                "broken_level": round(prev_high, 4), "signal": "LONG"}
    if cur_close < prev_low and prv_close >= prev_low:
        return {"bos_detected": True, "type": "BEARISH_BOS",
                "broken_level": round(prev_low, 4), "signal": "SHORT"}
    return {"bos_detected": False, "type": None,
            "key_high": round(prev_high, 4), "key_low": round(prev_low, 4)}

def detect_sr_interaction(candles: list, symbol: str) -> dict:
    """
    Detects what price is doing at Support / Resistance levels.

    Four scenarios:
    ─────────────────────────────────────────────────────────
    SUPPORT:
      BOUNCE_FROM_SUPPORT  → price approached support, wick touched, closed back above → LONG signal
      BREAK_BELOW_SUPPORT  → price closed below support with volume   → wait for confirmation
        └─ FAKE_BREAK_SUPPORT → price broke below but recovered above → LONG (stop-hunt reversal)
        └─ CONFIRMED_BREAK_SUPPORT → stayed below on next candle     → SHORT signal

    RESISTANCE:
      BOUNCE_FROM_RESISTANCE → price approached resistance, wick touched, closed back below → SHORT signal
      BREAK_ABOVE_RESISTANCE → price closed above resistance with volume → wait for confirmation
        └─ FAKE_BREAK_RESISTANCE → broke above but reversed back below → SHORT (stop-hunt reversal)
        └─ CONFIRMED_BREAK_RESISTANCE → stayed above on next candle   → LONG signal
    ─────────────────────────────────────────────────────────
    Uses orderbook imbalance, volume, and body/wick ratio for confirmation.
    """
    if len(candles) < 10:
        return {"scenario": "INSUFFICIENT_DATA", "signal": "WAIT", "confidence": 0}

    sup, res   = find_sr(candles, 30)
    price      = candles[-1]["close"]
    c0         = candles[-1]   # current candle
    c1         = candles[-2]   # previous candle
    c2         = candles[-3]   # two candles ago

    vols       = [c["volume"] for c in candles]
    avg_vol    = sum(vols[-20:]) / 20 if len(vols) >= 20 else sum(vols) / len(vols)
    vol_ratio  = vols[-1] / avg_vol if avg_vol > 0 else 1.0
    vol_prev   = vols[-2] / avg_vol if avg_vol > 0 else 1.0

    rng        = res - sup
    if rng == 0:
        return {"scenario": "NO_RANGE", "signal": "WAIT", "confidence": 0}

    # Zone thresholds: within 0.5% of level = "at the level"
    zone_pct   = 0.005
    at_sup     = abs(price - sup) / sup <= zone_pct or c0["low"] <= sup * (1 + zone_pct)
    at_res     = abs(price - res) / res <= zone_pct or c0["high"] >= res * (1 - zone_pct)

    body0      = abs(c0["close"] - c0["open"])
    wick_low0  = min(c0["open"], c0["close"]) - c0["low"]
    wick_hi0   = c0["high"] - max(c0["open"], c0["close"])
    candle_rng = c0["high"] - c0["low"]
    body_ratio = body0 / candle_rng if candle_rng > 0 else 0

    # ── SUPPORT SCENARIOS ────────────────────────────────────────
    if at_sup or c1["low"] <= sup * (1 + zone_pct):

        # 1. Price broke below support on previous candle, now check if recovered
        if c1["close"] < sup and c0["close"] > sup:
            # Recovered → FAKE BREAKOUT of support → LONG (stop hunt complete)
            return {
                "scenario":   "FAKE_BREAK_SUPPORT",
                "signal":     "LONG",
                "level":      round(sup, 4),
                "description":"Price broke below support then recovered above — stop hunt complete → LONG",
                "vol_ratio":  round(vol_ratio, 2),
                "confidence": 85 if vol_ratio >= 1.0 else 70,
                "entry_note": f"Enter LONG above {round(sup, 4)}, SL below the wick low {round(c0['low'], 4)}"
            }

        # 2. Price closed below support (possible real break) — need next candle confirmation
        if c0["close"] < sup:
            confirmed = c1["close"] < sup  # previous also below = confirmed break
            if confirmed and vol_prev >= 1.1:
                return {
                    "scenario":   "CONFIRMED_BREAK_SUPPORT",
                    "signal":     "SHORT",
                    "level":      round(sup, 4),
                    "description":"Support broken and price stayed below (confirmed) → SHORT. Old support now resistance.",
                    "vol_ratio":  round(vol_ratio, 2),
                    "confidence": 80 if vol_ratio >= 1.2 else 65,
                    "entry_note": f"Enter SHORT on retest of broken support {round(sup, 4)} as resistance, SL above {round(sup * 1.005, 4)}"
                }
            else:
                return {
                    "scenario":   "BREAK_BELOW_SUPPORT_UNCONFIRMED",
                    "signal":     "WAIT",
                    "level":      round(sup, 4),
                    "description":"Price closed below support — waiting for next candle to confirm real break vs fake",
                    "vol_ratio":  round(vol_ratio, 2),
                    "confidence": 0,
                    "entry_note": "Wait: if next candle stays below → SHORT. If it recovers above → LONG (fake breakout)"
                }

        # 3. Price touched support via wick but closed back above → BOUNCE → LONG
        if c0["low"] <= sup * (1 + zone_pct) and c0["close"] > sup and body_ratio >= 0.4:
            return {
                "scenario":   "BOUNCE_FROM_SUPPORT",
                "signal":     "LONG",
                "level":      round(sup, 4),
                "description":"Price wicked down to support and bounced back up → LONG",
                "vol_ratio":  round(vol_ratio, 2),
                "confidence": 80 if vol_ratio >= 1.0 and wick_low0 > body0 * 0.5 else 65,
                "entry_note": f"Enter LONG above {round(sup, 4)}, SL below support wick {round(c0['low'] * 0.999, 4)}"
            }

        # 4. Price approaching support but not yet touched
        if price > sup and (price - sup) / sup <= 0.008:
            return {
                "scenario":   "APPROACHING_SUPPORT",
                "signal":     "WAIT",
                "level":      round(sup, 4),
                "description":f"Price approaching support {round(sup,4)} — wait for bounce or break confirmation",
                "vol_ratio":  round(vol_ratio, 2),
                "confidence": 0,
                "entry_note": "Watch next 1-2 candles: bounce → LONG, break with volume → SHORT setup forming"
            }

    # ── RESISTANCE SCENARIOS ─────────────────────────────────────
    if at_res or c1["high"] >= res * (1 - zone_pct):

        # 1. Price broke above resistance on previous candle, now check if reversed
        if c1["close"] > res and c0["close"] < res:
            # Reversed → FAKE BREAKOUT of resistance → SHORT (stop hunt complete)
            return {
                "scenario":   "FAKE_BREAK_RESISTANCE",
                "signal":     "SHORT",
                "level":      round(res, 4),
                "description":"Price broke above resistance then reversed back below — stop hunt complete → SHORT",
                "vol_ratio":  round(vol_ratio, 2),
                "confidence": 85 if vol_ratio >= 1.0 else 70,
                "entry_note": f"Enter SHORT below {round(res, 4)}, SL above the wick high {round(c0['high'], 4)}"
            }

        # 2. Price closed above resistance (possible real break) — need next candle confirmation
        if c0["close"] > res:
            confirmed = c1["close"] > res  # previous also above = confirmed break
            if confirmed and vol_prev >= 1.1:
                return {
                    "scenario":   "CONFIRMED_BREAK_RESISTANCE",
                    "signal":     "LONG",
                    "level":      round(res, 4),
                    "description":"Resistance broken and price stayed above (confirmed) → LONG. Old resistance now support.",
                    "vol_ratio":  round(vol_ratio, 2),
                    "confidence": 80 if vol_ratio >= 1.2 else 65,
                    "entry_note": f"Enter LONG on retest of broken resistance {round(res, 4)} as support, SL below {round(res * 0.995, 4)}"
                }
            else:
                return {
                    "scenario":   "BREAK_ABOVE_RESISTANCE_UNCONFIRMED",
                    "signal":     "WAIT",
                    "level":      round(res, 4),
                    "description":"Price closed above resistance — waiting for next candle to confirm real break vs fake",
                    "vol_ratio":  round(vol_ratio, 2),
                    "confidence": 0,
                    "entry_note": "Wait: if next candle stays above → LONG. If it reverses below → SHORT (fake breakout)"
                }

        # 3. Price touched resistance via wick but closed back below → BOUNCE → SHORT
        if c0["high"] >= res * (1 - zone_pct) and c0["close"] < res and body_ratio >= 0.4:
            return {
                "scenario":   "BOUNCE_FROM_RESISTANCE",
                "signal":     "SHORT",
                "level":      round(res, 4),
                "description":"Price wicked up to resistance and got rejected back down → SHORT",
                "vol_ratio":  round(vol_ratio, 2),
                "confidence": 80 if vol_ratio >= 1.0 and wick_hi0 > body0 * 0.5 else 65,
                "entry_note": f"Enter SHORT below {round(res, 4)}, SL above resistance wick {round(c0['high'] * 1.001, 4)}"
            }

        # 4. Price approaching resistance but not yet touched
        if price < res and (res - price) / res <= 0.008:
            return {
                "scenario":   "APPROACHING_RESISTANCE",
                "signal":     "WAIT",
                "level":      round(res, 4),
                "description":f"Price approaching resistance {round(res,4)} — wait for rejection or break confirmation",
                "vol_ratio":  round(vol_ratio, 2),
                "confidence": 0,
                "entry_note": "Watch next 1-2 candles: rejection → SHORT, break with volume → LONG setup forming"
            }

    # Price is mid-range, not near any key level
    pos_pct = (price - sup) / rng * 100 if rng > 0 else 50
    return {
        "scenario":   "MID_RANGE",
        "signal":     "WAIT",
        "support":    round(sup, 4),
        "resistance": round(res, 4),
        "position_pct": round(pos_pct, 1),
        "description":f"Price is {round(pos_pct,1)}% through the range — not near S/R, wait for levels",
        "confidence": 0
    }

def get_psych_level(symbol: str, price: float) -> dict:
    levels = PSYCH_LEVELS.get(symbol, [])
    if not levels: return {"nearest_level": None, "distance_pct": None, "near_psych": False}
    nearest  = min(levels, key=lambda x: abs(x - price))
    dist_pct = abs(nearest - price) / price * 100
    return {"nearest_level": nearest, "distance_pct": round(dist_pct,2),
            "near_psych": dist_pct < 1.5, "above_psych": price > nearest}

def get_atr_filter(candles: list, symbol: str) -> dict:
    atr     = compute_atr(candles)
    price   = candles[-1]["close"]
    atr_pct = atr / price * 100 if price > 0 else 0
    min_atr = 0.1 if "BTC" in symbol else 0.12 if symbol in ["ETHUSDT","BNBUSDT"] else 0.15
    ok      = atr_pct >= min_atr
    return {"atr": round(atr,4), "atr_pct": round(atr_pct,3),
            "sufficient_volatility": ok,
            "advice": "Good volatility" if ok else f"Low volatility {atr_pct:.2f}% skip"}

def get_range_setup(candles: list, rsi: float) -> dict:
    if len(candles) < 30: return {"is_range": False}
    sup, res = find_sr(candles, 30)
    price    = candles[-1]["close"]
    rng      = res - sup
    if rng == 0: return {"is_range": False}
    pos         = (price - sup) / rng * 100
    near_sup    = pos <= 15 and rsi < 45
    near_res    = pos >= 85 and rsi > 55
    return {
        "is_range":        near_sup or near_res,
        "price_position":  round(pos, 1),
        "near_support":    near_sup,
        "near_resistance": near_res,
        "range_support":   round(sup, 4),
        "range_resistance":round(res, 4),
        "range_size_pct":  round(rng / sup * 100, 2) if sup > 0 else 0,
        "setup": "LONG_AT_SUPPORT" if near_sup else "SHORT_AT_RESISTANCE" if near_res else "MID_RANGE"
    }

# ─── BREAKOUT DETECTION ────────────────────────────────────────────────────────
def detect_breakout(candles: list, symbol: str) -> dict:
    """
    Detects whether price has broken support or resistance with volume confirmation.
    Used to allow counter-sentiment trades (e.g. SHORT in Extreme Fear if support broke).
    Also flags potential fake breakouts for the AI to consider.
    """
    if len(candles) < 30:
        return {"breakout_detected": False, "type": None}

    sup, res  = find_sr(candles, 30)
    price     = candles[-1]["close"]
    prev_close= candles[-2]["close"]
    vols      = [c["volume"] for c in candles]
    avg_vol   = sum(vols[-20:]) / 20 if len(vols) >= 20 else sum(vols) / len(vols)
    last_vol  = vols[-1]
    vol_ratio = last_vol / avg_vol if avg_vol > 0 else 1.0

    # Support break: price closed below support
    support_broken = prev_close > sup and price < sup
    # Resistance break: price closed above resistance
    resistance_broken = prev_close < res and price > res

    # Fake breakout signals:
    # - Low volume on breakout (vol_ratio < 1.2)
    # - Candle closed back inside range (wick extended but close reversed)
    last_candle  = candles[-1]
    candle_range = last_candle["high"] - last_candle["low"]
    body         = abs(last_candle["close"] - last_candle["open"])
    body_ratio   = body / candle_range if candle_range > 0 else 0

    fake_breakout_risk = vol_ratio < 1.2 or body_ratio < 0.4  # low vol or long wick = fake risk

    if support_broken:
        return {
            "breakout_detected": True,
            "type": "SUPPORT_BREAK",
            "direction_signal": "SHORT",
            "broken_level": round(sup, 4),
            "volume_ratio": round(vol_ratio, 2),
            "volume_confirmed": vol_ratio >= 1.2,
            "fake_breakout_risk": fake_breakout_risk,
            "fake_breakout_note": (
                "⚠️ LOW VOLUME breakout — possible fakeout, wait for retest"
                if not (vol_ratio >= 1.2)
                else ("⚠️ Long wick / small body — possible fakeout candle"
                      if body_ratio < 0.4 else "Breakout looks genuine")
            )
        }
    elif resistance_broken:
        return {
            "breakout_detected": True,
            "type": "RESISTANCE_BREAK",
            "direction_signal": "LONG",
            "broken_level": round(res, 4),
            "volume_ratio": round(vol_ratio, 2),
            "volume_confirmed": vol_ratio >= 1.2,
            "fake_breakout_risk": fake_breakout_risk,
            "fake_breakout_note": (
                "⚠️ LOW VOLUME breakout — possible fakeout, wait for retest"
                if not (vol_ratio >= 1.2)
                else ("⚠️ Long wick / small body — possible fakeout candle"
                      if body_ratio < 0.4 else "Breakout looks genuine")
            )
        }

    return {"breakout_detected": False, "type": None, "volume_ratio": round(vol_ratio, 2)}

def build_payload(symbol: str) -> dict:
    # Scalping-first: 5m for fast entry timing, 15m for structure, 1h for bias
    c5m = get_klines(symbol, "5m",  60)
    c15 = get_klines(symbol, "15m", 60)
    c1h = get_klines(symbol, "1h",  50)
    c4h = get_klines(symbol, "4h",  30)
    if not c15: return {}

    cl5m = [c["close"] for c in c5m] if c5m else []
    cl15 = [c["close"] for c in c15]
    cl1h = [c["close"] for c in c1h] if c1h else cl15
    cl4h = [c["close"] for c in c4h] if c4h else cl15
    vols5m = [c["volume"] for c in c5m] if c5m else []
    vols15 = [c["volume"] for c in c15]

    rsi5m = compute_rsi(cl5m) if cl5m else 50.0
    rsi15 = compute_rsi(cl15)
    rsi1h = compute_rsi(cl1h)
    rsi4h = compute_rsi(cl4h)
    mv5m, ms5m, mh5m = compute_macd(cl5m) if cl5m else (0,0,0)
    mv, ms, mh        = compute_macd(cl15)
    mv1h,_,mh1h       = compute_macd(cl1h) if c1h else (0,0,0)
    e9   = compute_ema(cl15, 9)
    e21  = compute_ema(cl15, 21)
    e50  = compute_ema(cl15, 50)
    e200 = compute_ema(cl15, 200)
    e50_4h  = compute_ema(cl4h, 50)  if c4h else []
    s5m, r5m  = find_sr(c5m, 20)  if c5m else (0, 0)
    s15, r15  = find_sr(c15)
    s1h, r1h  = find_sr(c1h, 20)  if c1h else (0, 0)

    ticker = get_ticker_24h(symbol)
    oi     = get_open_interest(symbol)
    fr     = get_funding_rate(symbol)
    price  = cl15[-1]

    avg_v15 = sum(vols15[-20:])/20 if len(vols15)>=20 else sum(vols15)/max(len(vols15),1)
    avg_v5m = sum(vols5m[-20:])/20 if len(vols5m)>=20 else sum(vols5m)/max(len(vols5m),1)

    # ── Deep order book + volume delta ──────────────────────────
    ob_analysis = analyze_orderbook(symbol)
    vol_delta   = get_volume_delta(c5m if c5m else c15)

    # ── Advanced TA ─────────────────────────────────────────────
    ms_detail   = detect_market_structure_detail(c15)
    ms_4h       = detect_market_structure_detail(c4h) if c4h else {}
    order_blocks= detect_order_blocks(c15)
    liq_levels  = detect_liquidity_levels(c15)
    bos         = detect_bos(c15)
    bos_1h      = detect_bos(c1h) if c1h else {}
    fvg_4h      = detect_fvg(c4h) if c4h else None

    return {
        "symbol": symbol, "current_price": price,
        # ── ORDER BOOK (primary signal) ──
        "orderbook": ob_analysis,          # BUY/SELL/NEUTRAL + confidence + walls
        "volume_delta": vol_delta,         # buyers_in_control, cvd_trend
        # ── RSI multi-TF ──
        "rsi_5m": round(rsi5m,2), "rsi_15m": round(rsi15,2),
        "rsi_1h": round(rsi1h,2), "rsi_4h":  round(rsi4h,2),
        # ── MACD ──
        "macd_5m": round(mv5m,6), "macd_hist_5m": round(mh5m,6),
        "macd":    round(mv,6),   "macd_hist":     round(mh,6),
        "macd_1h": round(mv1h,6), "macd_hist_1h":  round(mh1h,6),
        # ── EMA (fast EMAs for scalping) ──
        "ema9":  round(e9[-1],4)  if e9  else None,
        "ema21": round(e21[-1],4) if e21 else None,
        "ema50": round(e50[-1],4) if e50 else None,
        "ema200":round(e200[-1],4)if e200 else None,
        "ema50_4h": round(e50_4h[-1],4) if e50_4h else None,
        "price_vs_ema9":   "ABOVE" if e9   and price > e9[-1]   else "BELOW",
        "price_vs_ema21":  "ABOVE" if e21  and price > e21[-1]  else "BELOW",
        "price_vs_ema50":  "ABOVE" if e50  and price > e50[-1]  else "BELOW",
        "price_vs_ema200": "ABOVE" if e200 and price > e200[-1] else "BELOW",
        # ── S/R multi-TF ──
        "support_5m":  round(s5m,4), "resistance_5m": round(r5m,4),
        "support_15m": round(s15,4), "resistance_15m":round(r15,4),
        "support_1h":  round(s1h,4), "resistance_1h": round(r1h,4),
        # ── Market Structure ──
        "market_structure":     detect_structure(c15),
        "market_structure_15m": ms_detail,
        "market_structure_4h":  ms_4h,
        # ── Smart Money ──
        "order_blocks": order_blocks,
        "liquidity":    liq_levels,
        "bos_15m":      bos,
        "bos_1h":       bos_1h,
        # ── FVG ──
        "fvg_5m":  detect_fvg(c5m) if c5m else None,
        "fvg_15m": detect_fvg(c15),
        "fvg_4h":  fvg_4h,
        # ── S/R Interaction ──
        "sr_interaction_5m":  detect_sr_interaction(c5m,  symbol) if c5m else {},
        "sr_interaction_15m": detect_sr_interaction(c15,  symbol),
        "sr_interaction_1h":  detect_sr_interaction(c1h,  symbol) if c1h else {},
        # ── Volume ──
        "volume_ratio_5m":  round(vols5m[-1]/avg_v5m if avg_v5m>0 and vols5m else 1, 2),
        "volume_ratio_15m": round(vols15[-1]/avg_v15 if avg_v15>0 else 1, 2),
        # ── Other ──
        "atr_filter":  get_atr_filter(c15, symbol),
        "range_setup": get_range_setup(c15, rsi15),
        "psych_level": get_psych_level(symbol, price),
        "breakout":    detect_breakout(c15, symbol),
        "price_change_24h": float(ticker.get("priceChangePercent",0)),
        "open_interest": float(oi.get("openInterest",0)),
        "funding_rate":  float(fr.get("fundingRate",0)) if fr else 0,
        "last_3_candles_5m": c5m[-3:] if c5m else [],
    }

# ─── AI ANALYSIS ──────────────────────────────────────────────────────────────
def _warmup_ollama():
    """Send a tiny ping to Ollama so the model is loaded and ready.
    Called after a reconnect gap to avoid the 'timed out waiting for llama runner'
    error that happens when the model gets unloaded while the laptop was asleep.
    """
    try:
        client = OpenAI(base_url=OLLAMA_BASE_URL, api_key=AI_API_KEY, max_retries=0)
        client.chat.completions.create(
            model=OLLAMA_MODEL, max_tokens=1, temperature=0,
            timeout=30,
            messages=[{"role": "user", "content": "hi"}]
        )
        log.info("[OLLAMA] Model warm-up OK — runner is ready")
    except Exception as e:
        log.warning(f"[OLLAMA] Warm-up ping failed (model may still be loading): {e}")

def _format_analysis_for_llm(d: dict) -> str:
    """
    Convert a raw payload dict into a readable text block for the LLM.
    S/R interaction is placed at the very top as the PRIMARY signal.
    Orderbook, RSI, EMA, BOS are labelled as CONFIRMATORY signals.
    """
    sym   = d["symbol"]
    price = d["current_price"]
    ob    = d.get("orderbook", {})
    vd    = d.get("volume_delta", {})
    sr5   = d.get("sr_interaction_5m",  {})
    sr15  = d.get("sr_interaction_15m", {})
    sr1h  = d.get("sr_interaction_1h",  {})
    ms15  = d.get("market_structure_15m", {})
    ms4h  = d.get("market_structure_4h",  {})
    bos15 = d.get("bos_15m", {})
    bos1h = d.get("bos_1h",  {})
    liq   = d.get("liquidity", {})
    obs   = d.get("order_blocks", {})
    fvg15 = d.get("fvg_15m")
    bko   = d.get("breakout", {})
    atr   = d.get("atr_filter", {})

    # Build a one-line S/R summary so the model sees the actionable signal immediately
    sr_parts = []
    for label, sr in [("5m", sr5), ("15m", sr15), ("1h", sr1h)]:
        sig  = sr.get("signal", "WAIT")
        scen = sr.get("scenario", "")
        conf = sr.get("confidence", 0)
        if sig in ("LONG", "SHORT") and scen:
            sr_parts.append(f"{label}:{scen}→{sig}(conf={conf})")
    sr_summary = " | ".join(sr_parts) if sr_parts else "NO SIGNAL — price mid-range, not at S/R level"

    return (
        f"=== {sym} | ENTRY PRICE = {price} (use exactly for entry_price) ===\n"
        f"── PRIMARY S/R SIGNALS (decide trade direction from these) ──\n"
        f"SIGNAL SUMMARY : {sr_summary}\n"
        f"SR_INT 5m  : {sr5.get('scenario','?')} → {sr5.get('signal','WAIT')}"
        f"  conf={sr5.get('confidence',0)}  | hint: {sr5.get('entry_note','')}\n"
        f"SR_INT 15m : {sr15.get('scenario','?')} → {sr15.get('signal','WAIT')}"
        f"  conf={sr15.get('confidence',0)} | hint: {sr15.get('entry_note','')}\n"
        f"SR_INT 1h  : {sr1h.get('scenario','?')} → {sr1h.get('signal','WAIT')}"
        f"  conf={sr1h.get('confidence',0)} | hint: {sr1h.get('entry_note','')}\n"
        f"BREAKOUT   : type={bko.get('type','None')}  vol_confirmed={bko.get('volume_confirmed')}"
        f"  fake_risk={bko.get('fake_breakout_risk')}  note={bko.get('fake_breakout_note','')}\n"
        f"BOS        : 15m={bos15.get('type','None')}  1h={bos1h.get('type','None')}\n"
        f"STRUCTURE  : 15m={ms15.get('structure','?')}  4h={ms4h.get('structure','?')}\n"
        f"S/R levels : 5m sup={d.get('support_5m')} res={d.get('resistance_5m')}"
        f"  | 15m sup={d.get('support_15m')} res={d.get('resistance_15m')}"
        f"  | 1h sup={d.get('support_1h')} res={d.get('resistance_1h')}\n"
        f"── CONFIRMATORY SIGNALS (use to add confluence) ──\n"
        f"ORDERBOOK  : signal={ob.get('signal')} conf={ob.get('confidence')}%"
        f"  imb={ob.get('weighted_imb')}x  bid_walls={ob.get('bid_walls',[])}  ask_walls={ob.get('ask_walls',[])}\n"
        f"VOL DELTA  : {vd.get('delta_signal')} | {vd.get('cvd_trend')}"
        f"  buyers_in_control={vd.get('buyers_in_control')}\n"
        f"RSI        : 5m={d.get('rsi_5m')}  15m={d.get('rsi_15m')}"
        f"  1h={d.get('rsi_1h')}  4h={d.get('rsi_4h')}\n"
        f"MACD hist  : 5m={d.get('macd_hist_5m')}  15m={d.get('macd_hist')}"
        f"  1h={d.get('macd_hist_1h')}\n"
        f"EMA align  : vs_ema9={d.get('price_vs_ema9')}  vs_ema21={d.get('price_vs_ema21')}"
        f"  vs_ema50={d.get('price_vs_ema50')}\n"
        f"LIQUIDITY  : sweep_above={liq.get('liquidity_sweep_above')}"
        f"  sweep_below={liq.get('liquidity_sweep_below')}\n"
        f"ORDER BLK  : bullish={obs.get('bullish_ob')}  bearish={obs.get('bearish_ob')}\n"
        f"FVG 15m    : {fvg15}\n"
        f"ATR        : {atr.get('atr_pct')}%  sufficient={atr.get('sufficient_volatility')}\n"
        f"VOLUME     : ratio_5m={d.get('volume_ratio_5m')}  ratio_15m={d.get('volume_ratio_15m')}\n"
        f"FUNDING    : {d.get('funding_rate')}  24h_chg={d.get('price_change_24h')}%\n"
    )


def analyze_with_ai(analyses, balance, open_count, fg, btc, ev, market_regime="NEUTRAL", blocked_symbols=None) -> dict:
    # max_retries=0 — do NOT retry on timeout. One attempt only.
    # If Ollama is busy the retry loop was causing 3×3min = 9min hangs that
    # also dropped the WebSocket connection (Binance closes idle WS after ~3min).
    client = OpenAI(base_url=OLLAMA_BASE_URL, api_key=AI_API_KEY, max_retries=0)

    system = """You are a crypto futures trading AI. Your primary job is to identify Support/Resistance setups and trade them with confirmation.

PRICE RULE (CRITICAL):
- Every symbol block shows "ENTRY PRICE = X". Your entry_price MUST equal that exact number.
- SL and TP are % offsets from that price. NEVER guess or invent a price.

═══════════════════════════════════════════════
PRIMARY SETUPS — trade direction comes from S/R
═══════════════════════════════════════════════

SETUP 1 ► LONG — Confirmed Resistance Break
  Trigger : SR_INT = CONFIRMED_BREAK_RESISTANCE on any timeframe
  Logic   : Price closed above resistance AND stayed above → old resistance is new support → BUY
  Requires: volume_ratio > 1.1 OR BOS = BULLISH_BOS OR RSI < 75
  Trade   : LONG at current price, TP toward next resistance, SL just below broken level

SETUP 2 ► SHORT — Bounce / Rejection from Resistance
  Trigger : SR_INT = BOUNCE_FROM_RESISTANCE or FAKE_BREAK_RESISTANCE on any timeframe
  Logic   : Price wicked up to resistance but closed back below → sellers in control → SELL
  Requires: RSI > 55 OR OB=SELL OR bearish candle body OR volume_ratio > 0.8
  Trade   : SHORT at current price, TP toward support, SL just above the wick high

SETUP 3 ► SHORT — Confirmed Support Break
  Trigger : SR_INT = CONFIRMED_BREAK_SUPPORT on any timeframe
  Logic   : Price closed below support AND stayed below → old support is new resistance → SELL
  Requires: volume_ratio > 1.1 OR BOS = BEARISH_BOS OR RSI > 25
  Trade   : SHORT at current price, TP toward next support, SL just above broken level

SETUP 4 ► LONG — Bounce / Rejection from Support
  Trigger : SR_INT = BOUNCE_FROM_SUPPORT or FAKE_BREAK_SUPPORT on any timeframe
  Logic   : Price wicked down to support but closed back above → buyers stepped in → BUY
  Requires: RSI < 45 OR OB=BUY OR bullish candle body OR volume_ratio > 0.8
  Trade   : LONG at current price, TP toward resistance, SL just below the wick low

SETUP 5 ► TREND CONTINUATION (only when NO S/R signal on any TF)
  Trigger : BOS detected (BULLISH_BOS → LONG, BEARISH_BOS → SHORT) on 1h or 15m
  Requires: Market structure aligned on 4h + EMA stack aligned + RSI in trend zone (40-70 for LONG, 30-60 for SHORT)

═══════════════════════════════════════════════
CONFLUENCE SCORING — count how many align:
═══════════════════════════════════════════════
[1] SR_INT signal matches trade direction        ← PRIMARY (worth 2 points)
[2] Breakout volume confirmed (vol_ratio > 1.1)  ← strong add-on
[3] BOS on 15m or 1h matches direction           ← strong add-on
[4] OB signal matches direction                  ← confirmatory
[5] RSI supports direction (< 45 LONG, > 55 SHORT)
[6] EMA21 aligned (price above for LONG, below for SHORT)
[7] Volume delta (buyers_in_control for LONG, sellers for SHORT)
[8] Orderblock nearby matches direction

Trade if total score >= 3 points (S/R counts as 2).
Higher score = higher confidence and win_probability.

═══════════════════════════════════════════════
SKIP RULES — do NOT trade if:
═══════════════════════════════════════════════
- ALL SR_INT signals = WAIT or MID_RANGE (price not at any S/R level) AND no BOS
- volume_ratio_5m < 0.3 (no participation)
- econ_event should_skip = true
- Fake breakout risk is high AND no volume confirmation AND OB neutral
- RSI > 80 for LONG entry or RSI < 20 for SHORT entry (extreme exhaustion)
NOTE: ATR sufficient=false is a WARNING only — you may still trade a clear S/R bounce/break
      but use a slightly wider SL (add 10% to normal SL) to avoid noise-hits.

═══════════════════════════════════════════════
TP / SL SIZING
═══════════════════════════════════════════════
Bounce trades  : TP = full range to opposite S/R level, SL = just beyond the wick
Break trades   : TP = 0.5–1.0× the broken range, SL = just inside broken level
- BTC/ETH   : TP 0.3–0.7%,  SL 0.2–0.45%
- SOL/BNB   : TP 0.4–0.9%,  SL 0.25–0.55%
- Small caps: TP 0.5–1.3%,  SL 0.3–0.75%
LONG : TP = entry × (1 + tp_pct),  SL = entry × (1 - sl_pct)
SHORT: TP = entry × (1 - tp_pct),  SL = entry × (1 + sl_pct)
Risk/Reward must be >= 1.5.

HARD LIMITS: leverage 20x–50x | position_size_usdt min $20 | win_probability >= 75 | risk_reward >= 1.5

FEAR & GREED (soft bias — S/R setup overrides this):
- EXTREME FEAR  (<=25): prefer SHORT; LONG only if CONFIRMED_BREAK_RESISTANCE with volume
- EXTREME GREED (>=76): prefer LONG;  SHORT only if CONFIRMED_BREAK_SUPPORT with volume
- Otherwise: both directions freely allowed based on S/R setup

TRADE TYPES: SR_BOUNCE | SR_BREAK | FAKE_BREAK | BOS_BREAK | TREND | FVG | SMC

OUTPUT — JSON only, no markdown, no text outside the JSON object:
{"action":"TRADE","symbol":"BTCUSDT","trade_type":"SR_BOUNCE","direction":"SHORT","confidence":80,"win_probability":74,"leverage":25,"entry_price":83500.0,"stop_loss":84100.0,"take_profit":82600.0,"position_size_usdt":30.0,"risk_reward":1.5,"expected_profit_usdt":4.5,"reasoning":"15m BOUNCE_FROM_RESISTANCE conf=80, RSI_15m=68 overbought, OB=SELL conf=72, wick rejected at 83900 resistance","invalidation":"Candle closes above 84100","estimated_duration_hours":2}

If no valid setup found:
{"action":"WAIT","reason":"explain which setup was closest and why it did not qualify"}"""

    blocked_symbols = blocked_symbols or {}
    blocked_str = "\n".join(f"  BLOCKED: {k} — {v}" for k, v in blocked_symbols.items()) or "  None"

    if market_regime == "BULL":
        regime_rule = "BTC orderbook BULLISH — only LONG on alts, no alt SHORTs."
    elif market_regime == "BEAR":
        regime_rule = "BTC orderbook BEARISH — only SHORT on alts, no alt LONGs."
    else:
        regime_rule = "BTC neutral — both directions allowed."

    # Build condensed text summary (much better for small LLMs than raw JSON)
    market_data_str = "\n".join(_format_analysis_for_llm(d) for d in analyses)

    msg = (
        f"CONTEXT:\n"
        f"Fear&Greed={fg['value']}/100 ({fg['signal']}) | override: {fg['breakout_override_note']}\n"
        f"BTC Dominance={btc['btc_dominance']}% ({btc['signal']}) | avoid_alts={btc['avoid_alts']}\n"
        f"Economic event: {ev['advice']} | should_skip={ev['should_skip']}\n"
        f"BTC Regime: {market_regime} — {regime_rule}\n"
        f"Portfolio: balance=${balance:.2f} | open={open_count}/{MAX_OPEN_TRADES} | slots={MAX_OPEN_TRADES - open_count}\n"
        f"\nBLOCKED SYMBOLS:\n{blocked_str}\n"
        f"\nMARKET DATA ({len(analyses)} pairs):\n"
        f"{market_data_str}\n"
        f"\nRemember: entry_price MUST equal the CURRENT PRICE shown for the symbol you pick.\n"
        f"Pick the SINGLE best trade or return WAIT."
    )

    try:
        resp = client.chat.completions.create(
            model=OLLAMA_MODEL, max_tokens=350,
            temperature=0.1,
            timeout=90,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": msg}
            ]
        )
        raw = resp.choices[0].message.content.strip()
        log.info(f"[AI] raw response ({len(raw)} chars): {raw[:600]}")

        # Guard: empty response from model
        if not raw:
            log.error("AI error: model returned empty response")
            return {"action": "WAIT", "reason": "Model returned empty response"}

        # Strip markdown code fences if present
        if "```" in raw:
            parts = raw.split("```")
            # take the first fenced block content
            raw = parts[1] if len(parts) > 1 else parts[0]
            if raw.startswith("json"):
                raw = raw[4:]

        raw = raw.strip()

        # Guard: still empty after stripping
        if not raw:
            log.error("AI error: empty JSON after stripping markdown")
            return {"action": "WAIT", "reason": "Empty JSON after stripping"}

        # Extract first JSON object if model added extra text
        start = raw.find("{")
        end   = raw.rfind("}") + 1
        if start != -1 and end > start:
            raw = raw[start:end]

        result = json.loads(raw)
        log.info(f"[AI] action={result.get('action')} symbol={result.get('symbol','-')} "
                 f"direction={result.get('direction','-')} entry={result.get('entry_price','-')}")
        return result
    except json.JSONDecodeError as e:
        log.error(f"AI JSON parse error: {e} | raw='{raw[:200]}'")
        return {"action": "WAIT", "reason": f"JSON parse error: {e}"}
    except Exception as e:
        log.error(f"AI error: {e}")
        return {"action": "WAIT", "reason": str(e)}

# ─── TRADE MANAGEMENT ─────────────────────────────────────────────────────────
MIN_POSITION_USDT = 20.0   # hard minimum per trade
MIN_LEVERAGE      = 20     # hard minimum leverage
SL_COOLDOWN_MINS  = 45     # minutes to block a symbol+direction after SL hit
HARD_TIMEOUT_HRS  = 4      # absolute max trade duration regardless of estimated_duration_hours

def is_on_cooldown(state: dict, symbol: str, direction: str):
    """Returns (True, mins_remaining) if symbol+direction is blocked after recent SL, else (False, 0)."""
    key = f"{symbol}_{direction}"
    cooldowns = state.get("sl_cooldowns", {})
    if key in cooldowns:
        try:
            blocked_until = datetime.fromisoformat(cooldowns[key])
            if datetime.now() < blocked_until:
                mins_left = int((blocked_until - datetime.now()).total_seconds() / 60)
                return True, mins_left
            else:
                del cooldowns[key]   # expired — remove it
        except Exception:
            del cooldowns[key]
    return False, 0

def open_trade(state, decision) -> dict:
    # ── Enforce hard minimums ─────────────────────────────────────
    lev  = max(MIN_LEVERAGE, min(MAX_LEVERAGE, int(decision.get("leverage", MIN_LEVERAGE))))
    size = max(MIN_POSITION_USDT, float(decision.get("position_size_usdt", MIN_POSITION_USDT)))
    # Re-calculate expected profit with enforced values
    entry = float(decision["entry_price"])
    tp    = float(decision["take_profit"])
    sl    = float(decision["stop_loss"])

    # ── GUARD: validate AI entry price against real market price ──
    sym = decision.get("symbol", "")
    real_price = get_current_price(sym)
    if real_price > 0:
        deviation = abs(entry - real_price) / real_price
        log.info(f"[PRICE GUARD] {sym}: AI_entry={entry:.4f} market={real_price:.4f} deviation={deviation*100:.2f}%")
        if deviation > 0.02:  # AI price deviates >2% from real market — hallucination
            msg = (f"[PRICE GUARD] REJECT {sym}: AI entry {entry:.4f} deviates "
                   f"{deviation*100:.1f}% from market {real_price:.4f} — discarding trade")
            log.error(msg)
            print(msg)
            return {}
        # Use actual current price as entry (more accurate than AI's snapshot)
        entry = real_price

    # ── GUARD: SL and TP must be on correct side of entry ─────────
    direction = decision["direction"]
    if direction == "LONG":
        if sl >= entry:
            msg = f"[PRICE GUARD] REJECT {sym}: LONG stop_loss {sl} >= entry {entry:.4f}"
            log.error(msg); print(msg); return {}
        if tp <= entry:
            msg = f"[PRICE GUARD] REJECT {sym}: LONG take_profit {tp} <= entry {entry:.4f}"
            log.error(msg); print(msg); return {}
    else:
        if sl <= entry:
            msg = f"[PRICE GUARD] REJECT {sym}: SHORT stop_loss {sl} <= entry {entry:.4f}"
            log.error(msg); print(msg); return {}
        if tp >= entry:
            msg = f"[PRICE GUARD] REJECT {sym}: SHORT take_profit {tp} >= entry {entry:.4f}"
            log.error(msg); print(msg); return {}
    if decision["direction"] == "LONG":
        exp_profit = size * (tp - entry) / entry * lev if entry > 0 else 0
    else:
        exp_profit = size * (entry - tp) / entry * lev if entry > 0 else 0

    trade = {
        "id": f"SIM_{int(time.time())}",
        "symbol":      decision["symbol"],
        "trade_type":  decision.get("trade_type","TREND"),
        "direction":   decision["direction"],
        "confidence":  decision.get("confidence",0),
        "win_probability": decision.get("win_probability",0),
        "leverage":    lev,
        "entry_price": entry,
        "stop_loss":   sl,
        "take_profit": tp,
        "position_size": size,
        "risk_reward": decision.get("risk_reward",2.0),
        "expected_profit": round(exp_profit, 2),
        "reasoning":   decision.get("reasoning",""),
        "invalidation":decision.get("invalidation",""),
        "filters_passed": decision.get("filters_passed",""),
        "estimated_duration_hours": decision.get("estimated_duration_hours",4),
        "open_time":   datetime.now().isoformat(),
        "status":      "OPEN",
        "current_pnl": 0.0, "current_pnl_pct": 0.0,
        "current_price": entry,
    }
    with _state_lock:
        state["open_trades"].append(trade)

    e  = "🟢" if trade["direction"]=="LONG" else "🔴"
    c  = trade["symbol"].replace("USDT","")
    tt = trade["trade_type"]
    print(f"""
╔════════════════════════════════════════════════════════════════╗
║  {c:<6} FUTURES — {e} {trade['direction']:<5}  [{tt:<8}]  [OPEN]             ║
╠════════════════════════════════════════════════════════════════╣
║  Confidence: {trade['confidence']}%  |  Win Prob: {trade['win_probability']}%  |  RR: 1:{trade['risk_reward']:.1f}      ║
╠════════════════════════════════════════════════════════════════╣
║  Leverage: {trade['leverage']}x  |  Size: ${trade['position_size']:.2f}  |  Expected: ~${trade['expected_profit']:.2f}   ║
╠════════════════════════════════════════════════════════════════╣
║  Entry: ${trade['entry_price']:<12.4f}  SL: ${trade['stop_loss']:<12.4f}  TP: ${trade['take_profit']:<10.4f} ║
╠════════════════════════════════════════════════════════════════╣
║  {trade['reasoning'][:64]:<64} ║
╚════════════════════════════════════════════════════════════════╝""")

    log.info(f"[OPEN] {trade['symbol']} {trade['direction']} {tt} @ {trade['entry_price']}")
    save_state(state)
    return trade

def update_open_trades(state):
    with _state_lock:
        for trade in state["open_trades"][:]:
            cp = get_current_price(trade["symbol"])
            if cp == 0: continue
            # Skip if trade was already closed by another thread
            if trade["id"] not in {t["id"] for t in state["open_trades"]}:
                continue
            e, sl, tp = trade["entry_price"], trade["stop_loss"], trade["take_profit"]
            sz, lv, d = trade["position_size"], trade["leverage"], trade["direction"]

            if d == "LONG":
                pnl_u = sz * (cp-e)/e * lv
                pnl_p = (cp-e)/e * 100 * lv
            else:
                pnl_u = sz * (e-cp)/e * lv
                pnl_p = (e-cp)/e * 100 * lv

            # Cap loss at position size (liquidation floor)
            pnl_u = max(-sz, pnl_u)
            pnl_p = max(-100 * lv, pnl_p)

            trade["current_pnl"]     = round(pnl_u, 4)
            trade["current_pnl_pct"] = round(pnl_p, 2)
            trade["current_price"]   = cp

            tp_hit = (d=="LONG" and cp>=tp) or (d=="SHORT" and cp<=tp)
            sl_hit = (d=="LONG" and cp<=sl) or (d=="SHORT" and cp>=sl)
            hrs    = (datetime.now()-datetime.fromisoformat(trade["open_time"])).total_seconds()/3600

            # Hard cap: close after HARD_TIMEOUT_HRS regardless of estimated_duration_hours
            timed_out = hrs > HARD_TIMEOUT_HRS
            if tp_hit or sl_hit or timed_out:
                close_trade(state, trade, cp, "TP" if tp_hit else "SL" if sl_hit else "TIMEOUT")

    save_state(state)

def update_open_trades_ws(state, latest_prices: dict):
    """WebSocket variant — uses streaming mark prices instead of HTTP."""
    with _state_lock:
        for trade in state["open_trades"][:]:
            cp = latest_prices.get(trade["symbol"], 0.0)
            if cp == 0: continue
            # Skip if trade was already closed by another thread
            if trade["id"] not in {t["id"] for t in state["open_trades"]}:
                continue
            e, sl, tp = trade["entry_price"], trade["stop_loss"], trade["take_profit"]
            sz, lv, d = trade["position_size"], trade["leverage"], trade["direction"]

            if d == "LONG":
                pnl_u = sz * (cp-e)/e * lv
                pnl_p = (cp-e)/e * 100 * lv
            else:
                pnl_u = sz * (e-cp)/e * lv
                pnl_p = (e-cp)/e * 100 * lv

            # Cap loss at position size (liquidation floor)
            pnl_u = max(-sz, pnl_u)
            pnl_p = max(-100 * lv, pnl_p)

            trade["current_pnl"]     = round(pnl_u, 4)
            trade["current_pnl_pct"] = round(pnl_p, 2)
            trade["current_price"]   = cp

            tp_hit = (d=="LONG" and cp>=tp) or (d=="SHORT" and cp<=tp)
            sl_hit = (d=="LONG" and cp<=sl) or (d=="SHORT" and cp>=sl)
            hrs    = (datetime.now()-datetime.fromisoformat(trade["open_time"])).total_seconds()/3600
            timed_out = hrs > HARD_TIMEOUT_HRS
            if tp_hit or sl_hit or timed_out:
                close_trade(state, trade, cp, "TP" if tp_hit else "SL" if sl_hit else "TIMEOUT")

    save_state(state)

def close_trade(state, trade, cp, reason):
    e, sz, lv, d = trade["entry_price"], trade["position_size"], trade["leverage"], trade["direction"]
    pnl = sz*(cp-e)/e*lv if d=="LONG" else sz*(e-cp)/e*lv
    # Cap loss at position size (simulates liquidation — can never lose more than you put in)
    pnl = max(-sz, pnl)

    trade.update({"close_price":cp,"close_time":datetime.now().isoformat(),
                  "realized_pnl":round(pnl,4),"close_reason":reason,"status":"CLOSED"})
    state["balance"]      = round(state["balance"]+pnl, 4)
    state["total_profit"] = round(state["total_profit"]+pnl, 4)
    if pnl > 0: state["win_count"]  += 1
    else:       state["loss_count"] += 1
    state["open_trades"]   = [t for t in state["open_trades"] if t["id"]!=trade["id"]]
    state["closed_trades"].append(trade)
    # Keep only the last 200 closed trades to prevent state file from growing unboundedly
    if len(state["closed_trades"]) > 200:
        state["closed_trades"] = state["closed_trades"][-200:]

    # Record SL cooldown — block same symbol+direction for SL_COOLDOWN_MINS
    if reason == "SL":
        if "sl_cooldowns" not in state:
            state["sl_cooldowns"] = {}
        blocked_until = (datetime.now() + timedelta(minutes=SL_COOLDOWN_MINS)).isoformat()
        key = f"{trade['symbol']}_{trade['direction']}"
        state["sl_cooldowns"][key] = blocked_until
        log.info(f"[COOLDOWN] {trade['symbol']} {trade['direction']} blocked for {SL_COOLDOWN_MINS}min after SL hit")

    coin = trade["symbol"].replace("USDT","")
    res  = "WIN" if pnl>0 else "LOSS"
    print(f"\n{'='*60}\n  {res} | {coin} {d} [{trade.get('trade_type','?')}] [{reason}]")
    print(f"  Entry: ${e:.4f} -> Exit: ${cp:.4f}  |  PnL: ${pnl:+.2f}  |  Bal: ${state['balance']:.2f}")
    print(f"{'='*60}\n")
    log.info(f"[CLOSE] {trade['symbol']} {reason} PnL=${pnl:+.4f}")
    save_state(state)

# ─── DISPLAY ───────────────────────────────────────────────────────────────────
def print_portfolio(state):
    b,i  = state["balance"], state["initial_balance"]
    p    = state["total_profit"]
    w,l  = state["win_count"], state["loss_count"]
    t    = w+l
    wr   = w/t*100 if t>0 else 0
    print(f"""
+-----------------------------------------------+
|  Balance: ${b:>10.2f}  |  Profit: ${p:>+9.2f}  |
|  Return:  {(b-i)/i*100:>+10.2f}%  |  WR: {wr:.0f}%  Trades: {t}  |
|  Open: {len(state['open_trades'])}/{MAX_OPEN_TRADES}                                  |
+-----------------------------------------------+""")

def print_context(fg, btc, ev):
    ev_str = ev['event_name'] if ev['has_event'] else "None"
    print(f"  [F&G: {fg['value']}/100 {fg['label']} — {fg['signal']}]")
    print(f"  [BTC Dom: {btc['btc_dominance']}% {btc['signal']}] [Event: {ev_str}]")
    print(f"  [Override: {fg['breakout_override_note']}]")

# ─── WEBSOCKET MAIN ────────────────────────────────────────────────────────────
FUTURES_WS   = "wss://fstream.binance.com/stream"
_executor    = ThreadPoolExecutor(max_workers=2)
_state_lock  = threading.Lock()   # protects state["open_trades"] / balance from concurrent close

def _run_full_scan(state: dict, latest_prices: dict):
    """Blocking scan — runs in thread pool so it never blocks the event loop."""
    now = datetime.now()
    print(f"\n[{now.strftime('%H:%M:%S')}] Candle closed — scanning...")

    fg  = get_fear_and_greed()
    btc = get_btc_dominance()
    ev  = check_high_impact_event()
    print_context(fg, btc, ev)

    if ev["should_skip"]:
        print(f"  SKIP: {ev['advice']}")
        state["last_scan"] = now.isoformat()
        save_state(state)
        return

    # Check open trades with fresh HTTP prices (candle close = good checkpoint)
    if state["open_trades"]:
        update_open_trades(state)

    print_portfolio(state)

    slots = MAX_OPEN_TRADES - len(state["open_trades"])
    if slots <= 0:
        print(f"  Max {MAX_OPEN_TRADES} trades open. Waiting for a slot...")
        state["last_scan"] = now.isoformat()
        save_state(state)
        return

    print(f"  Scanning {len(SCAN_SYMBOLS)} symbols...")
    analyses = []
    for sym in SCAN_SYMBOLS:
        d = build_payload(sym)
        if d:
            atr_ok  = d["atr_filter"]["sufficient_volatility"]
            rng     = d["range_setup"]["setup"] if d["range_setup"]["is_range"] else "-"
            bko     = d["breakout"]
            bko_str = f"BO:{bko['type']}" if bko.get("breakout_detected") else "BO:-"
            print(f"    {sym:<12} | {d['market_structure']:<8} | ATR:{'OK' if atr_ok else 'LOW'} | {rng} | {bko_str}")
            analyses.append(d)
        time.sleep(0.5)   # throttle between symbols to reduce CPU/network burst

    # ── BTC market regime ────────────────────────────────────────────
    btc_data = next((a for a in analyses if a["symbol"] == "BTCUSDT"), None)
    btc_ob   = btc_data["orderbook"] if btc_data else {"signal": "NEUTRAL", "confidence": 0}
    if btc_ob["signal"] == "BUY"  and btc_ob["confidence"] >= 65:
        market_regime = "BULL"
    elif btc_ob["signal"] == "SELL" and btc_ob["confidence"] >= 65:
        market_regime = "BEAR"
    else:
        market_regime = "NEUTRAL"
    log.info(f"[REGIME] BTC OB={btc_ob['signal']} conf={btc_ob['confidence']} -> {market_regime}")
    print(f"  [BTC Regime: {market_regime} | OB={btc_ob['signal']} conf={btc_ob['confidence']}%]")

    # ── Blocked symbols ──────────────────────────────────────────────
    blocked_symbols = {}
    open_syms = {t["symbol"] for t in state["open_trades"]}
    for sym in SCAN_SYMBOLS:
        if sym in open_syms:
            blocked_symbols[sym] = "already have open trade"
        for direction in ("LONG", "SHORT"):
            on_cd, mins_left = is_on_cooldown(state, sym, direction)
            if on_cd:
                blocked_symbols[f"{sym}_{direction}"] = f"SL cooldown {mins_left}min remaining"
        if market_regime == "BULL" and sym != "BTCUSDT":
            blocked_symbols[f"{sym}_SHORT"] = "BTC regime BULL — no alt shorts"
        elif market_regime == "BEAR" and sym != "BTCUSDT":
            blocked_symbols[f"{sym}_LONG"]  = "BTC regime BEAR — no alt longs"

    # Log all active blocks so we can see why symbols are being skipped
    if blocked_symbols:
        log.info(f"[BLOCKS] {len(blocked_symbols)} blocks active: {blocked_symbols}")

    # ── Pre-filter to top 5 candidates before AI (reduces prompt size ~75%) ──
    # S/R interaction is the primary signal — scored heavily.
    # Orderbook, BOS, breakout are bonus points.
    _SR_SCENARIO_SCORE = {
        "CONFIRMED_BREAK_RESISTANCE": 100,
        "CONFIRMED_BREAK_SUPPORT":    100,
        "FAKE_BREAK_RESISTANCE":       85,
        "FAKE_BREAK_SUPPORT":          85,
        "BOUNCE_FROM_RESISTANCE":      80,
        "BOUNCE_FROM_SUPPORT":         80,
        "APPROACHING_RESISTANCE":      15,
        "APPROACHING_SUPPORT":         15,
        # WAIT / MID_RANGE → 0 (not in map)
    }

    def _candidate_score(d):
        score = 0

        # S/R signals across all three timeframes (take the best)
        for sr_key in ("sr_interaction_5m", "sr_interaction_15m", "sr_interaction_1h"):
            sr = d.get(sr_key, {})
            scen = sr.get("scenario", "")
            s = _SR_SCENARIO_SCORE.get(scen, 0)
            # Add the S/R interaction's own confidence on top
            if sr.get("signal") in ("LONG", "SHORT"):
                s += sr.get("confidence", 0) * 0.4
            score = max(score, s)   # use best single timeframe

        # Breakout adds bonus — aligned with S/R break scenarios
        bko = d.get("breakout", {})
        if bko.get("breakout_detected"):
            score += 25
            if bko.get("volume_confirmed"):
                score += 15   # confirmed with volume = extra quality

        # BOS adds bonus (structure break confirms direction)
        if d.get("bos_15m", {}).get("bos_detected"):
            score += 20
        if d.get("bos_1h", {}).get("bos_detected"):
            score += 25

        # Orderbook alignment is a bonus confirmatory signal
        ob = d.get("orderbook", {})
        if ob.get("signal") != "NEUTRAL":
            score += ob.get("confidence", 0) * 0.2

        # ATR must be sufficient for any trade to work
        if d["atr_filter"]["sufficient_volatility"]:
            score += 10
        else:
            score -= 30   # low ATR = SL would be hit by noise

        # Hard-deprioritize symbols already in use or blocked
        sym = d["symbol"]
        if sym in open_syms or sym in blocked_symbols:
            score -= 200

        return score

    candidates = sorted(analyses, key=_candidate_score, reverse=True)
    print(f"  AI analyzing top {len(candidates)} candidates (of {len(analyses)} scanned):")
    for c in candidates:
        sr15c = c.get("sr_interaction_15m", {})
        sr5c  = c.get("sr_interaction_5m",  {})
        sr1hc = c.get("sr_interaction_1h",  {})
        # Show the strongest S/R signal across timeframes
        sr_parts = []
        for lbl, sr in [("5m", sr5c), ("15m", sr15c), ("1h", sr1hc)]:
            if sr.get("signal") in ("LONG", "SHORT"):
                sr_parts.append(f"{lbl}:{sr.get('scenario','?')}→{sr.get('signal')}(c={sr.get('confidence',0)})")
        sr_str = " | ".join(sr_parts) if sr_parts else "mid-range"
        print(f"    {c['symbol']:<12} | S/R: {sr_str}")
    decision = analyze_with_ai(
        candidates, state["balance"], len(state["open_trades"]),
        fg, btc, ev, market_regime, blocked_symbols
    )

    trades_attempted = 0
    trades_opened    = 0

    if decision.get("action") == "TRADE":
        sym  = decision.get("symbol", "?")
        dir_ = decision.get("direction", "?")

        if sym in open_syms:
            msg = f"  [GUARD] SKIP {sym}: already have open trade on this symbol"
            print(msg); log.info(msg)
            decision = {"action": "WAIT", "reason": msg}

        elif is_on_cooldown(state, sym, dir_)[0]:
            _, mins_left = is_on_cooldown(state, sym, dir_)
            msg = f"  [GUARD] SKIP {sym} {dir_}: SL cooldown {mins_left}min remaining"
            print(msg); log.info(msg)
            decision = {"action": "WAIT", "reason": msg}

        elif market_regime == "BULL" and dir_ == "SHORT" and sym != "BTCUSDT":
            msg = f"  [GUARD] SKIP {sym} SHORT: BTC regime BULL — no alt shorts"
            print(msg); log.info(msg)
            decision = {"action": "WAIT", "reason": msg}

        elif market_regime == "BEAR" and dir_ == "LONG" and sym != "BTCUSDT":
            msg = f"  [GUARD] SKIP {sym} LONG: BTC regime BEAR — no alt longs"
            print(msg); log.info(msg)
            decision = {"action": "WAIT", "reason": msg}

    if decision.get("action") == "TRADE":
        trades_attempted = 1
        wp   = decision.get("win_probability", 0)
        rr   = decision.get("risk_reward", 0)
        sym  = decision.get("symbol", "?")
        dir_ = decision.get("direction", "?")
        conf = decision.get("confidence", 0)

        log.info(
            f"[TRADE GATE] {sym} {dir_}: win_prob={wp}% (min={int(MIN_WIN_PROB*100)}%) "
            f"rr={rr} (min=1.5) confidence={conf}%"
        )
        print(f"  [TRADE GATE] {sym} {dir_}: wp={wp}% rr={rr} conf={conf}%")

        if wp < MIN_WIN_PROB * 100:
            msg = f"  [AI_REJECT] {sym} {dir_}: win_prob {wp}% < {int(MIN_WIN_PROB*100)}% minimum — skipping"
            print(msg); log.info(msg)
        elif rr < 1.5:
            msg = f"  [AI_REJECT] {sym} {dir_}: risk_reward {rr} < 1.5 minimum (wp={wp}%) — skipping"
            print(msg); log.info(msg)
        else:
            result = open_trade(state, decision)
            if result:
                trades_opened = 1
    else:
        reason = decision.get("reason", "No setup found")
        print(f"  WAIT: {reason}")
        log.info(f"[WAIT] {reason}")

    # ── Persist market context in state so dashboard always has fresh data ──
    state["market_context"] = {
        "fear_greed":     fg,
        "btc_dominance":  btc,
        "data_freshness": now.isoformat(),
    }
    state["last_scan"] = now.isoformat()
    save_state(state)

    log.info(
        f"[SCAN COMPLETE] {len(SCAN_SYMBOLS)} symbols analyzed, "
        f"{len(analyses)} had data, {len(candidates)} sent to AI, "
        f"{trades_attempted} trades attempted, {trades_opened} trades opened"
    )
    print(
        f"  [SCAN DONE] {len(analyses)}/{len(SCAN_SYMBOLS)} symbols | "
        f"{trades_attempted} attempted | {trades_opened} opened"
    )


async def _trade_monitor(state: dict, latest_prices: dict):
    """Continuously check TP/SL/timeout using streaming mark prices (every 3s)."""
    loop = asyncio.get_event_loop()
    while True:
        await asyncio.sleep(3)
        if state["open_trades"] and latest_prices:
            try:
                await loop.run_in_executor(
                    _executor,
                    lambda: update_open_trades_ws(state, dict(latest_prices))
                )
            except Exception as e:
                log.error(f"Trade monitor error: {e}")


async def _ws_main(state: dict):
    """WebSocket listener: 15m kline closes → AI scan, mark prices → TP/SL."""
    loop = asyncio.get_event_loop()
    latest_prices: dict = {}
    scan_lock = asyncio.Lock()

    # Build combined stream URL
    streams = []
    for sym in SCAN_SYMBOLS:
        s = sym.lower()
        streams.append(f"{s}@kline_15m")
        streams.append(f"{s}@markPrice@1s")
    url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"

    # Debounce: collect all kline-close events in one batch then fire one scan
    candle_event = asyncio.Event()

    async def scan_runner():
        while True:
            await candle_event.wait()
            candle_event.clear()
            # Wait 2s for remaining symbols' close events to arrive
            await asyncio.sleep(2)
            candle_event.clear()
            if scan_lock.locked():
                log.info("[WS] Previous scan still running — skipping this candle.")
                continue
            async with scan_lock:
                try:
                    available = [s for s in SCAN_SYMBOLS if s in latest_prices]
                    log.info(f"[WS] Scan starting — {len(available)}/{len(SCAN_SYMBOLS)} symbols have prices: {available}")
                    await loop.run_in_executor(
                        _executor,
                        lambda: _run_full_scan(state, dict(latest_prices))
                    )
                except Exception as e:
                    log.error(f"Scan error: {e}", exc_info=True)

    asyncio.create_task(scan_runner())
    asyncio.create_task(_trade_monitor(state, latest_prices))

    reconnect_delay = 5
    _dns_fail_count = 0   # track consecutive DNS failures to avoid log spam
    _mp_tick_count: dict = {}  # per-symbol markPrice tick counter for debug sampling
    _startup_scan_done = False  # only fire the immediate startup scan once

    while True:
        try:
            log.info(f"[WS] Connecting to {len(streams)} streams — URL: {url}")
            async with websockets.connect(
                url,
                # ── Ping strategy ───────────────────────────────────────────
                # ping_interval=None disables CLIENT-side pings entirely.
                # The websockets library still auto-responds to BINANCE's server
                # pings (sent every ~3 min) with a pong — that keeps the
                # connection alive without us triggering spurious timeouts.
                # Setting ping_interval=20 caused "sent 1011 keepalive ping
                # timeout" errors whenever Binance was slow to respond.
                ping_interval=None,
                ping_timeout=None,
                # ── Handshake timeout ────────────────────────────────────────
                # Default is 10s which fails on high-latency connections.
                # 45s gives room for slow DNS + TCP + HTTP upgrade.
                open_timeout=45,
                close_timeout=10,
                # ── Message size ─────────────────────────────────────────────
                # Default 1 MB; 2 MB prevents silent truncation of large depth msgs.
                max_size=2**21,
            ) as ws:
                log.info(f"[WS] Connected — {len(streams)} streams active ({len(SCAN_SYMBOLS)} symbols × kline_15m + markPrice@1s)")
                reconnect_delay = 5   # reset backoff on successful connect
                _dns_fail_count = 0
                # Warm up Ollama after reconnect so model is loaded and ready.
                loop.run_in_executor(_executor, _warmup_ollama)

                # Trigger one immediate scan shortly after first connect so the
                # user doesn't wait up to 15 minutes for the first candle boundary.
                if not _startup_scan_done:
                    _startup_scan_done = True
                    async def _trigger_startup_scan():
                        await asyncio.sleep(10)
                        log.info("[WS] Triggering startup scan (won't wait for next candle close)")
                        candle_event.set()
                    asyncio.create_task(_trigger_startup_scan())
                async for raw in ws:
                    msg     = json.loads(raw)
                    stream  = msg.get("stream", "")
                    payload = msg.get("data", {})
                    log.debug(f"[WS] msg: stream={stream} bytes={len(raw)}")

                    if "@kline_15m" in stream:
                        k = payload.get("k", {})
                        if k.get("x"):   # candle CLOSED
                            sym = k["s"]
                            log.info(f"[WS] 15m candle closed: {sym} close={k.get('c')} — scan signal set")
                            candle_event.set()

                    elif "@markPrice" in stream:
                        sym   = payload.get("s")
                        price = payload.get("p")
                        if sym and price:
                            latest_prices[sym] = float(price)
                            _mp_tick_count[sym] = _mp_tick_count.get(sym, 0) + 1
                            if _mp_tick_count[sym] % 60 == 0:
                                log.debug(f"[WS] markPrice sample tick #{_mp_tick_count[sym]}: {sym}={price}")

        except (websockets.exceptions.ConnectionClosed,
                websockets.exceptions.WebSocketException) as e:
            log.warning(f"[WS] Connection closed: {e} — reconnecting in {reconnect_delay}s")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)

        except OSError as e:
            err_str = str(e)
            if "getaddrinfo failed" in err_str or "Name or service not known" in err_str:
                _dns_fail_count += 1
                if _dns_fail_count == 1:
                    log.error(
                        f"[WS] DNS failure — cannot resolve fstream.binance.com. "
                        f"Possible causes: (1) no internet, (2) Binance geo-blocked in your region "
                        f"(use a VPN), (3) Windows DNS cache issue (run: ipconfig /flushdns). "
                        f"Retrying every {reconnect_delay}s silently..."
                    )
                    print(
                        f"\n  [WS] DNS ERROR — fstream.binance.com unreachable.\n"
                        f"  Causes: no internet | Binance geo-blocked | DNS issue\n"
                        f"  Fix   : connect to VPN, or run: ipconfig /flushdns\n"
                        f"  Bot will keep retrying automatically.\n"
                    )
                elif _dns_fail_count % 10 == 0:
                    log.warning(f"[WS] DNS still failing ({_dns_fail_count} attempts). Check VPN/internet.")
            elif "timed out" in err_str or "handshake" in err_str:
                log.warning(f"[WS] Handshake timed out — network latency too high or Binance unreachable. "
                            f"Retrying in {reconnect_delay}s")
            else:
                log.warning(f"[WS] Network error: {e} — reconnecting in {reconnect_delay}s")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)

        except asyncio.CancelledError:
            break


def main():
    if not acquire_lock():
        return

    import atexit
    atexit.register(release_lock)
    atexit.register(allow_sleep)

    # Keep the system awake so the bot runs even with lid closed / screen off
    prevent_sleep()

    if "localhost" in OLLAMA_BASE_URL or "127.0.0.1" in OLLAMA_BASE_URL:
        _provider = "LOCAL OLLAMA"
    elif "groq.com" in OLLAMA_BASE_URL:
        _provider = "GROQ CLOUD"
    elif "googleapis.com" in OLLAMA_BASE_URL:
        _provider = "GOOGLE GEMINI"
    else:
        _provider = "CLOUD AI"
    print("="*60)
    print("  AI TRADING BOT v4 — WEBSOCKET EDITION")
    print(f"  Min Win Prob: {int(MIN_WIN_PROB*100)}% | Min Leverage: {MIN_LEVERAGE}x | Min Size: ${MIN_POSITION_USDT:.0f} | Max Trades: {MAX_OPEN_TRADES}")
    print("  4H+1H+15m | OB+BOS+Liquidity+FVG+Structure+SR")
    print(f"  AI: {_provider} | Model: {OLLAMA_MODEL}")
    print(f"  Mode: {'SIMULATION' if SIMULATION_MODE else 'LIVE'} | Capital: ${INITIAL_CAPITAL}")
    print(f"  PID: {os.getpid()}")
    print("  Trigger: 15m candle close via WebSocket (no polling)")
    print("="*60)

    if not OLLAMA_BASE_URL:
        print("[ERROR] OLLAMA_BASE_URL missing in .env")
        return

    # ── Validate AI model is reachable before starting ───────────
    _is_local_ollama = "localhost" in OLLAMA_BASE_URL or "127.0.0.1" in OLLAMA_BASE_URL
    print(f"  Checking AI model '{OLLAMA_MODEL}' at {OLLAMA_BASE_URL}...")

    if AI_API_KEY == "your_groq_api_key_here" or not AI_API_KEY:
        print("\n[ERROR] AI_API_KEY is not set in .env!")
        print("  1. Go to https://console.groq.com → sign up (free)")
        print("  2. Create an API key")
        print("  3. Paste it in .env as: AI_API_KEY=gsk_xxxxxxxxxxxx")
        release_lock()
        return

    if _is_local_ollama:
        # Local Ollama — check installed models via /api/tags
        try:
            import requests as _req
            _r = _req.get(OLLAMA_BASE_URL.replace("/v1", "/api/tags"), timeout=5)
            _models = [m["name"] for m in _r.json().get("models", [])]
            _model_base = OLLAMA_MODEL.split(":")[0]
            _matched = any(OLLAMA_MODEL == m or m.startswith(_model_base + ":") for m in _models)
            if not _matched:
                print(f"\n[ERROR] Model '{OLLAMA_MODEL}' is NOT installed in Ollama!")
                print(f"  Available: {_models}")
                print(f"  Run: ollama pull {OLLAMA_MODEL}")
                release_lock()
                return
            print(f"  Model '{OLLAMA_MODEL}' found locally. Starting bot...")
        except Exception as _e:
            print(f"\n[WARNING] Could not verify Ollama model ({_e}). Proceeding anyway...")
    else:
        # Cloud API (Groq, OpenRouter, etc.) — just confirm the key looks set
        print(f"  Cloud AI: {OLLAMA_BASE_URL} | model={OLLAMA_MODEL}")
        print(f"  API key set: {'yes' if AI_API_KEY else 'NO — set AI_API_KEY in .env'}")
        print(f"  Starting bot...")

    state = load_state()

    try:
        asyncio.run(_ws_main(state))
    except KeyboardInterrupt:
        print("\nBot stopped.")
        print_portfolio(state)
        allow_sleep()
        release_lock()


if __name__ == "__main__":
    main()