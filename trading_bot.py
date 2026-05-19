"""
Binance USDT-M Futures Intraday Trading Bot — Strategy v7
Multi-timeframe confluence: 4H EMA50 trend → 1H setup → 15M entry → 5M bonus
Partial TP: TP1=40% @ 1.0%, TP2=60% @ 2.0% with 0.5% trailing stop
"""

import os, sys, json, time, logging, asyncio, websockets, requests, threading, ctypes
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Optional

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try: sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    try: sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass

try:
    from transformers import AutoTokenizer, AutoModelForSequenceClassification
    import torch as _torch
    _FINBERT_AVAILABLE = True
except ImportError:
    _FINBERT_AVAILABLE = False

from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

# ─── CONFIG ────────────────────────────────────────────────────────────────────
BINANCE_API_KEY    = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
SIMULATION_MODE    = os.getenv("SIMULATION_MODE", "True") == "True"
INITIAL_CAPITAL    = float(os.getenv("INITIAL_CAPITAL", "500"))

MARGIN_PER_TRADE   = 20.0
LEVERAGE           = 30
POSITION_SIZE      = MARGIN_PER_TRADE * LEVERAGE   # $600 notional
MAX_OPEN_TRADES    = 3
ROUND_TRIP_FEE     = round(POSITION_SIZE * 0.001, 4)  # $0.60

TP1_PCT            = 0.010   # 1.0%
TP2_PCT            = 0.020   # 2.0%
TP1_CLOSE_RATIO    = 0.40
TP2_CLOSE_RATIO    = 0.60
BREAKEVEN_BUFFER   = 0.0     # move SL to exact entry after TP1
TRAILING_STOP_PCT  = 0.005   # 0.5% trailing after TP1
SL_MAX_PCT         = 0.010   # 1.0% hard max — skip if wider
SL_SWING_BUFFER    = 0.005   # 0.5% beyond swing (midpoint of 0.4-0.6%)

MIN_CONFLUENCE     = 55

SESSION_OPEN_H, SESSION_OPEN_M     = 8,  0
SESSION_CLOSE_H, SESSION_CLOSE_M   = 22, 30
FORCE_CLOSE_H, FORCE_CLOSE_M       = 23, 30

MAX_DAILY_LOSS              = 15.0
MAX_CONSEC_LOSSES           = 3
CONSEC_LOSS_PAUSE_MINS      = 30   # 30 min pause after 3 consecutive losses
PAIR_MAX_LOSSES_DAY         = 2    # skip pair 1hr after 2 losses
PAIR_SKIP_HRS               = 1    # hours to skip pair after streak
MAX_TRADES_PER_PAIR_DAY     = 6
MIN_GAP_BETWEEN_PAIR_MINS   = 10   # 10 min gap between trades on same pair
MAX_TRADES_PER_DAY          = 15
BTC_FLASH_PAUSE_MINS        = 20   # 20 min pause after BTC flash move
BTC_FLASH_MOVE_PCT          = 0.04  # 4% move triggers pause

FUNDING_LONG_MAX   = 0.0015  # +0.15%
FUNDING_SHORT_MIN  = -0.0010  # -0.10%
OB_MIN_DEPTH_USDT  = 1500.0   # top-10 combined depth >= $1500
OB_DEPTH_LEVELS    = 10       # use top 10 levels (was 20)
SR_NEAR_PCT        = 0.4      # within 0.4% of S/R counts as "near"
PAIR_24H_MIN       = 0.3      # skip pairs with < 0.3% change (dead)
PAIR_24H_MAX       = 9999.0   # no upper limit — high vol is fine with proper SL
PAIR_24H_PREF_LOW  = 1.0
PAIR_24H_PREF_HIGH = 8.0

SCAN_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "LTCUSDT", "DYDXUSDT", "RUNEUSDT", "LINKUSDT", "SUIUSDT"
]

COIN_NEWS_NAMES = {
    "BTCUSDT": "bitcoin",       "ETHUSDT": "ethereum",
    "SOLUSDT": "solana",        "BNBUSDT": "BNB binance coin",
    "XRPUSDT": "XRP ripple",    "LTCUSDT": "litecoin",
    "DYDXUSDT": "dydx",         "RUNEUSDT": "thorchain rune",
    "LINKUSDT": "chainlink",    "SUIUSDT": "sui crypto",
}

# (month, day, name, utc_hour, utc_minute) — only the 15-min window is blocked
HIGH_IMPACT_EVENTS = [
    (1,15,"US CPI",13,30),(2,12,"US CPI",13,30),(3,12,"US CPI",13,30),
    (4,10,"US CPI",13,30),(5,13,"US CPI",13,30),(6,11,"US CPI",13,30),
    (7,11,"US CPI",13,30),(8,13,"US CPI",13,30),(9,10,"US CPI",13,30),
    (10,15,"US CPI",13,30),(11,12,"US CPI",13,30),(12,10,"US CPI",13,30),
    (1,10,"NFP",13,30),(2,7,"NFP",13,30),(3,7,"NFP",13,30),
    (4,4,"NFP",13,30),(5,2,"NFP",13,30),(6,6,"NFP",13,30),
    (7,4,"NFP",13,30),(8,7,"NFP",13,30),(9,5,"NFP",13,30),
    (10,3,"NFP",13,30),(11,6,"NFP",13,30),(12,4,"NFP",13,30),
    (1,29,"FOMC",19,0),(3,19,"FOMC",19,0),(5,7,"FOMC",19,0),
    (6,18,"FOMC",19,0),(7,30,"FOMC",19,0),(9,17,"FOMC",19,0),
    (10,29,"FOMC",19,0),(12,10,"FOMC",19,0),
]

STATE_FILE = os.path.join(BASE_DIR, "trading_state.json")
LOCK_FILE  = os.path.join(BASE_DIR, "bot.lock")
LOG_FILE   = os.path.join(BASE_DIR, "logs", "bot.log")
os.makedirs(os.path.join(BASE_DIR, "logs"), exist_ok=True)

BINANCE_BASE = "https://fapi.binance.com"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─── SINGLE-INSTANCE LOCK ──────────────────────────────────────────────────────
def acquire_lock() -> bool:
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE, "r") as f:
                old_pid = int(f.read().strip())
            import subprocess
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {old_pid}", "/FO", "CSV"],
                capture_output=True, text=True
            )
            if str(old_pid) in result.stdout:
                print(f"[ERROR] Bot already running (PID {old_pid}). Exiting.")
                return False
            os.remove(LOCK_FILE)
            log.warning(f"Removed stale lock (PID {old_pid})")
        except Exception:
            try: os.remove(LOCK_FILE)
            except Exception: pass
    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))
    return True

def release_lock():
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
    except Exception:
        pass

# ─── SLEEP PREVENTION ──────────────────────────────────────────────────────────
_ES_CONTINUOUS        = 0x80000000
_ES_SYSTEM_REQUIRED   = 0x00000001
_ES_AWAYMODE_REQUIRED = 0x00000040

def prevent_sleep():
    if sys.platform == "win32":
        try:
            ctypes.windll.kernel32.SetThreadExecutionState(
                _ES_CONTINUOUS | _ES_SYSTEM_REQUIRED | _ES_AWAYMODE_REQUIRED
            )
            log.info("[SLEEP] Windows sleep prevention ENABLED")
            print("  [SLEEP] Windows sleep prevention ENABLED (lid-close safe)")
        except Exception as e:
            log.warning(f"[SLEEP] Could not enable: {e}")

def allow_sleep():
    if sys.platform == "win32":
        try:
            ctypes.windll.kernel32.SetThreadExecutionState(_ES_CONTINUOUS)
        except Exception:
            pass

# ─── STATE ─────────────────────────────────────────────────────────────────────
def _state_defaults():
    return {
        "balance": INITIAL_CAPITAL, "initial_balance": INITIAL_CAPITAL,
        "open_trades": [], "closed_trades": [],
        "total_profit": 0.0, "total_fees": 0.0,
        "win_count": 0, "loss_count": 0,
        "last_scan": None, "session_start": datetime.now().isoformat(),
        "sl_cooldowns": {},
        "daily_loss": 0.0, "daily_trades": 0, "daily_loss_date": None,
        "consecutive_losses": 0, "consecutive_loss_pause_until": None,
        "pair_daily_trades": {}, "pair_last_trade_time": {},
        "pair_loss_streak": {}, "pair_blacklist": {}, "pair_skip_until": {},
        "btc_flash_pause_until": None,
        "market_context": {},
    }

def load_state() -> dict:
    default = _state_defaults()
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)
            for k, v in default.items():
                saved.setdefault(k, v)
            return saved
    except Exception:
        pass
    return default

def save_state(state: dict):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, default=str)
    if os.path.exists(STATE_FILE):
        os.replace(tmp, STATE_FILE)
    else:
        os.rename(tmp, STATE_FILE)

# ─── BINANCE ───────────────────────────────────────────────────────────────────
_binance_fail_count  = 0
_binance_blocked_until: Optional[datetime] = None

def binance_get(endpoint: str, params: dict = {}) -> dict:
    global _binance_fail_count, _binance_blocked_until
    if _binance_blocked_until and datetime.now() < _binance_blocked_until:
        return {}
    try:
        r = requests.get(BINANCE_BASE + endpoint, params=params, timeout=5)
        r.raise_for_status()
        _binance_fail_count = 0
        _binance_blocked_until = None
        return r.json()
    except Exception as e:
        _binance_fail_count += 1
        if _binance_fail_count == 1 or _binance_fail_count % 10 == 0:
            log.error(f"Binance {endpoint}: {str(e)[:120]}")
        if _binance_fail_count >= 5:
            _binance_blocked_until = datetime.now() + timedelta(seconds=60)
            log.warning(f"[CIRCUIT BREAKER] Binance unreachable — pausing 60s")
        return {}

def get_klines(symbol: str, interval: str = "15m", limit: int = 100) -> list:
    data = binance_get("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit})
    if not data: return []
    return [{"open_time": k[0], "open": float(k[1]), "high": float(k[2]),
             "low": float(k[3]), "close": float(k[4]), "volume": float(k[5])} for k in data]

def get_ticker_24h(symbol: str) -> dict:
    return binance_get("/fapi/v1/ticker/24hr", {"symbol": symbol})

def get_funding_rate(symbol: str) -> dict:
    data = binance_get("/fapi/v1/fundingRate", {"symbol": symbol, "limit": 1})
    return data[0] if data else {}

def get_current_price(symbol: str) -> float:
    data = binance_get("/fapi/v1/ticker/price", {"symbol": symbol})
    return float(data.get("price", 0))

# ─── FEAR & GREED ─────────────────────────────────────────────────────────────
def get_fear_and_greed() -> dict:
    _default = {
        "value": 50, "label": "Neutral", "signal": "NEUTRAL",
        "soft_avoid_longs": False, "soft_avoid_shorts": False,
        "avoid_longs": False, "avoid_shorts": False,
        "breakout_override_note": "Neutral: both directions allowed"
    }
    url = f"https://api.alternative.me/fng/?limit=1&format=json&t={int(time.time())}"
    for attempt in range(2):
        try:
            r = requests.get(url, headers={"Cache-Control": "no-cache"}, timeout=10)
            r.raise_for_status()
            item = r.json()["data"][0]
            val = int(item["value"])
            lbl = item["value_classification"]
            if val <= 25:
                sig, note = "EXTREME_FEAR", "Extreme Fear: SHORT preferred. LONG only on confirmed breakout"
                sal, sas = True, False
            elif val <= 45:
                sig, note = "FEAR", "Fear: prefer shorts"
                sal, sas = False, False
            elif val <= 55:
                sig, note = "NEUTRAL", "Neutral: both directions"
                sal, sas = False, False
            elif val <= 75:
                sig, note = "GREED", "Greed: prefer longs"
                sal, sas = False, False
            else:
                sig, note = "EXTREME_GREED", "Extreme Greed: LONG preferred. SHORT only on confirmed breakdown"
                sal, sas = False, True
            return {"value": val, "label": lbl, "signal": sig,
                    "soft_avoid_longs": sal, "soft_avoid_shorts": sas,
                    "avoid_longs": False, "avoid_shorts": False,
                    "breakout_override_note": note}
        except Exception as e:
            log.warning(f"[F&G] attempt {attempt+1}/2: {e}")
            if attempt == 0: time.sleep(2)
    return _default

# ─── BTC DOMINANCE ────────────────────────────────────────────────────────────
def get_btc_dominance() -> dict:
    _default = {"value": 55.0, "btc_dominance": 55.0, "signal": "BALANCED", "avoid_alts": False}
    url = f"https://api.coingecko.com/api/v3/global?t={int(time.time())}"
    for attempt in range(2):
        try:
            r = requests.get(url, headers={"Cache-Control": "no-cache",
                                           "User-Agent": "TradingBot/1.0"}, timeout=10)
            r.raise_for_status()
            dom = r.json()["data"]["market_cap_percentage"]["btc"]
            if dom >= 58:   sig, avoid = "BTC_DOMINANT", True
            elif dom >= 52: sig, avoid = "BTC_STRONG",   False
            elif dom >= 47: sig, avoid = "BALANCED",     False
            else:           sig, avoid = "ALTSEASON",    False
            return {"value": round(dom, 2), "btc_dominance": round(dom, 2),
                    "signal": sig, "avoid_alts": avoid}
        except Exception as e:
            log.warning(f"[BTC_DOM] attempt {attempt+1}/2: {e}")
            if attempt == 0: time.sleep(2)
    return _default

# ─── ECONOMIC CALENDAR ────────────────────────────────────────────────────────
def check_high_impact_event() -> dict:
    """Blocks only within the ±15 min release window, not the entire day."""
    now = datetime.now(timezone.utc)
    for month, day, name, ev_h, ev_m in HIGH_IMPACT_EVENTS:
        if now.month == month and now.day == day:
            ev_time   = now.replace(hour=ev_h, minute=ev_m, second=0, microsecond=0)
            diff_secs = abs((now - ev_time).total_seconds())
            if diff_secs <= 15 * 60:
                return {"has_event": True, "event_name": name,
                        "advice": f"{name} release window (±15min) — skipping",
                        "should_skip": True}
    return {"has_event": False, "event_name": None,
            "advice": "No event window now", "should_skip": False}

# ─── FINBERT / NEWS SENTIMENT ─────────────────────────────────────────────────
_finbert_tokenizer = None
_finbert_model     = None
_finbert_device    = "cpu"

_POSITIVE_WORDS = {"bull","surge","rally","gain","high","record","growth","adopt",
                   "buy","rise","soar","pump","breakout","ath","partnership",
                   "launch","upgrade","approve","etf","institutional","bullish"}
_NEGATIVE_WORDS = {"bear","crash","drop","fall","down","loss","sell","fear","ban",
                   "hack","scam","fraud","warning","restrict","dump","liquidat",
                   "regulatory","fine","suspend","delist","probe","bearish"}

def _keyword_sentiment(headlines: list) -> tuple:
    pos = neg = 0
    for h in headlines:
        hl = h.lower()
        pos += sum(1 for w in _POSITIVE_WORDS if w in hl)
        neg += sum(1 for w in _NEGATIVE_WORDS if w in hl)
    total = pos + neg
    if total == 0: return "neutral", 0.5
    if pos > neg * 1.5: return "positive", round(min(0.5 + pos / (total * 2), 0.85), 3)
    if neg > pos * 1.5: return "negative", round(min(0.5 + neg / (total * 2), 0.85), 3)
    return "neutral", round(0.5 + abs(pos - neg) / max(total * 4, 1), 3)

def _load_finbert() -> bool:
    global _finbert_tokenizer, _finbert_model, _finbert_device
    if not _FINBERT_AVAILABLE: return False
    if _finbert_model is not None: return True
    try:
        _finbert_device = "cuda" if _torch.cuda.is_available() else "cpu"
        _finbert_tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
        _finbert_model = AutoModelForSequenceClassification.from_pretrained(
            "ProsusAI/finbert").to(_finbert_device)
        log.info(f"[FINBERT] Loaded on {_finbert_device}")
        return True
    except Exception as e:
        log.warning(f"[FINBERT] Load failed: {e}")
        return False

def fetch_crypto_news(symbol: str) -> list:
    import xml.etree.ElementTree as ET
    from urllib.parse import quote
    coin_name = COIN_NEWS_NAMES.get(symbol, symbol.replace("USDT", "").lower())
    ticker = symbol.replace("USDT", "").upper()
    ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    try:
        url = f"https://news.google.com/rss/search?q={quote(coin_name+' crypto price')}&hl=en-US&gl=US&ceid=US:en"
        r = requests.get(url, headers={"User-Agent": ua}, timeout=10)
        if r.status_code == 200:
            items = ET.fromstring(r.content).findall(".//item")
            hl = [i.findtext("title", "").strip() for i in items[:15] if i.findtext("title")]
            if hl: return hl
    except Exception: pass
    try:
        url = (f"https://www.reddit.com/r/CryptoCurrency/search.json"
               f"?q={quote(coin_name)}&sort=new&limit=15&t=day&restrict_sr=1")
        r = requests.get(url, headers={"User-Agent": "TradingBot/2.0"}, timeout=8)
        if r.status_code == 200:
            posts = r.json().get("data", {}).get("children", [])
            hl = [p["data"]["title"] for p in posts if p.get("data", {}).get("title")]
            if hl: return hl
    except Exception: pass
    return []

def get_coin_sentiment(symbol: str) -> dict:
    _neutral = {"sentiment": "neutral", "confidence": 0.5,
                "contrarian_signal": "NEUTRAL", "headline_count": 0, "source": "unavailable"}
    headlines = fetch_crypto_news(symbol)
    if not headlines: return _neutral

    def _contrarian(sentiment, confidence):
        if sentiment == "negative" and confidence >= 0.75: return "LONG"
        if sentiment == "positive" and confidence >= 0.80: return "SHORT"
        return "NEUTRAL"

    if _load_finbert():
        try:
            tokens = _finbert_tokenizer(headlines[:10], return_tensors="pt",
                                        padding=True, truncation=True, max_length=512
                                        ).to(_finbert_device)
            with _torch.no_grad():
                logits = _finbert_model(**tokens).logits
            probs = _torch.nn.functional.softmax(_torch.sum(logits, dim=0), dim=-1)
            labels = ["positive", "negative", "neutral"]
            idx = _torch.argmax(probs).item()
            sent, conf = labels[idx], round(probs[idx].item(), 3)
            return {"sentiment": sent, "confidence": conf, "contrarian_signal": _contrarian(sent, conf),
                    "headline_count": len(headlines), "source": "finbert"}
        except Exception as e:
            log.warning(f"[FINBERT] {symbol}: {e}")

    sent, conf = _keyword_sentiment(headlines)
    return {"sentiment": sent, "confidence": conf, "contrarian_signal": _contrarian(sent, conf),
            "headline_count": len(headlines), "source": "keyword"}

# ─── TECHNICAL ANALYSIS HELPERS ───────────────────────────────────────────────
def compute_ema(prices: list, period: int) -> list:
    if len(prices) < period: return []
    k = 2 / (period + 1)
    ema = [sum(prices[:period]) / period]
    for p in prices[period:]:
        ema.append(p * k + ema[-1] * (1 - k))
    return ema

def compute_rsi(closes: list, period: int = 14) -> float:
    if len(closes) < period + 1: return 50.0
    d = [closes[i+1] - closes[i] for i in range(len(closes) - 1)]
    ag = sum(x for x in d[-period:] if x > 0) / period
    al = sum(-x for x in d[-period:] if x < 0) / period
    if al == 0: return 100.0
    return round(100 - (100 / (1 + ag / al)), 2)

def compute_atr(candles: list, period: int = 14) -> float:
    if len(candles) < period + 1: return 0.0
    trs = [max(c["high"] - c["low"],
               abs(c["high"] - candles[i-1]["close"]),
               abs(c["low"]  - candles[i-1]["close"]))
           for i, c in enumerate(candles) if i > 0]
    return sum(trs[-period:]) / period

def compute_bollinger_bands(closes: list, period: int = 20, num_std: float = 2.0) -> tuple:
    if len(closes) < period: return 0.0, 0.0, 0.0
    recent = closes[-period:]
    mid = sum(recent) / period
    std = (sum((x - mid) ** 2 for x in recent) / period) ** 0.5
    return round(mid + num_std * std, 6), round(mid, 6), round(mid - num_std * std, 6)

def compute_macd_hist_series(closes: list) -> list:
    """Returns full MACD histogram series aligned with closes."""
    e12 = compute_ema(closes, 12)
    e26 = compute_ema(closes, 26)
    if not e12 or not e26: return []
    n = min(len(e12), len(e26))
    ml = [e12[-(n - i)] - e26[-(n - i)] for i in range(n)]
    sig = compute_ema(ml, 9)
    if not sig: return ml
    ns, nm = len(sig), len(ml)
    offset = nm - ns
    return [ml[offset + i] - sig[i] for i in range(ns)]

def compute_vwap_intraday(candles: list) -> float:
    now_utc = datetime.now(timezone.utc)
    day_start_ms = int(datetime(now_utc.year, now_utc.month, now_utc.day,
                                tzinfo=timezone.utc).timestamp() * 1000)
    day_c = [c for c in candles if c.get("open_time", 0) >= day_start_ms]
    if not day_c:
        return candles[-1]["close"] if candles else 0.0
    total_tpv = sum((c["high"] + c["low"] + c["close"]) / 3 * c["volume"] for c in day_c)
    total_vol = sum(c["volume"] for c in day_c)
    return round(total_tpv / total_vol, 6) if total_vol > 0 else day_c[-1]["close"]

# ─── 4H TREND FILTER ──────────────────────────────────────────────────────────
def get_4h_trend(symbol: str) -> str:
    """
    Returns 'LONG', 'SHORT', or 'SKIP' (data only).
    Direction: price > EMA50 → LONG, price < EMA50 → SHORT.
    No skip zone — EMA200 is context only, not a hard block.
    """
    candles = get_klines(symbol, "4h", 60)
    if not candles or len(candles) < 55: return "SKIP"
    closes = [c["close"] for c in candles]
    ema50  = compute_ema(closes, 50)
    if not ema50: return "SKIP"
    price = closes[-1]
    return "LONG" if price > ema50[-1] else "SHORT"

# ─── 1H SETUP ZONE ────────────────────────────────────────────────────────────
def check_1h_setup_zone(candles_1h: list, direction: str) -> dict:
    """
    Checks:
    - RSI 14 between 35-70 (widened)
    - Volume >= 1.1x 20-period SMA (lowered)
    - Price above EMA21 for LONG, below for SHORT
    BB proximity block removed.
    """
    if not candles_1h or len(candles_1h) < 25:
        return {"passes": False, "rsi": 50.0, "rsi_ok": False, "vol_ratio": 0.0,
                "vol_ok": False, "ema21_ok": False}
    closes  = [c["close"] for c in candles_1h]
    volumes = [c["volume"] for c in candles_1h]
    price   = closes[-1]
    rsi     = compute_rsi(closes, 14)
    rsi_ok  = 35.0 <= rsi <= 70.0
    vol_sma   = sum(volumes[-20:]) / 20
    vol_ratio = volumes[-1] / vol_sma if vol_sma > 0 else 1.0
    vol_ok    = vol_ratio >= 1.1
    ema21 = compute_ema(closes, 21)
    if ema21:
        ema21_ok = (price > ema21[-1]) if direction == "LONG" else (price < ema21[-1])
    else:
        ema21_ok = False
    passes = rsi_ok and vol_ok and ema21_ok
    return {
        "passes":    passes,
        "rsi":       rsi,
        "rsi_ok":    rsi_ok,
        "vol_ratio": round(vol_ratio, 2),
        "vol_ok":    vol_ok,
        "ema21_ok":  ema21_ok,
    }

# ─── 15M ENTRY SIGNAL ─────────────────────────────────────────────────────────
def check_15m_entry_signal(candles_15m: list, direction: str) -> dict:
    """
    Checks:
    - MACD (12,26,9) histogram flips in trade direction, OR
    - EMA9 crosses EMA21 within last 5 candles
    - RSI 14: 40-70 for LONG, 30-60 for SHORT
    Either MACD flip OR EMA cross is sufficient (not both required).
    """
    if not candles_15m or len(candles_15m) < 35:
        return {"passes": False, "macd_flip": False, "ema_cross": False, "rsi": 50.0}
    closes = [c["close"] for c in candles_15m]
    rsi    = compute_rsi(closes, 14)
    if direction == "LONG":
        rsi_ok = 40.0 <= rsi <= 70.0
    else:
        rsi_ok = 30.0 <= rsi <= 60.0
    hists = compute_macd_hist_series(closes)
    macd_flip = False
    if len(hists) >= 2:
        h_prev, h_now = hists[-2], hists[-1]
        if direction == "LONG":
            macd_flip = h_prev < 0 and h_now > 0
        else:
            macd_flip = h_prev > 0 and h_now < 0
    e9  = compute_ema(closes, 9)
    e21 = compute_ema(closes, 21)
    ema_cross = False
    if e9 and e21 and len(e9) >= 6 and len(e21) >= 6:
        min_len = min(len(e9), len(e21))
        e9s  = e9[-min_len:]
        e21s = e21[-min_len:]
        for i in range(-6, -1):   # last 5 candles
            if abs(i) > min_len: continue
            was_above = e9s[i] > e21s[i]
            is_above  = e9s[i + 1] > e21s[i + 1]
            if direction == "LONG" and not was_above and is_above:
                ema_cross = True; break
            if direction == "SHORT" and was_above and not is_above:
                ema_cross = True; break
    passes = (macd_flip or ema_cross) and rsi_ok
    return {
        "passes":    passes,
        "macd_flip": macd_flip,
        "ema_cross": ema_cross,
        "rsi":       rsi,
        "rsi_ok":    rsi_ok,
    }

# ─── 5M ENTRY TIMING ──────────────────────────────────────────────────────────
def check_5m_entry_timing(candles_5m: list, direction: str) -> dict:
    """
    Optional bonus — checks last 2 closed 5M candles for a confirming pattern.
    LONG:  bullish engulfing or hammer
    SHORT: bearish engulfing or shooting star
    Not a gate: no pattern still allows entry, just scores lower.
    """
    if not candles_5m or len(candles_5m) < 3:
        return {"passes": False, "pattern": None}
    for i in [-1, -2]:
        try:
            c    = candles_5m[i]
            prev = candles_5m[i - 1]
        except IndexError:
            continue
        body       = abs(c["close"] - c["open"])
        c_range    = c["high"] - c["low"]
        if c_range == 0: continue
        body_ratio = body / c_range
        lower_wick = min(c["open"], c["close"]) - c["low"]
        upper_wick = c["high"] - max(c["open"], c["close"])
        if direction == "LONG":
            if (c["close"] > c["open"] and prev["close"] < prev["open"] and
                    c["open"] <= prev["close"] and c["close"] >= prev["open"]):
                return {"passes": True, "pattern": "BULLISH_ENGULFING"}
            if (body > 0 and lower_wick >= body * 2 and
                    upper_wick <= body * 0.5 and body_ratio < 0.4):
                return {"passes": True, "pattern": "HAMMER"}
        else:
            if (c["close"] < c["open"] and prev["close"] > prev["open"] and
                    c["open"] >= prev["close"] and c["close"] <= prev["open"]):
                return {"passes": True, "pattern": "BEARISH_ENGULFING"}
            if (body > 0 and upper_wick >= body * 2 and
                    lower_wick <= body * 0.5 and body_ratio < 0.4):
                return {"passes": True, "pattern": "SHOOTING_STAR"}
    return {"passes": False, "pattern": None}

# ─── SUPPORT & RESISTANCE ─────────────────────────────────────────────────────
def get_sr_levels(symbol: str, price: float, vwap: float) -> list:
    """Prev day H/L, current day open, VWAP, round levels every 0.5%."""
    levels = []
    daily = get_klines(symbol, "1d", 3)
    if daily and len(daily) >= 2:
        prev = daily[-2]
        levels.extend([prev["high"], prev["low"], daily[-1]["open"]])
    if vwap > 0:
        levels.append(vwap)
    step = price * 0.005
    for i in range(-5, 6):
        levels.append(price + i * step)
    return [l for l in levels if l > 0]

def is_near_sr(price: float, levels: list) -> bool:
    """Within SR_NEAR_PCT (0.4%) of any S/R level."""
    for lvl in levels:
        if lvl > 0 and abs(price - lvl) / price * 100 <= SR_NEAR_PCT:
            return True
    return False

# ─── SWING SL CALCULATION ─────────────────────────────────────────────────────
def calc_swing_sl(candles_15m: list, direction: str, entry_price: float) -> dict:
    """
    SL = SL_SWING_BUFFER (0.5%) beyond the last 15M swing point.
    Hard max SL_MAX_PCT (1.0%) from entry — skip if wider.
    """
    if not candles_15m or len(candles_15m) < 5:
        return {"ok": False, "reason": "insufficient candles", "sl_price": 0.0, "sl_pct": 0.0}
    recent = candles_15m[-min(10, len(candles_15m)):]
    if direction == "LONG":
        swing_pt = min(c["low"] for c in recent[-5:])
        sl_price = round(swing_pt * (1 - SL_SWING_BUFFER), 6)
        if sl_price >= entry_price:
            sl_price = round(entry_price * (1 - SL_MAX_PCT * 0.5), 6)
    else:
        swing_pt = max(c["high"] for c in recent[-5:])
        sl_price = round(swing_pt * (1 + SL_SWING_BUFFER), 6)
        if sl_price <= entry_price:
            sl_price = round(entry_price * (1 + SL_MAX_PCT * 0.5), 6)
    sl_pct = abs(entry_price - sl_price) / entry_price * 100
    if sl_pct > SL_MAX_PCT * 100:
        return {"ok": False,
                "reason": f"SL too wide: {sl_pct:.2f}% > {SL_MAX_PCT*100:.1f}%",
                "sl_price": round(sl_price, 6), "sl_pct": round(sl_pct, 3)}
    return {"ok": True, "sl_price": sl_price, "sl_pct": round(sl_pct, 3),
            "swing_point": round(swing_pt, 6)}

# ─── ORDER BOOK DEPTH ─────────────────────────────────────────────────────────
def check_ob_depth(symbol: str) -> bool:
    """Top OB_DEPTH_LEVELS (10) combined bid+ask depth >= OB_MIN_DEPTH_USDT ($1500)."""
    ob = binance_get("/fapi/v1/depth", {"symbol": symbol, "limit": OB_DEPTH_LEVELS})
    bids = ob.get("bids", [])
    asks = ob.get("asks", [])
    if not bids or not asks: return False
    depth = (sum(float(p) * float(q) for p, q in bids[:OB_DEPTH_LEVELS]) +
             sum(float(p) * float(q) for p, q in asks[:OB_DEPTH_LEVELS]))
    return depth >= OB_MIN_DEPTH_USDT

# ─── ATR FILTER ───────────────────────────────────────────────────────────────
def check_atr_filter(candles_15m: list) -> dict:
    """ATR filter removed in v7 — always passes."""
    return {"ok": True, "ratio": 1.0, "reason": "ATR filter removed"}

# ─── BTC FLASH MOVE ───────────────────────────────────────────────────────────
def check_and_update_btc_flash(state: dict, candles_15m_btc: list) -> bool:
    """Returns True if all trading is paused due to BTC 3% flash move."""
    pause_str = state.get("btc_flash_pause_until")
    if pause_str:
        try:
            if datetime.now() < datetime.fromisoformat(pause_str):
                return True
            else:
                state["btc_flash_pause_until"] = None
        except Exception:
            state["btc_flash_pause_until"] = None
    if candles_15m_btc and len(candles_15m_btc) >= 2:
        last_c = candles_15m_btc[-1]["close"]
        prev_c = candles_15m_btc[-2]["close"]
        if prev_c > 0 and abs(last_c - prev_c) / prev_c >= BTC_FLASH_MOVE_PCT:
            pause = (datetime.now() + timedelta(minutes=BTC_FLASH_PAUSE_MINS)).isoformat()
            state["btc_flash_pause_until"] = pause
            log.warning(f"[BTC FLASH] {abs(last_c-prev_c)/prev_c*100:.1f}% in 15m — pausing {BTC_FLASH_PAUSE_MINS}min")
            return True
    return False

# ─── CONFLUENCE SCORE ─────────────────────────────────────────────────────────
def compute_confluence_score(
        trend_aligned: bool,
        rsi_1h_ok: bool,
        vol_1h_ok: bool,
        signal_15m: bool,
        candle_pattern: bool,
        near_sr: bool) -> int:
    """
    4H EMA trend aligned       : 25
    1H RSI in range            : 15
    1H volume spike            : 15
    15M MACD flip OR EMA cross : 20
    5M candle pattern (bonus)  : 10
    Price near S/R             :  5
    Max: 90 | Min to trade: 55
    """
    score = 0
    if trend_aligned:  score += 25
    if rsi_1h_ok:      score += 15
    if vol_1h_ok:      score += 15
    if signal_15m:     score += 20
    if candle_pattern: score += 10
    if near_sr:        score += 5
    return score

# ─── PAIR RANKING ─────────────────────────────────────────────────────────────
def rank_pairs() -> list:
    """Rank by 24H% change; prefer 1.5%-6%; skip dead (<0.5%) or wild (>8%)."""
    ranked = []
    for sym in SCAN_SYMBOLS:
        try:
            t   = get_ticker_24h(sym)
            chg = abs(float(t.get("priceChangePercent", 0)))
            if PAIR_24H_MIN <= chg <= PAIR_24H_MAX:
                ranked.append((sym, chg))
        except Exception:
            pass
    def sort_key(item):
        _, chg = item
        preferred = PAIR_24H_PREF_LOW <= chg <= PAIR_24H_PREF_HIGH
        return (0 if preferred else 1, -chg)
    ranked.sort(key=sort_key)
    return [s for s, _ in ranked]

# ─── RISK RULES ───────────────────────────────────────────────────────────────
def _maybe_reset_daily(state: dict):
    today = datetime.now(timezone.utc).date().isoformat()
    if state.get("daily_loss_date") != today:
        state["daily_loss_date"]       = today
        state["daily_loss"]            = 0.0
        state["daily_trades"]          = 0
        state["pair_daily_trades"]     = {}
        state["pair_last_trade_time"]  = {}
        state["pair_loss_streak"]      = {}
        state["pair_blacklist"]        = {}
        log.info(f"[DAILY RESET] New UTC day: {today}")

def check_risk_rules(state: dict, symbol: str) -> tuple:
    """Returns (allowed: bool, reason: str)."""
    _maybe_reset_daily(state)
    if state.get("daily_loss", 0.0) >= MAX_DAILY_LOSS:
        return False, f"Daily loss ${state['daily_loss']:.2f} >= ${MAX_DAILY_LOSS} limit"
    if state.get("daily_trades", 0) >= MAX_TRADES_PER_DAY:
        return False, f"Max {MAX_TRADES_PER_DAY} trades/day reached"
    pause_str = state.get("consecutive_loss_pause_until")
    if pause_str:
        try:
            pause_dt = datetime.fromisoformat(pause_str)
            if datetime.now() < pause_dt:
                mins = int((pause_dt - datetime.now()).total_seconds() / 60)
                return False, f"3-loss pause: {mins}min remaining"
            state["consecutive_loss_pause_until"] = None
        except Exception:
            state["consecutive_loss_pause_until"] = None
    skip_until_str = state.get("pair_skip_until", {}).get(symbol)
    if skip_until_str:
        try:
            skip_dt = datetime.fromisoformat(skip_until_str)
            if datetime.now() < skip_dt:
                mins = int((skip_dt - datetime.now()).total_seconds() / 60)
                return False, f"{symbol} skipped {mins}min (2 losses — 1hr cooldown)"
            else:
                state.setdefault("pair_skip_until", {}).pop(symbol, None)
        except Exception:
            state.setdefault("pair_skip_until", {}).pop(symbol, None)
    pair_trades = state.get("pair_daily_trades", {}).get(symbol, 0)
    if pair_trades >= MAX_TRADES_PER_PAIR_DAY:
        return False, f"{symbol} max {MAX_TRADES_PER_PAIR_DAY} trades/day"
    last_str = state.get("pair_last_trade_time", {}).get(symbol)
    if last_str:
        try:
            gap = (datetime.now() - datetime.fromisoformat(last_str)).total_seconds() / 60
            if gap < MIN_GAP_BETWEEN_PAIR_MINS:
                return False, f"{symbol} {MIN_GAP_BETWEEN_PAIR_MINS}min gap ({gap:.0f}min elapsed)"
        except Exception:
            pass
    return True, "ok"

def _update_pair_streak(state: dict, symbol: str, won: bool):
    streaks = state.setdefault("pair_loss_streak", {})
    if won:
        streaks[symbol] = 0
    else:
        streaks[symbol] = streaks.get(symbol, 0) + 1
        if streaks[symbol] >= PAIR_MAX_LOSSES_DAY:
            skip_until = (datetime.now() + timedelta(hours=PAIR_SKIP_HRS)).isoformat()
            state.setdefault("pair_skip_until", {})[symbol] = skip_until
            log.warning(f"[PAIR SKIP] {symbol} skipped {PAIR_SKIP_HRS}hr after {PAIR_MAX_LOSSES_DAY} losses")

# ─── SESSION WINDOW ───────────────────────────────────────────────────────────
def is_in_session_window() -> bool:
    now = datetime.now(timezone.utc)
    open_t  = now.replace(hour=SESSION_OPEN_H,  minute=SESSION_OPEN_M,  second=0, microsecond=0)
    close_t = now.replace(hour=SESSION_CLOSE_H, minute=SESSION_CLOSE_M, second=0, microsecond=0)
    return open_t <= now <= close_t

def should_force_close() -> bool:
    now = datetime.now(timezone.utc)
    return now.hour == FORCE_CLOSE_H and now.minute >= FORCE_CLOSE_M

# ─── TRADE MANAGEMENT ─────────────────────────────────────────────────────────
_state_lock = threading.Lock()

def open_trade(state: dict, symbol: str, direction: str,
               sl_price: float, confluence_score: int,
               reasoning: str, sentiment: dict = None) -> dict:
    entry = get_current_price(symbol)
    if entry <= 0:
        log.error(f"[OPEN] Cannot fetch price for {symbol}")
        return {}
    if state["balance"] < MARGIN_PER_TRADE:
        log.error(f"[OPEN] Insufficient balance: ${state['balance']:.2f}")
        return {}
    if direction == "LONG":
        tp1 = round(entry * (1 + TP1_PCT), 6)
        tp2 = round(entry * (1 + TP2_PCT), 6)
    else:
        tp1 = round(entry * (1 - TP1_PCT), 6)
        tp2 = round(entry * (1 - TP2_PCT), 6)
    sl_pct = round(abs(entry - sl_price) / entry * 100, 3)
    trade = {
        "id":               f"SIM_{int(time.time())}",
        "symbol":           symbol,
        "direction":        direction,
        "leverage":         LEVERAGE,
        "entry_price":      entry,
        "stop_loss":        sl_price,
        "take_profit_1":    tp1,
        "take_profit_2":    tp2,
        "sl_pct":           sl_pct,
        "tp1_pct":          round(TP1_PCT * 100, 2),
        "tp2_pct":          round(TP2_PCT * 100, 2),
        "position_size":    MARGIN_PER_TRADE,
        "remaining_pct":    1.0,
        "tp1_done":         False,
        "tp1_price":        None,
        "tp1_pnl":          0.0,
        "trailing_stop_active": False,
        "trailing_stop_peak":   None,
        "trailing_stop_price":  None,
        "confluence_score": confluence_score,
        "reasoning":        reasoning,
        "sentiment":        sentiment or {},
        "open_time":        datetime.now().isoformat(),
        "status":           "OPEN",
        "current_pnl":      0.0,
        "current_pnl_pct":  0.0,
        "current_price":    entry,
    }
    with _state_lock:
        state["open_trades"].append(trade)
    _maybe_reset_daily(state)
    state["daily_trades"] = state.get("daily_trades", 0) + 1
    state.setdefault("pair_daily_trades", {})[symbol] = \
        state["pair_daily_trades"].get(symbol, 0) + 1
    state.setdefault("pair_last_trade_time", {})[symbol] = datetime.now().isoformat()
    coin  = symbol.replace("USDT", "")
    arrow = "LONG " if direction == "LONG" else "SHORT"
    cs    = sentiment.get("contrarian_signal", "NEUTRAL") if sentiment else "NEUTRAL"
    print(f"""
+------------------------------------------------------------------+
|  {coin:<6} FUTURES  {arrow}  [score={confluence_score}/100]  [OPEN]       |
+------------------------------------------------------------------+
|  Entry: ${entry:<12.4f}  SL: ${sl_price:<12.4f} (-{sl_pct:.2f}%)        |
|  TP1  : ${tp1:<12.4f}  (+{TP1_PCT*100:.1f}% → close 40%)             |
|  TP2  : ${tp2:<12.4f}  (+{TP2_PCT*100:.1f}% → close 60%)             |
|  Margin: ${MARGIN_PER_TRADE}  Notional: ${POSITION_SIZE}  Fee: ${ROUND_TRIP_FEE}         |
|  Sentiment: {cs:<8}  |  {reasoning[:52]:<52} |
+------------------------------------------------------------------+""")
    log.info(f"[OPEN] {symbol} {direction} @ {entry:.4f} | SL={sl_price:.4f} ({sl_pct:.2f}%) "
             f"TP1={tp1:.4f} TP2={tp2:.4f} | score={confluence_score}")
    save_state(state)
    return trade

def _process_trade_price(state: dict, trade: dict, cp: float):
    """Core price update logic shared by WS and HTTP update paths."""
    e   = trade["entry_price"]
    sl  = trade["stop_loss"]
    tp1 = trade["take_profit_1"]
    tp2 = trade["take_profit_2"]
    d   = trade["direction"]
    sz  = trade["position_size"]
    lv  = trade["leverage"]
    rem = trade.get("remaining_pct", 1.0)

    # Maintain trailing stop peak
    if trade.get("trailing_stop_active"):
        peak = trade.get("trailing_stop_peak") or cp
        if d == "LONG":
            if cp > peak: peak = cp
            trail = round(peak * (1 - TRAILING_STOP_PCT), 6)
        else:
            if cp < peak: peak = cp
            trail = round(peak * (1 + TRAILING_STOP_PCT), 6)
        trade["trailing_stop_peak"]  = peak
        trade["trailing_stop_price"] = trail
    else:
        trail = None

    tp1_hit = (not trade.get("tp1_done")) and (
        (d == "LONG" and cp >= tp1) or (d == "SHORT" and cp <= tp1))
    tp2_hit = trade.get("tp1_done") and (
        (d == "LONG" and cp >= tp2) or (d == "SHORT" and cp <= tp2))
    trail_hit = (trail is not None and trade.get("trailing_stop_active") and (
        (d == "LONG" and cp <= trail) or (d == "SHORT" and cp >= trail)))
    sl_hit = (d == "LONG" and cp <= sl) or (d == "SHORT" and cp >= sl)

    if tp1_hit:
        partial_gross = sz * TP1_CLOSE_RATIO * abs(cp - e) / e * lv
        partial_fee   = ROUND_TRIP_FEE * TP1_CLOSE_RATIO
        partial_pnl   = round(partial_gross - partial_fee, 4)
        trade["tp1_done"]     = True
        trade["tp1_price"]    = cp
        trade["tp1_pnl"]      = partial_pnl
        trade["remaining_pct"] = TP2_CLOSE_RATIO
        # Move SL to breakeven + 0.1%
        if d == "LONG":
            trade["stop_loss"] = round(e * (1 + BREAKEVEN_BUFFER), 6)
            trade["trailing_stop_price"] = round(cp * (1 - TRAILING_STOP_PCT), 6)
        else:
            trade["stop_loss"] = round(e * (1 - BREAKEVEN_BUFFER), 6)
            trade["trailing_stop_price"] = round(cp * (1 + TRAILING_STOP_PCT), 6)
        trade["trailing_stop_active"] = True
        trade["trailing_stop_peak"]   = cp
        state["balance"]      = round(state["balance"] + partial_pnl, 4)
        state["total_profit"] = round(state["total_profit"] + partial_pnl, 4)
        state["total_fees"]   = round(state.get("total_fees", 0.0) + partial_fee, 4)
        coin = trade["symbol"].replace("USDT", "")
        print(f"  [TP1] {coin} {d}: 40% closed @ {cp:.4f} | +${partial_pnl:.2f} | "
              f"SL→breakeven {trade['stop_loss']:.4f} | trailing 0.5% active")
        log.info(f"[TP1] {trade['symbol']} {d} partial close 40% @ {cp:.4f} PnL=${partial_pnl:+.4f}")
    elif tp2_hit:
        _close_trade(state, trade, cp, "TP2")
    elif trail_hit:
        _close_trade(state, trade, cp, "TRAIL_STOP")
    elif sl_hit:
        _close_trade(state, trade, cp, "SL")
    else:
        # Update unrealized PnL on remaining position
        rem_sz = sz * rem
        if d == "LONG":
            unreal = rem_sz * (cp - e) / e * lv
        else:
            unreal = rem_sz * (e - cp) / e * lv
        unreal = max(-rem_sz, unreal)
        total  = round(trade.get("tp1_pnl", 0.0) + unreal, 4)
        trade["current_pnl"]     = total
        trade["current_pnl_pct"] = round(total / sz * 100, 2)
        trade["current_price"]   = cp

def update_open_trades_ws(state: dict, latest_prices: dict):
    with _state_lock:
        for trade in state["open_trades"][:]:
            cp = latest_prices.get(trade["symbol"], 0.0)
            if cp == 0: continue
            if trade["id"] not in {t["id"] for t in state["open_trades"]}: continue
            _process_trade_price(state, trade, cp)
    save_state(state)

def update_open_trades_http(state: dict):
    with _state_lock:
        for trade in state["open_trades"][:]:
            cp = get_current_price(trade["symbol"])
            if cp == 0: continue
            if trade["id"] not in {t["id"] for t in state["open_trades"]}: continue
            _process_trade_price(state, trade, cp)
    save_state(state)

def _close_trade(state: dict, trade: dict, cp: float, reason: str):
    e   = trade["entry_price"]
    sz  = trade["position_size"]
    lv  = trade["leverage"]
    d   = trade["direction"]
    rem = trade.get("remaining_pct", 1.0)
    tp1_done = trade.get("tp1_done", False)

    rem_sz = sz * rem
    if d == "LONG":
        rem_pnl = rem_sz * (cp - e) / e * lv
    else:
        rem_pnl = rem_sz * (e - cp) / e * lv
    rem_pnl = max(-rem_sz, rem_pnl)

    # Fee: full if no TP1, partial on remaining if TP1 done
    fee = round(ROUND_TRIP_FEE * (rem if tp1_done else 1.0), 4)
    rem_pnl = round(rem_pnl - fee, 4)

    tp1_pnl   = trade.get("tp1_pnl", 0.0)
    total_pnl = round(tp1_pnl + rem_pnl, 4)

    state["total_fees"]   = round(state.get("total_fees", 0.0) + fee, 4)
    state["balance"]      = round(state["balance"] + rem_pnl, 4)
    state["total_profit"] = round(state["total_profit"] + rem_pnl, 4)

    trade.update({
        "close_price":  cp,
        "close_time":   datetime.now().isoformat(),
        "realized_pnl": total_pnl,
        "close_reason": reason,
        "status":       "CLOSED",
    })

    won = total_pnl > 0
    if won:
        state["win_count"]          = state.get("win_count", 0) + 1
        state["consecutive_losses"] = 0
    else:
        state["loss_count"]   = state.get("loss_count", 0) + 1
        state["consecutive_losses"] = state.get("consecutive_losses", 0) + 1
        loss_amt = abs(total_pnl)
        state["daily_loss"] = round(state.get("daily_loss", 0.0) + loss_amt, 4)
        if state["consecutive_losses"] >= MAX_CONSEC_LOSSES:
            pause = (datetime.now() + timedelta(minutes=CONSEC_LOSS_PAUSE_MINS)).isoformat()
            state["consecutive_loss_pause_until"] = pause
            log.warning(f"[RISK] {MAX_CONSEC_LOSSES} consecutive losses — pausing {CONSEC_LOSS_PAUSE_MINS}min")
            print(f"  [RISK] 3 consecutive losses — trading paused {CONSEC_LOSS_PAUSE_MINS}min")

    _update_pair_streak(state, trade["symbol"], won)

    if reason == "SL":
        key = f"{trade['symbol']}_{d}"
        state.setdefault("sl_cooldowns", {})[key] = \
            (datetime.now() + timedelta(minutes=45)).isoformat()

    state["open_trades"]   = [t for t in state["open_trades"] if t["id"] != trade["id"]]
    state["closed_trades"].append(trade)
    if len(state["closed_trades"]) > 200:
        state["closed_trades"] = state["closed_trades"][-200:]

    coin = trade["symbol"].replace("USDT", "")
    res  = "WIN " if won else "LOSS"
    tp1_str = f" (TP1+{tp1_pnl:+.2f})" if tp1_done else ""
    print(f"\n{'='*60}")
    print(f"  {res} | {coin} {d} [{reason}]{tp1_str}")
    print(f"  Entry: ${e:.4f} → Exit: ${cp:.4f}  |  Total PnL: ${total_pnl:+.2f}  |  Bal: ${state['balance']:.2f}")
    print(f"{'='*60}\n")
    log.info(f"[CLOSE] {trade['symbol']} {reason} total_pnl=${total_pnl:+.4f} bal=${state['balance']:.2f}")
    save_state(state)

def force_close_all(state: dict, latest_prices: dict = None):
    for trade in state["open_trades"][:]:
        if latest_prices:
            cp = latest_prices.get(trade["symbol"], 0.0)
        else:
            cp = get_current_price(trade["symbol"])
        if cp > 0:
            _close_trade(state, trade, cp, "FORCE_CLOSE_23:30")

# ─── DISPLAY ───────────────────────────────────────────────────────────────────
def print_portfolio(state: dict):
    b, i  = state["balance"], state["initial_balance"]
    p     = state["total_profit"]
    fees  = state.get("total_fees", 0.0)
    w, l  = state.get("win_count", 0), state.get("loss_count", 0)
    t     = w + l
    wr    = w / t * 100 if t > 0 else 0
    dl    = state.get("daily_loss", 0.0)
    dt    = state.get("daily_trades", 0)
    print(f"""
+-----------------------------------------------------------+
|  Balance : ${b:>10.2f}  |  Net P&L : ${p:>+9.2f}           |
|  Return  : {(b-i)/i*100:>+10.2f}%  |  Fees paid: ${fees:>7.2f}          |
|  Win Rate: {wr:>5.1f}%  ({w}W/{l}L/{t} trades)                   |
|  Open    : {len(state['open_trades'])}/{MAX_OPEN_TRADES}  |  Daily: {dt} trades, ${dl:.2f} loss      |
+-----------------------------------------------------------+""")
    if state["open_trades"]:
        print("  Open positions:")
        for tr in state["open_trades"]:
            coin = tr["symbol"].replace("USDT", "")
            tp1_str = " [TP1✓ trailing]" if tr.get("tp1_done") else ""
            print(f"    {coin} {tr['direction']} @ {tr['entry_price']:.4f} "
                  f"| PnL ${tr['current_pnl']:+.2f} "
                  f"| SL {tr['stop_loss']:.4f}{tp1_str}")

# ─── MAIN SCAN ─────────────────────────────────────────────────────────────────
def _run_full_scan(state: dict, latest_prices: dict):
    now_utc   = datetime.now(timezone.utc)
    now_local = datetime.now()
    print(f"\n[{now_local.strftime('%H:%M:%S')}] Scanning | UTC={now_utc.strftime('%H:%M')}")

    _maybe_reset_daily(state)

    # Force close at 23:30 UTC
    if should_force_close():
        if state["open_trades"]:
            print("  [FORCE CLOSE] 23:30 UTC — closing all positions")
            force_close_all(state, latest_prices if latest_prices else None)
        state["last_scan"] = now_local.isoformat()
        save_state(state)
        return

    # Update open trades first
    if state["open_trades"]:
        if latest_prices:
            update_open_trades_ws(state, latest_prices)
        else:
            update_open_trades_http(state)

    print_portfolio(state)

    # Session window
    if not is_in_session_window():
        print(f"  [OUTSIDE SESSION] {now_utc.strftime('%H:%M')} UTC — session is 08:00-22:30 UTC")
        state["last_scan"] = now_local.isoformat()
        save_state(state)
        return

    # Daily loss limit
    if state.get("daily_loss", 0.0) >= MAX_DAILY_LOSS:
        print(f"  [DAILY LIMIT] ${state['daily_loss']:.2f} loss today — stopped for day")
        state["last_scan"] = now_local.isoformat()
        save_state(state)
        return

    # Slots
    slots = MAX_OPEN_TRADES - len(state["open_trades"])
    if slots <= 0:
        print(f"  [FULL] {MAX_OPEN_TRADES} trades open — waiting for slot")
        state["last_scan"] = now_local.isoformat()
        save_state(state)
        return

    # Economic event
    ev = check_high_impact_event()
    if ev["should_skip"]:
        print(f"  [EVENT] {ev['advice']}")
        state["last_scan"] = now_local.isoformat()
        save_state(state)
        return

    # BTC flash move
    btc_c15 = get_klines("BTCUSDT", "15m", 5)
    if check_and_update_btc_flash(state, btc_c15):
        try:
            pause_dt = datetime.fromisoformat(state.get("btc_flash_pause_until", ""))
            mins = max(0, int((pause_dt - datetime.now()).total_seconds() / 60))
        except Exception:
            mins = BTC_FLASH_PAUSE_MINS
        print(f"  [BTC FLASH] Paused {mins}min after BTC ≥4% move")
        state["last_scan"] = now_local.isoformat()
        save_state(state)
        return

    # Consecutive loss pause
    pause_str = state.get("consecutive_loss_pause_until")
    if pause_str:
        try:
            pause_dt = datetime.fromisoformat(pause_str)
            if datetime.now() < pause_dt:
                mins = int((pause_dt - datetime.now()).total_seconds() / 60)
                print(f"  [PAUSE] 3-loss pause: {mins}min remaining")
                state["last_scan"] = now_local.isoformat()
                save_state(state)
                return
        except Exception:
            state["consecutive_loss_pause_until"] = None

    # Fetch sentiment in parallel
    print(f"  [SENTIMENT] Fetching news for {len(SCAN_SYMBOLS)} pairs...")
    with ThreadPoolExecutor(max_workers=5) as ex:
        sent_futures = {sym: ex.submit(get_coin_sentiment, sym) for sym in SCAN_SYMBOLS}
        sentiments   = {sym: sent_futures[sym].result() for sym in SCAN_SYMBOLS}
    for sym in SCAN_SYMBOLS:
        s = sentiments[sym]
        print(f"    {sym:<12} sentiment={s['contrarian_signal']:<7} {s['headline_count']} headlines ({s['source']})")

    # Rank pairs
    ranked = rank_pairs()
    if not ranked:
        ranked = SCAN_SYMBOLS
        print(f"  [RANK] All pairs in 24H range (fallback)")
    else:
        skipped = [s.replace("USDT","") for s in SCAN_SYMBOLS if s not in ranked]
        print(f"  [RANK] Active: {[s.replace('USDT','') for s in ranked]}")
        if skipped:
            print(f"  [RANK] Skipped (dead/wild): {skipped}")

    open_syms = {t["symbol"] for t in state["open_trades"]}
    candidates = []

    print(f"  [SCAN] Checking {len(ranked)} pairs for setups...")
    for symbol in ranked:
        if symbol in open_syms:
            continue

        # Risk rules
        allowed, reason = check_risk_rules(state, symbol)
        if not allowed:
            print(f"    {symbol:<12} SKIP risk: {reason}")
            continue

        # 4H trend filter
        trend = get_4h_trend(symbol)
        if trend == "SKIP":
            print(f"    {symbol:<12} SKIP: 4H data insufficient")
            continue
        direction = trend

        # Funding rate filter
        fr_data = get_funding_rate(symbol)
        fr      = float(fr_data.get("fundingRate", 0)) if fr_data else 0.0
        if direction == "LONG" and fr > FUNDING_LONG_MAX:
            print(f"    {symbol:<12} SKIP: funding {fr:.4%} > {FUNDING_LONG_MAX:.4%} (LONG)")
            continue
        if direction == "SHORT" and fr < FUNDING_SHORT_MIN:
            print(f"    {symbol:<12} SKIP: funding {fr:.4%} < {FUNDING_SHORT_MIN:.4%} (SHORT)")
            continue

        # Order book depth
        if not check_ob_depth(symbol):
            print(f"    {symbol:<12} SKIP: OB depth < ${OB_MIN_DEPTH_USDT:.0f}")
            continue

        # Fetch multi-TF candles once
        c4h = get_klines(symbol, "4h",  10)   # just for context
        c1h = get_klines(symbol, "1h",  50)
        c15 = get_klines(symbol, "15m", 60)
        c5m = get_klines(symbol, "5m",  15)
        if not c15 or len(c15) < 35:
            print(f"    {symbol:<12} SKIP: insufficient 15M data")
            continue

        # 1H setup zone
        setup_1h = check_1h_setup_zone(c1h, direction)

        # 15M entry signal
        entry_15m = check_15m_entry_signal(c15, direction)

        # 5M timing
        timing_5m = check_5m_entry_timing(c5m, direction)

        # S/R near check
        price = c15[-1]["close"]
        c15_day = get_klines(symbol, "15m", 96)
        vwap    = compute_vwap_intraday(c15_day)
        sr_lvls = get_sr_levels(symbol, price, vwap)
        near_sr = is_near_sr(price, sr_lvls)

        # Confluence score
        score = compute_confluence_score(
            trend_aligned  = True,
            rsi_1h_ok      = setup_1h.get("rsi_ok",   False),
            vol_1h_ok      = setup_1h.get("vol_ok",   False),
            signal_15m     = entry_15m.get("passes",  False),
            candle_pattern = timing_5m.get("passes",  False),
            near_sr        = near_sr,
        )

        print(f"    {symbol:<12} {direction:<5} score={score:>3}/100 | "
              f"1H rsi={setup_1h.get('rsi',0):.0f}({'✓' if setup_1h.get('rsi_ok') else '✗'}) "
              f"vol={setup_1h.get('vol_ratio',0):.2f}x({'✓' if setup_1h.get('vol_ok') else '✗'}) | "
              f"macd={'✓' if entry_15m.get('macd_flip') else '✗'} "
              f"ema_x={'✓' if entry_15m.get('ema_cross') else '✗'} "
              f"rsi={entry_15m.get('rsi',0):.0f} | "
              f"5m={timing_5m.get('pattern','-')!s:<20} sr={'✓' if near_sr else '✗'}")

        if score < MIN_CONFLUENCE:
            print(f"    {symbol:<12} SKIP: score {score} < {MIN_CONFLUENCE} minimum")
            time.sleep(0.3)
            continue

        # Swing SL calculation
        sl_calc = calc_swing_sl(c15, direction, price)
        if not sl_calc["ok"]:
            print(f"    {symbol:<12} SKIP: {sl_calc.get('reason', 'SL too wide')}")
            time.sleep(0.3)
            continue

        reasoning = (
            f"Score={score}/100 | 4H={direction} | "
            f"1H RSI={setup_1h.get('rsi',0):.1f}{'✓' if setup_1h.get('rsi_ok') else '✗'} "
            f"vol={setup_1h.get('vol_ratio',0):.2f}x | "
            f"MACD_flip={'✓' if entry_15m.get('macd_flip') else '✗'} "
            f"EMA_cross={'✓' if entry_15m.get('ema_cross') else '✗'} | "
            f"5M={timing_5m.get('pattern','-')} | "
            f"near_SR={near_sr} | SL={sl_calc['sl_pct']:.2f}%"
        )
        candidates.append({
            "symbol":    symbol,
            "direction": direction,
            "score":     score,
            "sl_price":  sl_calc["sl_price"],
            "sl_pct":    sl_calc["sl_pct"],
            "price":     price,
            "sentiment": sentiments.get(symbol, {}),
            "reasoning": reasoning,
        })
        time.sleep(0.3)

    # Persist market context
    fg  = get_fear_and_greed()
    btc = get_btc_dominance()
    state["market_context"] = {
        "fear_greed":    fg,
        "btc_dominance": btc,
        "data_freshness": now_local.isoformat(),
    }

    if not candidates:
        print(f"  WAIT: no setup scored >= {MIN_CONFLUENCE} this candle")
        state["last_scan"] = now_local.isoformat()
        save_state(state)
        return

    candidates.sort(key=lambda x: x["score"], reverse=True)
    best = candidates[0]
    print(f"\n  [BEST SETUP] {best['symbol']} {best['direction']} "
          f"score={best['score']} SL={best['sl_pct']:.2f}%")
    if len(candidates) > 1:
        print(f"  [OTHER SETUPS] {[(c['symbol'], c['score']) for c in candidates[1:]]}")

    open_trade(
        state           = state,
        symbol          = best["symbol"],
        direction       = best["direction"],
        sl_price        = best["sl_price"],
        confluence_score= best["score"],
        reasoning       = best["reasoning"],
        sentiment       = best["sentiment"],
    )

    state["last_scan"] = now_local.isoformat()
    save_state(state)
    log.info(f"[SCAN DONE] {len(ranked)} ranked | {len(candidates)} qualified | best={best['symbol']}")

# ─── WEBSOCKET MAIN ────────────────────────────────────────────────────────────
_executor = ThreadPoolExecutor(max_workers=2)

async def _trade_monitor(state: dict, latest_prices: dict):
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
    loop          = asyncio.get_event_loop()
    latest_prices: dict = {}
    scan_lock     = asyncio.Lock()
    candle_event  = asyncio.Event()

    streams = []
    for sym in SCAN_SYMBOLS:
        s = sym.lower()
        streams.append(f"{s}@kline_15m")
        streams.append(f"{s}@markPrice@1s")
    url = f"wss://fstream.binance.com/market/stream?streams={'/'.join(streams)}"

    async def scan_runner():
        while True:
            await candle_event.wait()
            candle_event.clear()
            await asyncio.sleep(2)
            candle_event.clear()
            if scan_lock.locked():
                log.info("[WS] Previous scan still running — skipping")
                continue
            async with scan_lock:
                try:
                    await loop.run_in_executor(
                        _executor,
                        lambda: _run_full_scan(state, dict(latest_prices))
                    )
                except Exception as e:
                    log.error(f"Scan error: {e}", exc_info=True)

    asyncio.create_task(scan_runner())
    asyncio.create_task(_trade_monitor(state, latest_prices))

    reconnect_delay    = 5
    _dns_fail_count    = 0
    _startup_scan_done = False

    while True:
        try:
            log.info(f"[WS] Connecting — {len(streams)} streams")
            async with websockets.connect(
                url,
                ping_interval=None,
                ping_timeout=None,
                open_timeout=45,
                close_timeout=10,
                max_size=2**21,
            ) as ws:
                log.info(f"[WS] Connected — {len(SCAN_SYMBOLS)} symbols × kline_15m + markPrice")
                reconnect_delay = 5
                _dns_fail_count = 0
                if not _startup_scan_done:
                    _startup_scan_done = True
                    async def _startup():
                        await asyncio.sleep(10)
                        log.info("[WS] Startup scan triggered")
                        candle_event.set()
                    asyncio.create_task(_startup())
                async for raw in ws:
                    msg     = json.loads(raw)
                    stream  = msg.get("stream", "")
                    payload = msg.get("data", {})
                    if "@kline_15m" in stream:
                        k = payload.get("k", {})
                        if k.get("x"):
                            log.info(f"[WS] 15m candle closed: {k['s']}")
                            candle_event.set()
                    elif "@markPrice" in stream:
                        sym   = payload.get("s")
                        price = payload.get("p")
                        if sym and price:
                            latest_prices[sym] = float(price)

        except (websockets.exceptions.ConnectionClosed,
                websockets.exceptions.WebSocketException) as e:
            log.warning(f"[WS] Closed: {e} — reconnect in {reconnect_delay}s")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)

        except OSError as e:
            err_str = str(e)
            if "getaddrinfo failed" in err_str or "Name or service not known" in err_str:
                _dns_fail_count += 1
                if _dns_fail_count == 1:
                    log.error("[WS] DNS failure — check internet / VPN / ipconfig /flushdns")
                    print("\n  [WS] DNS ERROR — fstream.binance.com unreachable\n"
                          "  Fix: connect VPN or run: ipconfig /flushdns\n")
                elif _dns_fail_count % 10 == 0:
                    log.warning(f"[WS] DNS still failing ({_dns_fail_count} attempts)")
            else:
                log.warning(f"[WS] Network error: {e} — reconnect in {reconnect_delay}s")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)

        except asyncio.CancelledError:
            break

# ─── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    if not acquire_lock():
        return
    import atexit
    atexit.register(release_lock)
    atexit.register(allow_sleep)
    prevent_sleep()

    print("=" * 65)
    print("  INTRADAY CONFLUENCE BOT v7")
    print("-" * 65)
    print(f"  Strategy  : 4H EMA50 trend → 1H setup → 15M entry (OR logic) → 5M bonus")
    print(f"  Confluence: min {MIN_CONFLUENCE}/90 to trade")
    print(f"  Score     : 4H(25)+1H_RSI(15)+1H_vol(15)+15M_sig(20)+5M(10)+SR(5)")
    print(f"  TP/SL     : TP1={TP1_PCT*100:.1f}%(40%) TP2={TP2_PCT*100:.1f}%(60%) "
          f"SL=swing±0.5% max {SL_MAX_PCT*100:.1f}%")
    print(f"  Trailing  : {TRAILING_STOP_PCT*100:.1f}% after TP1 | Breakeven at entry after TP1")
    print(f"  Session   : {SESSION_OPEN_H:02d}:{SESSION_OPEN_M:02d}–{SESSION_CLOSE_H:02d}:{SESSION_CLOSE_M:02d} UTC | Force-close {FORCE_CLOSE_H:02d}:{FORCE_CLOSE_M:02d} UTC")
    print(f"  Risk      : Daily loss ${MAX_DAILY_LOSS} | {MAX_CONSEC_LOSSES} losses → {CONSEC_LOSS_PAUSE_MINS}min pause | BTC flash → {BTC_FLASH_PAUSE_MINS}min pause")
    print(f"  Margin    : ${MARGIN_PER_TRADE}/trade | {LEVERAGE}x | Max {MAX_OPEN_TRADES} open | Capital ${INITIAL_CAPITAL}")
    print(f"  Mode      : {'SIMULATION' if SIMULATION_MODE else 'LIVE'}")
    print(f"  Pairs     : {', '.join(s.replace('USDT','') for s in SCAN_SYMBOLS)}")
    print(f"  PID       : {os.getpid()}")
    print("=" * 65)

    state = load_state()
    try:
        asyncio.run(_ws_main(state))
    except KeyboardInterrupt:
        print("\nBot stopped.")
        print_portfolio(state)
        allow_sleep()
        release_lock()
    except RuntimeError as e:
        if "Cannot close a running event loop" in str(e):
            pass
        else:
            log.error(f"Runtime error: {e}", exc_info=True)
            allow_sleep()
            release_lock()

if __name__ == "__main__":
    main()
