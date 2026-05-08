"""
Trading Bot API Server — v2
Serves live state + market context to frontend
"""

from flask import Flask, jsonify, Response
from flask_cors import CORS
import json, os, time, requests

app = Flask(__name__)
CORS(app)

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE_DIR, "trading_state.json")

# Cache market context (refresh every 5 min)
_market_cache = {}
_market_cache_time = 0
CACHE_TTL = 300  # seconds

def load_state():
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return {
        "balance": 500.0, "initial_balance": 500.0,
        "open_trades": [], "closed_trades": [],
        "total_profit": 0.0, "win_count": 0, "loss_count": 0,
        "last_scan": None, "session_start": None
    }

def enrich_state(state):
    bal    = state.get("balance", 500)
    init   = state.get("initial_balance", 500)
    wins   = state.get("win_count", 0)
    losses = state.get("loss_count", 0)
    total  = wins + losses
    state["return_pct"] = round((bal - init) / init * 100, 2) if init > 0 else 0
    state["win_rate"]   = round(wins / total * 100, 1) if total > 0 else 0
    return state

def get_market_context():
    global _market_cache, _market_cache_time
    now = time.time()
    if now - _market_cache_time < CACHE_TTL and _market_cache:
        return _market_cache

    ctx = {
        "fear_greed": {"value": 50, "label": "Neutral", "signal": "NEUTRAL",
                       "avoid_longs": False, "avoid_shorts": False},
        "btc_dominance": {"value": 55.0, "signal": "BALANCED", "avoid_alts": False},
        "last_ai_decision": "Waiting...",
        "last_ai_reason": ""
    }

    # Fear & Greed
    try:
        r   = requests.get("https://api.alternative.me/fng/?limit=1", timeout=6)
        d   = r.json()["data"][0]
        val = int(d["value"])
        lbl = d["value_classification"]
        if val <= 25:   sig, al, as_ = "EXTREME_FEAR",  True,  False   # avoid longs in extreme fear
        elif val <= 45: sig, al, as_ = "FEAR",          False, False
        elif val <= 55: sig, al, as_ = "NEUTRAL",       False, False
        elif val <= 75: sig, al, as_ = "GREED",         False, False
        else:           sig, al, as_ = "EXTREME_GREED", False, True    # avoid shorts in extreme greed
        ctx["fear_greed"] = {"value": val, "label": lbl, "signal": sig,
                              "avoid_longs": al, "avoid_shorts": as_}
    except Exception:
        pass

    # BTC Dominance
    try:
        r   = requests.get("https://api.coingecko.com/api/v3/global", timeout=6)
        dom = r.json()["data"]["market_cap_percentage"]["btc"]
        if dom >= 58:   sig, aa = "BTC_DOMINANT", True
        elif dom >= 52: sig, aa = "BTC_STRONG",   False
        elif dom >= 47: sig, aa = "BALANCED",     False
        else:           sig, aa = "ALTSEASON",    False
        ctx["btc_dominance"] = {"value": round(dom, 2), "signal": sig, "avoid_alts": aa}
    except Exception:
        pass

    # Last AI decision from state file
    try:
        state = load_state()
        open_t  = state.get("open_trades", [])
        closed_t= state.get("closed_trades", [])
        if open_t:
            last = open_t[-1]
            ctx["last_ai_decision"] = f"TRADE — {last['symbol']} {last['direction']} [{last.get('trade_type','?')}]"
            ctx["last_ai_reason"]   = last.get("reasoning", "")
        elif closed_t:
            last = closed_t[-1]
            ctx["last_ai_decision"] = f"Last: {last['symbol']} {last['direction']} → {last.get('close_reason','?')}"
            ctx["last_ai_reason"]   = last.get("reasoning", "")
    except Exception:
        pass

    _market_cache      = ctx
    _market_cache_time = now
    return ctx

def _build_context(state_dict):
    """
    Always call get_market_context() so last_ai_decision is populated.
    Then override fear_greed / btc_dominance with the bot's freshly-fetched
    data when available (the bot fetches on every 15m candle close, which is
    more frequent than the api_server's 5-minute cache).
    """
    ctx = get_market_context()
    bot_ctx = state_dict.pop("market_context", None)
    if bot_ctx and bot_ctx.get("data_freshness"):
        if bot_ctx.get("fear_greed"):
            ctx["fear_greed"] = bot_ctx["fear_greed"]
        if bot_ctx.get("btc_dominance"):
            ctx["btc_dominance"] = bot_ctx["btc_dominance"]
        ctx["data_freshness"] = bot_ctx["data_freshness"]
    return ctx

@app.route("/api/state")
def get_state():
    state = enrich_state(load_state())
    state["market_context"] = _build_context(state)
    return jsonify(state)

@app.route("/api/trades/open")
def get_open_trades():
    return jsonify(load_state().get("open_trades", []))

@app.route("/api/trades/closed")
def get_closed_trades():
    return jsonify(load_state().get("closed_trades", [])[-50:])

@app.route("/api/stream")
def stream():
    def generate():
        while True:
            state = enrich_state(load_state())
            state["market_context"] = _build_context(state)
            yield f"data: {json.dumps(state)}\n\n"
            time.sleep(3)
    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

if __name__ == "__main__":
    print("API Server starting on http://localhost:5000")
    print("Keep this window open!")
    app.run(debug=False, host="0.0.0.0", port=5000, threaded=True)
