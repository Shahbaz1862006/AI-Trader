"""
Profile the AI completion call with fake market data.
Run: python profile_ai.py
"""
import time
import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

MODEL    = os.getenv("OLLAMA_MODEL", "llama3.2")
BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1")
API_KEY  = os.getenv("AI_API_KEY", "ollama")

SYSTEM = """You are a merged crypto trading AI combining two strategies:
  1. TA-based (S/R, BOS, Order Blocks, FVG, Orderbook)
  2. Contrarian sentiment (FinBERT on real news headlines)

FIXED PARAMETERS: Trade size=$20 | Leverage=30x | SL=3.333% | TP=5.0%
Your job: ONLY decide TRADE or WAIT.

OUTPUT — JSON only, no markdown:
{"action":"TRADE","symbol":"BTCUSDT","direction":"LONG","confidence":78,"win_probability":62,"reasoning":"..."}
If no setup: {"action":"WAIT","reason":"..."}"""

# Fake market data for 10 symbols (mirrors real payload size)
FAKE_MARKET = "\n".join(
    f"Symbol={s} | price=50000 | RSI_5m=50 | RSI_15m=50 | RSI_1h=50 | "
    f"OB=NEUTRAL | BOS=NONE | SR_INT=WAIT | vol_ratio=1.0 | sentiment=NEUTRAL"
    for s in ["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT",
              "LTCUSDT","DYDXUSDT","RUNEUSDT","LINKUSDT","SUIUSDT"]
)

MSG = (
    f"Fear&Greed=50/100 | BTC Dominance=50% | Economic event: none\n"
    f"Portfolio: balance=$500 | open=0/5\n\n"
    f"MARKET DATA (10 pairs):\n{FAKE_MARKET}\n\n"
    f"Pick the SINGLE best trade or return WAIT."
)

print(f"Provider : {BASE_URL}")
print(f"Model    : {MODEL}")
print(f"Prompt   : ~{len(SYSTEM)+len(MSG)} chars ({(len(SYSTEM)+len(MSG))//4} est. tokens)")
print("-" * 55)

client = OpenAI(base_url=BASE_URL, api_key=API_KEY, max_retries=0)

# --- Cold start (first call, model may need to load) ---
print("Cold start call ...")
t0 = time.perf_counter()
try:
    resp = client.chat.completions.create(
        model=MODEL, max_tokens=350, temperature=0.1,
        timeout=600,
        messages=[
            {"role": "system", "content": SYSTEM},
            {"role": "user",   "content": MSG},
        ]
    )
    elapsed = time.perf_counter() - t0
    raw = resp.choices[0].message.content.strip()
    print(f"  Time   : {elapsed:.1f}s")
    print(f"  Output : {raw[:200]}")
except Exception as e:
    elapsed = time.perf_counter() - t0
    print(f"  FAILED after {elapsed:.1f}s: {e}")

print()

# --- Warm call (model already in RAM) ---
print("Warm call ...")
t0 = time.perf_counter()
try:
    resp = client.chat.completions.create(
        model=MODEL, max_tokens=350, temperature=0.1,
        timeout=600,
        messages=[
            {"role": "system", "content": SYSTEM},
            {"role": "user",   "content": MSG},
        ]
    )
    elapsed = time.perf_counter() - t0
    raw = resp.choices[0].message.content.strip()
    print(f"  Time   : {elapsed:.1f}s")
    print(f"  Output : {raw[:200]}")
except Exception as e:
    elapsed = time.perf_counter() - t0
    print(f"  FAILED after {elapsed:.1f}s: {e}")

print()
print("=" * 55)
print("RECOMMENDATION:")
print("  If warm call > 120s → switch to Groq cloud (free, fast):")
print("    OLLAMA_MODEL=llama-3.3-70b-versatile")
print("    OLLAMA_BASE_URL=https://api.groq.com/openai/v1")
print("    AI_API_KEY=<your groq key from console.groq.com>")
print("  If warm call < 120s → bump timeout in trading_bot.py:1528")
print("    timeout=300  (instead of 180)")
