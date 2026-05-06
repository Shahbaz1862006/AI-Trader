# 🤖 AI Crypto Trading Bot — Setup Guide

## System Overview

```
trading_system/
├── backend/
│   ├── trading_bot.py    ← Main AI trading engine
│   └── api_server.py     ← Flask API for frontend
├── frontend/
│   └── index.html        ← Live dashboard
├── logs/
│   └── bot.log           ← Trade logs
├── requirements.txt
├── start.sh              ← Main startup script
└── trading_state.json    ← Auto-created (live state)
```

---

## ⚙️ Requirements

- Python 3.10+
- Binance Futures API keys (read + trade permissions)
- Anthropic API key

---

## 🚀 Step-by-Step Setup

### Step 1: Get Your API Keys

**Binance:**
1. binance.com → Account → API Management
2. Create new API key
3. Enable: "Enable Futures", "Enable Reading"
4. Copy API Key + Secret Key

**Anthropic:**
1. console.anthropic.com
2. API Keys → Create Key
3. Copy key

---

### Step 2: Configure Keys

Open `start.sh` and replace:
```bash
export ANTHROPIC_API_KEY="sk-ant-xxxx..."
export BINANCE_API_KEY="your_binance_key"
export BINANCE_API_SECRET="your_binance_secret"
```

---

### Step 3: Install & Run

```bash
# Make start script executable
chmod +x start.sh

# Start the system
./start.sh
```

---

### Step 4: Open Dashboard

Open `frontend/index.html` in your browser.
The dashboard auto-connects to the bot at localhost:5000.

---

## 🎮 Simulation Mode (Week 1)

In `backend/trading_bot.py`:
```python
SIMULATION_MODE = True   # Change to False for live trading
```

During simulation:
- ✅ Real Binance market data
- ✅ Real AI analysis (Claude)
- ✅ Real technical analysis
- ✅ Simulated trade execution (no real money)
- ✅ Full P&L tracking

---

## 📊 What The Bot Does

Every 5 minutes:
1. Scans 20 crypto pairs on Binance Futures
2. Collects: RSI, MACD, EMA, Support/Resistance, FVG, Order Flow
3. Sends all data to Claude AI
4. AI picks best trade with 75%+ win probability
5. Opens trade (simulated), monitors price
6. Closes at Take Profit or Stop Loss

**Rules:**
- Max 2 trades open at once
- Max leverage: 10x
- Risk per trade: 2% of balance
- Min win probability: 75%
- Trades complete within hours (not days)

---

## 🛡️ Risk Management

- Stop Loss: Always set
- Take Profit: Single TP (100% position close)
- Max drawdown: 2% per trade
- R:R Ratio: Minimum 1:2

---

## ⚠️ Disclaimer

This is educational/simulation software.
Crypto trading involves significant risk.
Test thoroughly in simulation before going live.
Never trade more than you can afford to lose.
