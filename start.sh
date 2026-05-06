#!/bin/bash
# ┌─────────────────────────────────────────┐
# │   AI CRYPTO TRADING BOT - STARTUP       │
# └─────────────────────────────────────────┘

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║     AI TRADING SYSTEM - SIMULATION MODE      ║"
echo "╚══════════════════════════════════════════════╝"
echo ""

# ── SET YOUR KEYS HERE ──────────────────────────────
export ANTHROPIC_API_KEY="your_anthropic_key_here"
export BINANCE_API_KEY="your_binance_api_key_here"
export BINANCE_API_SECRET="your_binance_api_secret_here"
# ────────────────────────────────────────────────────

if [ -z "$ANTHROPIC_API_KEY" ] || [ "$ANTHROPIC_API_KEY" = "your_anthropic_key_here" ]; then
    echo "❌ ERROR: Please set your API keys in start.sh first!"
    echo ""
    echo "   Edit start.sh and replace:"
    echo "   - your_anthropic_key_here  → your Anthropic key"
    echo "   - your_binance_api_key_here → your Binance API key"
    echo "   - your_binance_api_secret_here → your Binance secret"
    echo ""
    exit 1
fi

echo "✅ API keys loaded"
echo "📦 Installing dependencies..."
pip install -r requirements.txt -q

echo "🌐 Starting API server on port 5000..."
cd backend
python api_server.py &
API_PID=$!

sleep 2

echo "🤖 Starting trading bot..."
python trading_bot.py &
BOT_PID=$!

echo ""
echo "┌─────────────────────────────────────────────┐"
echo "│  SYSTEM RUNNING                             │"
echo "│  API Server: http://localhost:5000          │"
echo "│  Dashboard:  Open frontend/index.html       │"
echo "│                                             │"
echo "│  Press Ctrl+C to stop all processes         │"
echo "└─────────────────────────────────────────────┘"
echo ""

# Cleanup on exit
cleanup() {
    echo ""
    echo "🛑 Stopping all processes..."
    kill $API_PID $BOT_PID 2>/dev/null
    echo "✅ Stopped."
    exit 0
}

trap cleanup SIGINT SIGTERM

wait
