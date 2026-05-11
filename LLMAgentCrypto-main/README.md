# Crypto LLM Agent Trading Bot
How to build a trading bot for crypto with LLMs (not so great) and finbert (way better) 

## See it live and in action 📺
<a href="https://youtu.be/cYqNBY7i0hI"><img src="https://i.imgur.com/xxHhw9O.jpeg"/></a>

# Startup 🚀
1. Create a virtual environment `uv venv` and activate it `source .venv/bin/activate`
2. Update your `SERPER_API_KEY` in the `.env` file -> Available from <a href="https://serper.dev/api-key ">here</a> 
3. Install dependencies `uv pip install -r pyproject.toml`
4. Run whichever bot you want `uv run 1. cryptobot_aggregate_sentiment-r1.py`

# Who, When, Why?

👨🏾‍💻 Author: Nick Renotte <br />
📅 Version: 1.x<br />
📜 License: This project is licensed under the MIT License </br>

NOTE: if you get a error that looks something like this, it's probs because the CCXT datastore isn't picking up the strategy returns, 
You can edit the ccxt datastore file to fix it. I gotta find the exact details on how to do this, but i'll update it in the next few days.
`lumibot/tools/ccxt_data_store.py`