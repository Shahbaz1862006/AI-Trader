from lumibot.entities import Asset
from lumibot.backtesting import CcxtBacktesting
from lumibot.strategies.strategy import Strategy
from datetime import datetime
from colorama import Fore
import random

# New Imports
from timedelta import Timedelta
from langchain_ollama import OllamaLLM
import json
from llmprompts import get_web_deets, prompt_template

# Setup the LLM
llm = OllamaLLM(model="qwen2.5:14b", format="json")


class CryptoTrader(Strategy):

    def initialize(self, cash_at_risk: float = 0.2, coin: str = "BTC"):
        self.set_market("24/7")
        self.sleeptime = "1D"
        self.last_trade = None
        self.cash_at_risk = cash_at_risk
        self.coin = coin

    def position_sizing(self):
        cash = self.get_cash()
        last_price = self.get_last_price(
            Asset(symbol=self.coin, asset_type=Asset.AssetType.CRYPTO),
            quote=Asset(symbol="USD", asset_type="crypto"),
        )
        if last_price == None:
            quantity = 0
        else:
            quantity = cash * self.cash_at_risk / last_price
        return cash, last_price, quantity

    # Added date retrieval
    def get_dates(self):
        today = self.get_datetime()
        day_prior = today - Timedelta(days=1)
        return today.strftime("%Y-%m-%d"), day_prior.strftime("%Y-%m-%d")

    # Added sentiment
    def get_sentiment(self):
        today, day_prior = self.get_dates()
        news = get_web_deets(day_prior, today)
        print(news)
        result = llm.invoke(prompt_template(news))
        print(result)
        return json.loads(result)

    def on_trading_iteration(self):
        cash, last_price, quantity = self.position_sizing()
        # Get News, Sentiment and Probability
        news_data = self.get_sentiment()
        sentiment = news_data["sentiment"]
        probability = news_data["score"]

        if last_price != None:
            if cash > (quantity * last_price):
                # Remove random choice and add sentiment trading to go with sentiment
                if sentiment == "positive" and probability >= 0.7:
                    if self.last_trade == "sell":
                        self.sell_all()
                    order = self.create_order(
                        Asset(symbol=self.coin, asset_type=Asset.AssetType.CRYPTO),
                        quantity,
                        "buy",
                        type="market",
                        quote=Asset(symbol="USD", asset_type="crypto"),
                    )
                    print(Fore.LIGHTMAGENTA_EX + str(order) + Fore.RESET)
                    self.submit_order(order)
                    self.last_trade = "buy"
                if sentiment == "negative" and probability >= 0.7:
                    if self.last_trade == "buy":
                        self.sell_all()
                    order = self.create_order(
                        Asset(symbol=self.coin, asset_type=Asset.AssetType.CRYPTO),
                        quantity,
                        "sell",
                        type="market",
                        quote=Asset(symbol="USD", asset_type="crypto"),
                    )
                    print(Fore.LIGHTMAGENTA_EX + str(order) + Fore.RESET)
                    self.submit_order(order)
                    self.last_trade = "sell"


if __name__ == "__main__":
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 11, 1)
    exchange_id = "kraken"
    kwargs = {
        "exchange_id": exchange_id,
    }
    CcxtBacktesting.MIN_TIMESTEP = "day"

    results, strat_obj = CryptoTrader.run_backtest(
        CcxtBacktesting,
        start_date,
        end_date,
        benchmark_asset="BTC/USD",
        quote_asset=Asset(symbol="USD", asset_type="crypto"),
        parameters={"cash_at_risk": 0.25, "coin": "BTC"},
        **kwargs,
    )
