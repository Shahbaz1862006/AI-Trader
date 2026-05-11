from dotenv import load_dotenv
from langchain_community.utilities import GoogleSerperAPIWrapper

load_dotenv()
search = GoogleSerperAPIWrapper(k=15)

coin_name = "BTC"
news_start_date = "2023-12-31"
news_end_date = "2024-01-01"

search.results(f"{coin_name} price before:{news_end_date} after:{news_start_date}")
