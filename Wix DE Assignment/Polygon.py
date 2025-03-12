from BaseAPI import BaseAPI
import pandas as pd



class PolygonAPI(BaseAPI):
    def __init__(self, api_key, base_rul=None, end_point=None, timeout=30, retries=3):
        super().__init__(api_key, base_rul, end_point)
        self.timeout = timeout
        self.retries = retries

    # pull stock market data for specific day
    def get_stock_data(self, date):
        url = f'{self._build_url()}/{date}'
        print(url)
        params = {
            "adjusted": "true",
            "apiKey": self.api_key,
            "limit": 50000
        }
        stocks_data = []

        while True:
            data = self._build_request(url, params)
            if data and "results" in data:
                for stock in data["results"]:
                    stocks_data.append(
                        {
                            "stock_symbol": stock["T"],
                            "stock_date": date,
                            "open_price": stock["o"],
                            "high_price": stock["h"],
                            "low_price": stock["l"],
                            "close_price": stock["c"],
                            "volume": stock["v"],
                            "transactions": stock["n"] if "n" in stock else 0

                        }
                    )
            if not data or "next_url" not in data: #checking if there is next page for more data
                break
            url = data["next_url"]  # updating URL address
        return pd.DataFrame(stocks_data)
