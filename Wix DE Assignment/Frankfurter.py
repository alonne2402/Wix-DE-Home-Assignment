import logging
from BaseAPI import BaseAPI
import pandas as pd
import json

class FrankfurterAPI(BaseAPI):
    def __init__(self, api_key=None, base_rul=None, end_point=None, timeout=30, retries=3):
        super().__init__(api_key, base_rul, end_point)
        self.timeout = timeout
        self.retries = retries

    # converts dictionary with exchange rates into data frame
    def _convert_rates_to_dataframe(self, rates, start_date):
        data = []

        if all(isinstance(value, dict) for value in rates.values()):
            # case 1: The dictionary contains dates as keys (nested structure)
            for date, currencies in rates.items():
                for currency, rate in currencies.items():
                    data.append({"exchange_date": date, "currency": currency, "exchange_rate": rate})
        else:
            # case 2: The dictionary contains currencies directly (no date)
            for currency, rate in rates.items():
                data.append({"exchange_date": start_date, "currency": currency, "exchange_rate": rate})

        return pd.DataFrame(data)

    # fetching exchange rates for a period between dates.
    def get_exchange_rates_for_period(self, start_date, end_date=None, base_currency="USD", target_currency=None, symbols=None):
        if end_date is None:
            endpoint = start_date # תאריך ההתחלה כחלק מה-path
        else:
            endpoint = f"{start_date}..{end_date}"
        url = f'{self._build_url()}/{endpoint}'
        print(url)
        params = {
            "base": base_currency
        }
        # adding a target currency if specified.
        if target_currency:
            params["to"] = target_currency
        # adding specific stocks if specified.
        if symbols:
            params["symbols"] = symbols

        # send request to API
        data = self._build_request(url, params)
        # validation of received data
        if "rates" in data:
            return self._convert_rates_to_dataframe(data["rates"], start_date)
        else:
            logging.warning(f"No exchange rates found for period {start_date} to {end_date}")
            return {}
