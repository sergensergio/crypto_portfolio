from typing import List, Callable, Optional, Union, Dict

import os
import json
from datetime import datetime
from tqdm import tqdm
from requests import Session
import pandas as pd
from forex_python.converter import CurrencyRates

from TransactionsHandler import TransactionsHandler


URL_CMC = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"


class Portfolio:
    def __init__(
        self,
        cache_path: str = "cache",
        api_key_path: str = "api_key.txt",
    ) -> None:
        self.transactions_handler = TransactionsHandler()
        self.cache_path = cache_path
        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)
        if not os.path.exists(api_key_path):
            raise FileNotFoundError(f"{api_key_path} not found.")
        with open(api_key_path, "r") as file:
            self.api_key = file.read()

    def add_transactions_from_csv(self, file_path: str) -> None:
        self.transactions_handler.add_transactions_from_csv(file_path)

    def _get_data_from_cache(self, symbol: str) -> Union[Dict, None]:
        """
        Check cache if requested symbol has been fetched already today.
        If available, return cached data. Otherwise return None
        """
        file_name = symbol + ".json"
        path = os.path.join(self.cache_path, file_name)
        if os.path.exists(path):
            with open(path, 'r') as file:
                symbol_data = json.load(file)
            now = datetime.strftime(datetime.utcnow(), "%Y-%m-%d")
            cache_time = symbol_data["status"]["timestamp"][:10]
            if now == cache_time:
                return symbol_data

        return None

    def _get_data_from_api(self, symbol: str) -> Dict:
        """
        Fetch data for crypto via API call.‚
        """
        headers = {
            "Accepts": "application/json",
            "X-CMC_PRO_API_KEY": self.api_key
        }

        parameters = { "symbol": symbol, "convert": "USD" }
        session = Session()
        session.headers.update(headers)
        response = session.get(URL_CMC, params=parameters) # Receiving the response from the API

        new_data = json.loads(response.text)

        file_name = symbol + ".json"
        path = os.path.join(self.cache_path, file_name)
        with open(path, 'w') as file:
            json.dump(new_data, file, indent=4)
        
        return new_data

    def _get_data_for_symbol(self, symbol: str) -> Dict:
        """
        Returns Coinmarketcap API response for a cryptocurrency as dict.
        Saves data in cache to reduce API calls. If the data has been
        fetched today already the cached data is returned.

        Args:
            symbol: Symbol of currency, eg. BTC or ETH
        """

        data = self._get_data_from_cache(symbol)
        if data:
            return data

        new_data = self._get_data_from_api(symbol)
        
        return new_data

    def show_portfolio(self):
        # Sum up transactions for same buy symbol and different currencies
        df = self.transactions_handler.transactions.copy()
        index_columns = ["Symbol Buy", "Symbol Sell"]
        df[index_columns] = df["Pair"].str.split("-", expand=True)
        df.set_index(index_columns, inplace=True)
        df = df.groupby(level=index_columns).agg({
            "Size": "sum",
            "Funds": "sum",
            "Fee": "sum"
        })
        df.reset_index(inplace=True)

        # Convert EUR values to USD
        c = CurrencyRates()
        eur_in_usd = c.get_rates("EUR")["USD"]
        df.loc[df["Symbol Sell"] == "EUR", ["Funds", "Fee"]] *= eur_in_usd

        # Sum up transactions for same buy symbol
        df.set_index("Symbol Buy", inplace=True)
        df = df.groupby(level="Symbol Buy").agg({
            "Size": "sum",
            "Funds": "sum",
            "Fee": "sum"
        })

        df["Current Price"] = 0.0

        for symbol in tqdm(
            df.index,
            desc="Get current prices",
            total=len(df)
        ):
            data = self._get_data_for_symbol(symbol)
            current_price = data["data"][symbol]["quote"]["USD"]["price"]
            df.loc[symbol, "Current Price"] = current_price

        df["Current Value"] = df["Current Price"] * df["Size"]
        df["Profit/Loss"] = df["Current Value"] + df["Funds"] - df["Fee"]

        print(df)


if __name__ == "__main__":
    path_root = "exports"
    csv_names = os.listdir(path=path_root)
    pf = Portfolio()
    for c in csv_names:
        file_name = os.path.join(path_root, c)
        pf.add_transactions_from_csv(file_path=file_name)
    pf.show_portfolio()