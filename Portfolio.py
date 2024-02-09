from typing import Union, Dict

import glob
import os
import json
from datetime import datetime
from tqdm import tqdm
from requests import Session
import pandas as pd
import matplotlib.pyplot as plt

from transactions_handler import TransactionsHandler


URL_CMC = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"


class Portfolio:
    def __init__(
        self,
        cache_root: str = "cache",
        api_key_path: str = "api_key.txt",
    ) -> None:
        self.transactions_handler = TransactionsHandler(cache_root)
        self.cache_path = cache_root + "/symbols"
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
        Fetch data for crypto via API call and save it in the cache.
        """
        headers = {
            "Accepts": "application/json",
            "X-CMC_PRO_API_KEY": self.api_key
        }

        parameters = {"symbol": symbol, "convert": "USD"}
        session = Session()
        session.headers.update(headers)
        response = session.get(URL_CMC, params=parameters)

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
        # Get transactions and split pair column
        df = self.transactions_handler.transactions.copy()
        index_columns = ["Symbol Buy", "Symbol Sell"]
        df[index_columns] = df["Pair"].str.split("-", expand=True)

        # Sum up all buy and sell sizes
        buy = df.groupby("Symbol Buy").agg({
            "Size": "sum"
        }).reset_index()
        sell = df.groupby("Symbol Sell").agg({
            "Funds": "sum"
        }).reset_index()
        buy.columns = ["Asset", "Size"]
        sell.columns = ["Asset", "Size"]
        pf = pd.concat((buy, sell)).groupby("Asset").sum()

        # Get total spent value and drop row
        total_spent = pf.loc["USD"]["Size"]
        pf.drop("USD", inplace=True)

        # Set current market prices
        pf["Current Price"] = 0.0
        for symbol in tqdm(
            pf.index,
            desc="Get current prices",
            total=len(pf.index)
        ):
            if symbol == "CHNG":
                current_price = 0.098
            else:
                data = self._get_data_for_symbol(symbol)
                current_price = data["data"][symbol]["quote"]["USD"]["price"]
            pf.loc[symbol, "Current Price"] = current_price

        # Set current value of assets
        pf["Current Value"] = pf["Current Price"] * pf["Size"]
        pf = pf[pf["Current Value"] > 0.1]

        pf.reset_index(inplace=True)
        pf.sort_values("Current Value", inplace=True)

        # Show and plot information
        print(f"Total invested money (USD): {total_spent}")
        print(f"Total portfolio value (USD): {pf['Current Value'].sum()}")
        plt.figure(figsize=(10, 7))
        plt.pie(
            pf['Current Value'],
            labels=pf['Asset'],
            autopct=lambda pct: "{:d}".format(int(pct*pf["Current Value"].sum()/100)),
            startangle=90,
            pctdistance=0.8
        )
        plt.title('Portfolio Value Distribution')
        plt.show()

if __name__ == "__main__":
    path_csvs = "exports"

    pf = Portfolio()
    for filename in glob.iglob(path_csvs + "/**/*.csv", recursive=True):
        pf.add_transactions_from_csv(file_path=filename)
        
    pf.show_portfolio()