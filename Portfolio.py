from typing import List, Callable, Optional, Union, Dict

import os
import json
import requests
from datetime import datetime
from tqdm import tqdm
from requests import Session
import pandas as pd
from forex_python.converter import CurrencyRates


URL_CMC = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"


class Portfolio:

    def __init__(
        self,
        cache_path: str = "cache",
        api_key_path: str = "api_key.txt",
    ) -> None:
        columns = ["Datetime", "Pair", "Side", "Price", "Size", "Funds", "Fee"]
        self.transactions = pd.DataFrame(columns=columns)
        self.cache_path = cache_path
        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)
        if not os.path.exists(api_key_path):
            raise FileNotFoundError(f"{api_key_path} not found.")
        with open(api_key_path, "r") as file:
            self.api_key = file.read()

    def add_transactions_from_csv(self, file_path: str) -> None:
        if "mexc" in file_path:
            df = self._get_transactions_mexc(file_path)
        elif "kucoin" in file_path:
            df = self._get_transactions_kucoin(file_path)
        elif "bitvavo" in file_path:
            df = self._get_transactions_bitvavo(file_path)

        self.transactions = pd.concat((self.transactions, df))
        self.transactions.sort_values(by=["Datetime"] , inplace=True)
        self.transactions.reset_index(drop=True, inplace=True)

    def _get_transactions(
        self,
        file_name: str,
        index_columns: List[str],
        agg_columns: List[str],
        preprocess: Optional[Callable] = None
    ) -> pd.DataFrame:
        """
        Args:
            file_name: path to transaction csv
            index_columns: column names used for the grouping of the data frame.
                Values should correspond to ["Datetime", "Pair", "Side"] in this order
            agg_columns: column names used to aggregate values for the grouping
                of the data frame. Values should correspond to ["Size", "Funds", "Fee"] in
                this order
            preprocess: Optional preprocessing function to call right after reading csv file
        Return
        """
        # Read
        df = pd.read_csv(file_name)
        if preprocess:
            df = preprocess(df)
        df[index_columns[1]] = df[index_columns[1]].str.replace("_", "-")
        df[index_columns[2]] = df[index_columns[2]].str.lower()
        df = df[df[index_columns[2]].isin(["buy", "sell"])]

        # Group
        df.set_index(index_columns, inplace=True)
        agg_dict = {key: "sum" for key in agg_columns}
        df = df.groupby(level=index_columns).agg(agg_dict)
        
        # Set average buy price
        df["Price"] = df[agg_columns[1]] / df[agg_columns[0]]

        # Sort and reset index
        df.sort_index(inplace=True)
        df.reset_index(inplace=True)
        columns_reordered = index_columns + ["Price"] + agg_columns
        df = df[columns_reordered]
        df.columns = self.transactions.columns

        return df

    def _get_transactions_mexc(self, file_name: str) -> pd.DataFrame:

        index_columns = ["Zeit", "Paare", "Seite"]
        agg_columns = ["Ausgeführter Betrag", "Gesamt", "Gebühr"]
        df = self._get_transactions(file_name, index_columns, agg_columns)

        return df

    def _get_transactions_kucoin(self, file_name: str) -> pd.DataFrame:
        
        index_columns = ["tradeCreatedAt", "symbol", "side"]
        agg_columns = ["size", "funds", "fee"]
        df = self._get_transactions(file_name, index_columns, agg_columns)

        return df
        
    def _get_transactions_bitvavo(self, file_name: str) -> pd.DataFrame:
        def preprocess_bitvavo(df: pd.DataFrame) -> pd.DataFrame:
            df["Time"] = df["Time"].str[:8]
            df["Datetime"] = df["Date"] + " " + df["Time"]
            df["Currency"] = df["Currency"] + "-EUR"
            df["EUR received / paid"] = df["EUR received / paid"].abs()
            df["Amount"] = df["Amount"].abs()
            return df
        index_columns = ["Datetime", "Currency", "Type"]
        agg_columns = ["Amount", "EUR received / paid", "Fee amount"]
        df = self._get_transactions(file_name, index_columns, agg_columns, preprocess_bitvavo)

        return df

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
        index_columns = ["Pair", "Side"]
        df = self.transactions.set_index(index_columns)
        df = df.groupby(level=index_columns).agg({
            "Size": "sum",
            "Funds": "sum",
            "Fee": "sum"
        })
        df = df.unstack(level="Side").fillna(0)
        s_size = df["Size"]["buy"] - df["Size"]["sell"]
        s_funds = df["Funds"]["sell"] - df["Funds"]["buy"]
        s_fee = df["Fee"]["buy"] + df["Fee"]["sell"]

        c = CurrencyRates()
        eur_in_usd = c.get_rates("EUR")["USD"]
        profits = []
        for pair, size, fund, fee in tqdm(
            zip(s_size.index, s_size.values, s_funds.values, s_fee.values),
            desc="Get current prices",
            total=len(s_size.index)
        ):
            symbol, currency = pair.split("-")
            if currency == "EUR":
                fund *= eur_in_usd
                fee *= eur_in_usd

            data = self._get_data_for_symbol(symbol)
            current_price = data["data"][symbol]["quote"]["USD"]["price"]
            current_value = current_price * size
            profits.append({symbol: current_value + fund})

        df_profits = pd.DataFrame(profits)


if __name__ == "__main__":
    path_root = "exports"
    csv_names = os.listdir(path=path_root)
    pf = Portfolio()
    for c in csv_names:
        file_name = os.path.join(path_root, c)
        pf.add_transactions_from_csv(file_path=file_name)
    pf.show_portfolio()