from typing import List, Callable, Optional, Dict

import os
import json
from datetime import datetime
from tqdm import tqdm
import pandas as pd
from forex_python.converter import CurrencyRates


COLUMNS = ["Datetime", "Pair", "Side", "Price", "Size", "Funds", "Fee"]


class TransactionsHandler:
    """
    Loads transactions and converts all numbers in USD.
    """
    def __init__(self, cache_root: str) -> None:
        self.transactions = pd.DataFrame(columns=COLUMNS)
        self.cache_path = cache_root + "/conversion"
        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)
        # Get EUR to USD conversion
        self.converter = CurrencyRates()

    def add_transactions_from_csv(self, file_path: str) -> None:
        if "mexc" in file_path:
            df = self._get_transactions_mexc(file_path)
        elif "kucoin" in file_path:
            df = self._get_transactions_kucoin(file_path)
        elif "bitvavo" in file_path:
            df = self._get_transactions_bitvavo(file_path)

        if self.transactions.empty:
            self.transactions = df
        else:
            self.transactions = pd.concat((self.transactions, df))
        self.transactions.sort_values(by=["Datetime"] , inplace=True)
        self.transactions.reset_index(drop=True, inplace=True)

    def _get_transactions(
        self,
        file_name: str,
        index_columns: List[str],
        agg_columns: List[str],
        delimiter: str = ",",
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
        df = pd.read_csv(file_name, delimiter=delimiter)
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
        df["Price"] = -1 * df[agg_columns[1]] / df[agg_columns[0]]

        # Sort and reset index
        df.sort_index(inplace=True)
        df.reset_index(inplace=True)
        columns_reordered = index_columns + ["Price"] + agg_columns
        df = df[columns_reordered]
        df.columns = self.transactions.columns

        return df

    def _get_transactions_mexc(self, file_name: str) -> pd.DataFrame:
        def preprocess_mexc(df: pd.DataFrame) -> pd.DataFrame:
            df.loc[df["Seite"] == "SELL", "Ausgeführter Betrag"] *= -1
            df.loc[df["Seite"] == "BUY", "Gesamt"] *= -1
            return df
        index_columns = ["Zeit", "Paare", "Seite"]
        agg_columns = ["Ausgeführter Betrag", "Gesamt", "Gebühr"]
        df = self._get_transactions(file_name, index_columns, agg_columns, ";", preprocess_mexc)

        return df

    def _get_transactions_kucoin(self, file_name: str) -> pd.DataFrame:
        def preprocess_kucoin(df: pd.DataFrame) -> pd.DataFrame:
            df.loc[df["side"] == "sell", "size"] *= -1
            df.loc[df["side"] == "buy", "funds"] *= -1
            return df
        index_columns = ["tradeCreatedAt", "symbol", "side"]
        agg_columns = ["size", "funds", "fee"]
        df = self._get_transactions(file_name, index_columns, agg_columns, ",", preprocess_kucoin)

        return df
        
    def _get_transactions_bitvavo(self, file_name: str) -> pd.DataFrame:
        # Get conversion data to convert from EUR to USD
        from_curr = "EUR"
        to_curr = "USD"
        conversion_data = self._get_conversion_dict(from_curr, to_curr)

        def preprocess_bitvavo(df: pd.DataFrame) -> pd.DataFrame:
            df["Time"] = df["Time"].str[:8]
            df["Datetime"] = df["Date"] + " " + df["Time"]
            df["Currency"] = df["Currency"] + "-EUR"
            return df
        index_columns = ["Datetime", "Currency", "Type"]
        agg_columns = ["Amount", "EUR received / paid", "Fee amount"]
        df = self._get_transactions(file_name, index_columns, agg_columns, ",", preprocess_bitvavo)

        df["Conversion"] = df["Datetime"].apply(
            lambda str_timestamp: self._get_conversion_rate(
                from_curr,
                to_curr,
                str_timestamp[:10],
                conversion_data
            )
        )
        df["Price"] *= df["Conversion"]
        df["Funds"] *= df["Conversion"]
        df["Fee"] *= df["Conversion"]
        df["Pair"] = df["Pair"].str.replace("EUR", "USD")
        df.drop(columns="Conversion", inplace=True)

        self._save_conversion_dict(from_curr, to_curr, conversion_data)

        return df

    def _get_conversion_rate(
            self,
            from_curr: str,
            to_curr: str,
            str_timestamp: str,
            conversion_data: Dict
        )-> float:
        if not str_timestamp in conversion_data.keys():
            timestamp = datetime.strptime(str_timestamp, "%Y-%m-%d")
            conversion = self.converter.get_rates(from_curr, timestamp)[to_curr]
            conversion_data[str_timestamp] = conversion
        return conversion_data[str_timestamp]

    def _get_conversion_dict(self, from_curr: str, to_curr: str):
        # Load conversion dict from file or create new dict
        file_name = "_".join([from_curr, to_curr]) + ".json"
        path = os.path.join(self.cache_path, file_name)
        if os.path.exists(path):
            with open(path, 'r') as file:
                conversion_data = json.load(file)
        else:
            conversion_data = dict()
        return conversion_data
    
    def _save_conversion_dict(self, from_curr: str, to_curr: str, conversion_data: Dict):
        file_name = "_".join([from_curr, to_curr]) + ".json"
        path = os.path.join(self.cache_path, file_name)
        with open(path, 'w') as file:
            json.dump(conversion_data, file, indent=4)
        
