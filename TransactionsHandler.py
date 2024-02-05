from typing import List, Callable, Optional, Union, Dict

import os
import json
import requests
from datetime import datetime
from tqdm import tqdm
from requests import Session
import pandas as pd
from forex_python.converter import CurrencyRates


COLUMNS = ["Datetime", "Pair", "Side", "Price", "Size", "Funds", "Fee"]


class TransactionsHandler:

    def __init__(self) -> None:
        self.transactions = pd.DataFrame(columns=COLUMNS)

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
        df = self._get_transactions(file_name, index_columns, agg_columns, preprocess_mexc)

        return df

    def _get_transactions_kucoin(self, file_name: str) -> pd.DataFrame:
        def preprocess_kucoin(df: pd.DataFrame) -> pd.DataFrame:
            df.loc[df["side"] == "sell", "size"] *= -1
            df.loc[df["side"] == "buy", "funds"] *= -1
            return df
        index_columns = ["tradeCreatedAt", "symbol", "side"]
        agg_columns = ["size", "funds", "fee"]
        df = self._get_transactions(file_name, index_columns, agg_columns, preprocess_kucoin)

        return df
        
    def _get_transactions_bitvavo(self, file_name: str) -> pd.DataFrame:
        def preprocess_bitvavo(df: pd.DataFrame) -> pd.DataFrame:
            df["Time"] = df["Time"].str[:8]
            df["Datetime"] = df["Date"] + " " + df["Time"]
            df["Currency"] = df["Currency"] + "-EUR"
            return df
        index_columns = ["Datetime", "Currency", "Type"]
        agg_columns = ["Amount", "EUR received / paid", "Fee amount"]
        df = self._get_transactions(file_name, index_columns, agg_columns, preprocess_bitvavo)

        return df
