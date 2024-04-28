import pandas as pd

from typing import List

from .broker_interface import BrokerInterface


class BitvavoInterface(BrokerInterface):

    def __init__(self, columns: List[str]):
        super().__init__(columns)
        self.broker = "Bitvavo"
        self.index_columns = ["Datetime", "Currency", "Type"]
        self.agg_columns = ["Amount", "EUR received / paid", "Fee amount"]

    def get_transactions(self, file_name: str) -> pd.DataFrame:
        df = self._get_transactions(file_name, ",")
        return df

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        df["Time"] = df["Time"].str[:8]
        df["Datetime"] = df["Date"] + " " + df["Time"]
        df["Currency"] = df["Currency"] + "-EUR"
        return df