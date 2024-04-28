import pandas as pd

from typing import List

from .broker_interface import BrokerInterface


class BitvavoInterface(BrokerInterface):

    def __init__(self, columns: List[str]):
        super().__init__(columns)
        self.broker = "Bitvavo"
        self.index_columns = ["Datetime", "Currency", "Type"]
        self.agg_columns = ["Amount", "EUR received / paid", "Fee amount"]
        self.delimiter = ","

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        df["Time"] = df["Time"].str[:8]
        df["Datetime"] = df["Date"] + " " + df["Time"]
        df["Currency"] = df["Currency"] + "-EUR"
        return df