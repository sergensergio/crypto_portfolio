import pandas as pd

from typing import List

from .broker_interface import BrokerInterface


class BisonInterface(BrokerInterface):

    def __init__(self, columns: List[str]):
        super().__init__(columns)
        self.broker = "Bison"
        self.index_columns = [" Date", "Pair", "TransactionType"]
        self.agg_columns = [" AssetAmount", " EurAmount", " Fee"]

    def get_transactions(self, file_name: str) -> pd.DataFrame:
        df = self._get_transactions(file_name, ";")
        return df

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        for c in df.columns:
            if df[c].dtype == "object":
                df[c] = df[c].str.strip()
        df["Pair"] = df[" Asset"].str.upper() + "-" + df[" Currency"].str.upper()
        df[" AssetAmount"] = pd.to_numeric(df[' AssetAmount'], errors='coerce')
        df[" EurAmount"] = pd.to_numeric(df[' EurAmount'], errors='coerce')
        df[" Fee"] = pd.to_numeric(df[' Fee'], errors='coerce')
        # Set sign
        df.loc[df["TransactionType"] == "Sell", " AssetAmount"] *= -1
        df.loc[df["TransactionType"] == "Buy", " EurAmount"] *= -1
        return df