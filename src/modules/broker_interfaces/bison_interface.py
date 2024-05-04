import pandas as pd

from typing import List

from .broker_interface import BrokerInterface


class BisonInterface(BrokerInterface):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.broker = "Bison"
        self.index_columns = [" Date", "Pair", "TransactionType"]
        self.agg_columns = [" AssetAmount", " EurAmount", " Fee"]
        self.delimiter = ";"

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

    def get_withdrawals(self, file_path: str, columns: List[str]) -> List[str]:
        df = pd.read_csv(file_path, delimiter=self.delimiter, encoding="latin1")
        for c in df.columns:
            if df[c].dtype == "object":
                df[c] = df[c].str.strip()
        df = df[df["TransactionType"] == "Withdraw"]
        df = df[df[" Currency"] == ""]
        df = df[df.columns[[7, 2, 0, 1, 3, 6]]]
        df.columns = columns
        df["Coin"] = df["Coin"].str.upper()
        df["Chain"] = None
        df["Address"] = None
        df["TxHash"] = None
        df["Fee"] = pd.to_numeric(df["Fee"], errors="coerce")
        return df
