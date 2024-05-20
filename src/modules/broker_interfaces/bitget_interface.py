import pandas as pd

from typing import List

from .broker_interface import BrokerInterface


class BitgetInterface(BrokerInterface):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.broker = "Bitget"
        self.index_columns = ["Date", "Trading pair", "Direction"]
        self.agg_columns = ["Amount", "Total", "Fee"]
        self.delimiter = ","

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        df["Trading pair"] = df["Trading pair"].str.replace("_SPBL", "")
        df["Trading pair"] = df["Trading pair"].str.replace("USDT", "-USDT-")
        df["Trading pair"] = df["Trading pair"].str.strip("-")
        df.loc[df["Direction"] == "Sell", "Amount"] *= -1
        df.loc[df["Direction"] == "Buy", "Total"] *= -1
        df["Fee"] = -df["Fee"] * df["Price"]
        return df

    def get_withdrawals(self, file_path: str, columns: List[str]) -> pd.DataFrame:
        df = pd.read_csv(file_path, delimiter=self.delimiter)
        df = df[df["Type"] == "Ordinary Withdrawal"]
        df["Chain"] = None
        df["Address"] = None
        df["TxHash"] = None
        df["Fee currency"] = df["Coin"]
        df = df[df.columns[[0,1,7,8,9,4,10]]]
        df.columns = columns
        return df