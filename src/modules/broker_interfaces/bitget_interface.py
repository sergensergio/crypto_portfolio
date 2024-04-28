import pandas as pd

from typing import List

from .broker_interface import BrokerInterface


class BitgetInterface(BrokerInterface):

    def __init__(self, columns: List[str]):
        super().__init__(columns)
        self.broker = "Bitget"
        self.index_columns = ["Date", "Trading pair", "Direction"]
        self.agg_columns = ["Amount", "Total", "Fee"]

    def get_transactions(self, file_name: str) -> pd.DataFrame:
        df = self._get_transactions(file_name, ",")
        return df

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        df["Trading pair"] = df["Trading pair"].str.replace("_SPBL", "")
        df["Trading pair"] = df["Trading pair"].str.replace("USDT", "-USDT-")
        df["Trading pair"] = df["Trading pair"].str.strip("-")
        df.loc[df["Direction"] == "Sell", "Amount"] *= -1
        df.loc[df["Direction"] == "Buy", "Total"] *= -1
        df["Fee"] = -df["Fee"] * df["Price"]
        return df