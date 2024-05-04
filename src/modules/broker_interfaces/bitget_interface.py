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

    def get_withdrawals(self, file_path: str, columns: List[str]) -> List[str]:
        return pd.DataFrame(columns=columns)