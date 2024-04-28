import pandas as pd

from typing import List

from .broker_interface import BrokerInterface


class KuCoinInterface(BrokerInterface):

    def __init__(self, columns: List[str]):
        super().__init__(columns)
        self.broker = "KuCoin"
        self.index_columns = ["tradeCreatedAt", "symbol", "side"]
        self.agg_columns = ["size", "funds", "fee"]

    def get_transactions(self, file_name: str) -> pd.DataFrame:
        df = self._get_transactions(file_name, ",")
        return df

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        df.loc[df["side"] == "sell", "size"] *= -1
        df.loc[df["side"] == "buy", "funds"] *= -1
        return df