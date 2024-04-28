import pandas as pd

from typing import List

from .broker_interface import BrokerInterface


class MEXCInterface(BrokerInterface):

    def __init__(self, columns: List[str]):
        super().__init__(columns)
        self.broker = "MEXC"
        self.index_columns = ["Zeit", "Paare", "Seite"]
        self.agg_columns = ["Ausgeführter Betrag", "Gesamt", "Gebühr"]

    def get_transactions(self, file_name: str) -> pd.DataFrame:
        df = self._get_transactions(file_name, ";")
        return df

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        df.loc[df["Seite"] == "SELL", "Ausgeführter Betrag"] *= -1
        df.loc[df["Seite"] == "BUY", "Gesamt"] *= -1
        return df