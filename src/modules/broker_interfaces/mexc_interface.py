import pandas as pd

from typing import List

from .broker_interface import BrokerInterface


class MEXCInterface(BrokerInterface):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.broker = "MEXC"
        self.index_columns = ["Zeit", "Paare", "Seite"]
        self.agg_columns = ["Ausgeführter Betrag", "Gesamt", "Gebühr"]
        self.delimiter = ";"

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        df.loc[df["Seite"] == "SELL", "Ausgeführter Betrag"] *= -1
        df.loc[df["Seite"] == "BUY", "Gesamt"] *= -1
        return df

    def get_withdrawals(self, file_path: str, columns: List[str]) -> pd.DataFrame:
        df = pd.read_csv(file_path, delimiter=self.delimiter, encoding="latin1")
        if "Auszahlungsadresse" in df.columns:
            df[["Coin", "Chain"]] = df["Krypto"].str.split("-", expand=True)
            df.drop(columns=["Status", "Angeforderter Betrag", "Abrechnungsbetrag", "Auszahlungsbeschreibungen", "Krypto"], inplace=True)
            df = df[df.columns[[0,4,5,1,2,3]]]
            df.columns = columns
            return df
        else:
            return pd.DataFrame(columns=columns)