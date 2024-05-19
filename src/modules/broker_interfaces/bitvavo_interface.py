import pandas as pd

from typing import List

from .broker_interface import BrokerInterface


class BitvavoInterface(BrokerInterface):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.broker = "Bitvavo"
        self.index_columns = ["Datetime", "Currency", "Type"]
        self.agg_columns = ["Amount", "EUR received / paid", "Fee amount"]
        self.delimiter = ","

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        df["Time"] = df["Time"].str[:8]
        df["Datetime"] = df["Date"] + " " + df["Time"]
        df["Currency"] = df["Currency"] + "-EUR"
        return df

    def get_withdrawals(self, file_path: str, columns: List[str]) -> pd.DataFrame:
        df = pd.read_csv(file_path, delimiter=self.delimiter, encoding="latin1")
        df = df[df["Type"] == "withdrawal"]
        df["Datetime"] = df["Date"] + " " + df["Time"]
        df = df[["Datetime", "Currency", "Timezone", "Address", "Status", "Fee amount", "Fee currency"]]
        df.columns = columns
        df["TxHash"] = None
        df["Chain"] = None
        return df