import pandas as pd

from typing import List

from .broker_interface import BrokerInterface


class KuCoinInterface(BrokerInterface):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.broker = "KuCoin"
        self.index_columns = ["tradeCreatedAt", "symbol", "side"]
        self.agg_columns = ["size", "funds", "fee"]
        self.delimiter = ","

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        df.loc[df["side"] == "sell", "size"] *= -1
        df.loc[df["side"] == "buy", "funds"] *= -1
        return df

    def get_withdrawals(self, file_path: str, columns: List[str]) -> List[str]:
        df = pd.read_csv(file_path, delimiter=self.delimiter, encoding="latin1")
        if "Wallet Address/Account" in df.columns:
            df.drop(columns=["Amount", "Type"], inplace=True)
            df["Fee"] = None
            df.columns = columns
            df["TxHash"] = None
            df["Chain"] = df["Chain"].apply(lambda x: x.split("(")[0]).str.upper()
            df.loc[df["Chain"] == "ARBITRUM", "Chain"] = "ARB"
            return df
        else:
            return pd.DataFrame(columns=columns)