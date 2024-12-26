import pandas as pd

from typing import List

from .broker_interface import BrokerInterface
from ..utils.utils import update_df

class BitgetInterface(BrokerInterface):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.broker = "Bitget"
        self.index_columns = ["Date", "Trading pair", "Direction"]
        self.agg_columns = ["Amount", "Total", "Fee"]
        self.delimiter = ","

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        if "Trading pair" in df.columns:
            df["Trading pair"] = df["Trading pair"].str.replace("_SPBL", "")
            df["Trading pair"] = df["Trading pair"].str.replace("USDT", "-USDT-")
            df["Trading pair"] = df["Trading pair"].str.strip("-")
            df.loc[df["Direction"] == "Sell", "Amount"] *= -1
            df.loc[df["Direction"] == "Buy", "Total"] *= -1
            df["Fee"] = -df["Fee"] * df["Price"]
        else:
            df = df[df["type"].isin(["Buy", "Sell"])]
            df = df.groupby(["date", "type", "coin"]).agg({"amount": "sum", "fee": "sum"})
            df_new = pd.DataFrame()
            for d in df.index.levels[0]:
                buy_sym = df.loc[d].loc["Buy"].index[0]
                sell_sym = df.loc[d].loc["Sell"].index[0]
                size = df.loc[d].loc["Buy", "amount"].values[0]
                funds = df.loc[d].loc["Sell", "amount"].values[0]
                fee = df.loc[d].loc["Buy", "fee"].values[0]
                price = abs(funds) / (abs(size) + abs(fee))
                fee_in_sell_sym = price * fee
                new_row = pd.DataFrame([{
                    "Date": d,
                    "Trading pair": buy_sym + "-" + sell_sym,
                    "Direction": "Buy",
                    "Amount": size,
                    "Total": funds,
                    "Fee": fee_in_sell_sym
                }])
                df_new = update_df(df_new, new_row)
            df = df_new

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