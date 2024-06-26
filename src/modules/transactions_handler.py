import os
import pandas as pd
import numpy as np
from typing import List, Callable, Optional, Dict, Tuple

from .conversion_handler import ConversionHandler
from .broker_interfaces import BisonInterface, BitvavoInterface, KuCoinInterface, MEXCInterface, BitgetInterface
from .blockchain_explorer import BlockchainExplorer
from .utils.utils import update_df


COLUMNS = ["Datetime", "Pair", "Side", "Size", "Funds", "Fee", "Fee currency", "Broker"]
DTYPES = ["object", "object", "object", "float", "float", "float", "object", "object"]
COLUMNS_W = ["Datetime", "Coin", "Chain", "Address", "TxHash", "Fee", "Fee currency"]
DTYPES_W = ["object", "object", "object", "object", "object", "float", "object"]


class TransactionsHandler:
    """
    Loads transactions and converts all numbers in USD.
    """
    def __init__(self, cache_root: str, history_root: str) -> None:
        self.transactions = pd.DataFrame(columns=COLUMNS)
        for c, d in zip(COLUMNS, DTYPES):
            self.transactions[c] = self.transactions[c].astype(d)
        self.withdrawals = pd.DataFrame(columns=COLUMNS_W)
        for c, d in zip(COLUMNS_W, DTYPES_W):
            self.withdrawals[c] = self.withdrawals[c].astype(d)
        self.conversion_handler = ConversionHandler(cache_root)
        self.blockchain_explorer = BlockchainExplorer()
        self.history_root = history_root

    def _extend_transactions_dataframe(self, df: pd.DataFrame):
        """
        Concats df to self.transactions
        """
        self.transactions = update_df(self.transactions, df)
        self.transactions.sort_values(by=["Datetime"] , inplace=True)
        self.transactions.drop_duplicates(subset=["Datetime", "Pair", "Side"], inplace=True)
        self.transactions.reset_index(drop=True, inplace=True)

    def _extend_withdrawals_dataframe(self, df: pd.DataFrame):
        """
        Concats df to self.withdrawals
        """
        self.withdrawals = update_df(self.withdrawals, df)
        self.withdrawals.sort_values(by=["Datetime"] , inplace=True)
        self.withdrawals.drop_duplicates(subset=["Datetime", "Coin", "Fee currency"], inplace=True)
        self.withdrawals.reset_index(drop=True, inplace=True)

    def _prepare_dataframe_from_dict_list(self, d_l: List[Dict]) -> pd.DataFrame: 
        # Check dict keywords
        for d in d_l:
            keys = d.keys()
            assert all([key in COLUMNS for key in keys]), \
            f"Unrecognised keyword in dict. Use {[c for c in COLUMNS if c != 'Price']}"

        # Create dataframe and set correct values
        df = pd.DataFrame(d_l)
        def sign_side(elem):
            return 1 if elem == "buy" else -1
        df["sign"] = df["Side"].apply(lambda x: sign_side(x))
        df["Size"] = df["sign"] * abs(df["Size"])
        df["Funds"] = -1 * df["sign"] * abs(df["Funds"])
        df.drop(columns="sign", inplace=True)

        return df

    def add_transactions_manually(self, transaction_dict: Dict):
        df = self._prepare_dataframe_from_dict_list(transaction_dict)
        df = self._sanitize_df(df, COLUMNS, DTYPES)
        df = self._convert_transactions(df)
        self._extend_transactions_dataframe(df)

    def add_transactions_from_csv(self, file_path: str) -> None:
        if "mexc" in file_path:
            broker = MEXCInterface(columns=COLUMNS)
        elif "kucoin" in file_path:
            broker = KuCoinInterface(columns=COLUMNS)
        elif "bitvavo" in file_path:
            broker = BitvavoInterface(columns=COLUMNS)
        elif "bison" in file_path:
            broker = BisonInterface(columns=COLUMNS)
        elif "bitget" in file_path:
            broker = BitgetInterface(columns=COLUMNS)
        else:
            raise NotImplementedError(f"Broker not recognised: {file_path}")
    
        df = broker.get_transactions(file_path)
        df = self._sanitize_df(df, COLUMNS, DTYPES)
        df = self._convert_transactions(df)
        self._extend_transactions_dataframe(df)

    def _convert_transactions(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Converts transactions with EUR to USD.
        """
        if not df["Pair"].str.contains("EUR").any():
            return df
        # Get conversion data to convert from EUR to USD
        from_curr = "EUR"
        to_curr = "USD"
        self.conversion_handler.load_conversion_dict(from_curr, to_curr)

        df["Conversion"] = df.loc[df["Pair"].str.contains("EUR"), "Datetime"].apply(
            lambda str_timestamp: self.conversion_handler.get_conversion_rate(
                from_curr,
                to_curr,
                str_timestamp[:10]
            )
        )
        df.loc[df["Pair"].str.contains("EUR"), "Funds"] *= df["Conversion"]
        df.loc[df["Fee currency"].str.contains("EUR"), "Fee"] *= df["Conversion"]
        df["Pair"] = df["Pair"].str.replace("EUR", "USD")
        df["Fee currency"] = df["Fee currency"].str.replace("EUR", "USD")
        df.drop(columns="Conversion", inplace=True)

        self.conversion_handler.save_conversion_dict(from_curr, to_curr)

        return df

    def _sanitize_df(self, df: pd.DataFrame, columns: List[str], dtypes: List[str]) -> pd.DataFrame:
        df = df[columns]
        for c, d in zip(columns, dtypes):
            df[c] = df[c].astype(d)
        return df

    def add_withdrawals_from_csv(self, file_path: str) -> None:
        if "mexc" in file_path:
            broker = MEXCInterface()
        elif "kucoin" in file_path:
            broker = KuCoinInterface()
        elif "bitvavo" in file_path:
            broker = BitvavoInterface()
        elif "bison" in file_path:
            broker = BisonInterface()
        elif "bitget" in file_path:
            broker = BitgetInterface()
        else:
            raise NotImplementedError(f"Broker not recognised: {file_path}")
        
        df = broker.get_withdrawals(file_path, COLUMNS_W)
        df = self._sanitize_df(df, COLUMNS_W, DTYPES_W)
        self._extend_withdrawals_dataframe(df)

    def _get_historical_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Gets historical data for all sell symbols which are not USD and puts them in
        a new df. The rows which are not USD are also returned."""
        # Get transations where sell symbol is not USD
        df_swaps_org = self.transactions[self.transactions["Pair"].apply(lambda x: x.split('-')[1]) != "USD"].copy()
        df_fee = self.transactions[self.transactions["Fee currency"] != "USD"].copy()

        # Get historical data for those symbols
        df_hist = pd.DataFrame()
        for sym in np.unique(np.concatenate((
            df_swaps_org["Pair"].apply(lambda x: x.split('-')[1]).unique(),
            df_fee["Fee currency"].unique()
        ))):
            df = pd.read_csv(os.path.join(self.history_root, sym + ".csv"), delimiter=";")
            df["sym"] = sym
            df_hist = update_df(df_hist, df)
        df_hist["day"] = df_hist["timestamp"].str[:10]
        df_hist = df_hist[["open", "close", "day", "sym"]]

        return df_swaps_org, df_hist

    def get_transactions_based_on_usd(self) -> pd.DataFrame:
        """
        Returns transactions with sell symbol equal to USD.
        For this, swaps are split into two transactions:
            Buy buy symbol with USD
            Sell sell symbol with USD
            Use sell symbol to determine price in USD
        Purchase of coins with USDT are also considered a swap.
        """
        if self.transactions["Pair"].apply(lambda x: x.split('-')[1] == "USD").all() and \
            (self.transactions["Fee currency"] == "USD").all():
            return self.transactions.copy()
        df_swaps_org, df_hist = self._get_historical_data()

        # Additional transactions
        df_swaps_add = df_swaps_org.copy()
        df_swaps_add["day"] = df_swaps_add["Datetime"].str[:10]
        df_swaps_add["sym"] = df_swaps_add["Pair"].apply(lambda x: x.split('-')[1])

        # Match historical data to rows from the additional transactions and keep index
        df_swaps_add = df_swaps_add.reset_index().merge(df_hist, on=["day", "sym"], how="left").set_index("index")

        # Rename sym column and match again for the fees
        df_swaps_add.rename(columns={"sym": "sym_pair"}, inplace=True)
        df_swaps_add["sym"] = df_swaps_add["Fee currency"]
        df_swaps_add = df_swaps_add.reset_index().merge(df_hist, on=["day", "sym"], how="left", suffixes=(None, "_fee")).set_index("index")

        # Set values for the new transactions
        df_swaps_add["price USD"] = (df_swaps_add["open"] + df_swaps_add["close"]) / 2
        df_swaps_add["price USD fee"] = (df_swaps_add["open_fee"] + df_swaps_add["close_fee"]) / 2
        df_swaps_add["Pair"] = df_swaps_add["Pair"].apply(lambda x: x.split('-')[1] + "-USD")
        df_swaps_add["Side"] = df_swaps_add["Side"].apply(lambda x: "sell" if x == "buy" else "buy")
        df_swaps_add["Size"] = df_swaps_add["Funds"]
        df_swaps_add["Funds"] = -df_swaps_add["price USD"] * df_swaps_add["Funds"]
        df_swaps_add["Fee"] = df_swaps_add["price USD fee"] * df_swaps_add["Fee"]
        df_swaps_add["Fee currency"] = "USD"

        df_swaps_add.drop(
            columns=[
                "open", "close", "day", "sym", "price USD", "sym_pair",
                "open_fee", "close_fee", "price USD fee"
            ],
            inplace=True
        )

        # Set values for original transactions
        df_swaps_org["Pair"] = df_swaps_org["Pair"].apply(lambda x: x.split('-')[0] + "-USD")
        df_swaps_org["Funds"] = -df_swaps_add["Funds"]
        df_swaps_org["Fee"] = 0.0
        df_swaps_org["Fee currency"] = "USD"

        df_return = pd.concat((
            df_swaps_org,
            df_swaps_add, 
            self.transactions[self.transactions['Pair'].apply(lambda x: x.split('-')[1]) == "USD"]
        ))

        df_return.sort_values("Datetime", inplace=True)
        df_return.reset_index(inplace=True)
        df_return.drop(columns="index", inplace=True)

        return df_return

    def get_fees_per_broker(self) -> pd.DataFrame:
        """
        Returns fees per broker in USD.
        """
        if self.transactions["Pair"].apply(lambda x: x.split('-')[1] == "USD").all() and \
            (self.transactions["Fee currency"] == "USD").all():
            df_no_usd = pd.DataFrame()
        else:
            df_no_usd, df_hist = self._get_historical_data()

            # Prepare merge
            df_no_usd["day"] = df_no_usd["Datetime"].str[:10]
            df_no_usd["sym"] = df_no_usd["Pair"].apply(lambda x: x.split('-')[1])

            # Match historical data to rows from the additional transactions and keep index
            df_no_usd = df_no_usd.reset_index().merge(df_hist, on=["day", "sym"], how="left").set_index("index")

            # Rename sym column and match again for the fees
            df_no_usd.rename(columns={"sym": "sym_pair"}, inplace=True)
            df_no_usd["sym"] = df_no_usd["Fee currency"]
            df_no_usd = df_no_usd.reset_index().merge(df_hist, on=["day", "sym"], how="left", suffixes=(None, "_fee")).set_index("index")

            # Convert to usd
            df_no_usd["price USD"] = (df_no_usd["open"] + df_no_usd["close"]) / 2
            df_no_usd["price USD fee"] = (df_no_usd["open_fee"] + df_no_usd["close_fee"]) / 2
            df_no_usd["Funds"] *= df_no_usd["price USD"]
            df_no_usd["Fee"] *= df_no_usd["price USD fee"]
        
        # Put in full df
        df = self.transactions.copy()
        df.loc[df_no_usd.index] = df_no_usd

        # Get fees per broker
        df_fees = df.groupby("Broker").agg({"Funds": "sum", "Fee": "sum"})
        df_fees["% Fee/Funds"] = (df_fees["Fee"] / df_fees["Funds"]).abs() * 100

        return df_fees

    def add_blockchain_transactions(self) -> pd.DataFrame:
        addr_list = self.withdrawals["Address"].values.tolist()
        addr_list = list(set([addr.lower() for addr in addr_list if addr]))
        if not addr_list:
            return
        wallets = self.blockchain_explorer.search_blockchain(addr_list)
        df_tx, df_w = self.blockchain_explorer.get_transactions_and_withdrawals_for_address_list(wallets, COLUMNS, COLUMNS_W)
        df_tx = self._sanitize_df(df_tx, COLUMNS, DTYPES)
        df_tx = self._convert_transactions(df_tx)
        self._extend_transactions_dataframe(df_tx)
        df_w.drop_duplicates(subset=["TxHash", "Fee currency"], inplace=True)
        df_w = self._sanitize_df(df_w, COLUMNS_W, DTYPES_W)
        self._extend_withdrawals_dataframe(df_w)
