import os
import pandas as pd
from typing import List, Callable, Optional, Dict, Tuple

from .conversion_handler import ConversionHandler
from .broker_interfaces import BisonInterface, BitvavoInterface, KuCoinInterface, MEXCInterface, BitgetInterface


COLUMNS = ["Datetime", "Pair", "Side", "Size", "Funds", "Fee", "Broker"]
DTYPES = ["object", "object", "object", "float", "float", "float", "object"]


class TransactionsHandler:
    """
    Loads transactions and converts all numbers in USD.
    """
    def __init__(self, cache_root: str, history_root: str) -> None:
        self.transactions = pd.DataFrame(columns=COLUMNS)
        for c, d in zip(COLUMNS, DTYPES):
            self.transactions[c] = self.transactions[c].astype(d)
        self.deposit_withdrawals = pd.DataFrame()
        self.conversion_handler = ConversionHandler(cache_root)
        self.history_root = history_root

    def _extend_transactions_dataframe(self, df: pd.DataFrame):
        """
        Concats df to self.transactions
        """
        if self.transactions.empty:
            self.transactions = df
        else:
            self.transactions = pd.concat((self.transactions, df))
        self.transactions.sort_values(by=["Datetime"] , inplace=True)
        self.transactions.drop_duplicates(subset=["Datetime", "Pair", "Side"], inplace=True)
        self.transactions.reset_index(drop=True, inplace=True)

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
        df = self._sanitize_df(df)
        df = self._convert_transactions(df)
        self._extend_transactions_dataframe(df)

    def add_transactions_from_csv(self, file_path: str) -> None:
        if "mexc" in file_path:
            broker = MEXCInterface(COLUMNS)
        elif "kucoin" in file_path:
            broker = KuCoinInterface(COLUMNS)
        elif "bitvavo" in file_path:
            broker = BitvavoInterface(COLUMNS)
        elif "bison" in file_path:
            broker = BisonInterface(COLUMNS)
        elif "bitget" in file_path:
            broker = BitgetInterface(COLUMNS)
        else:
            raise NotImplementedError(f"Broker not recognised: {file_path}")
    
        df = broker.get_transactions(file_path)
        df = self._sanitize_df(df)
        df = self._convert_transactions(df)
        self._extend_transactions_dataframe(df)

    def _convert_transactions(self, df: pd.DataFrame) -> pd.DataFrame:
        if not df["Pair"].str.contains("EUR").any():
            return df
        # Get conversion data to convert from EUR to USD
        print("Loading conversion rates...")
        from_curr = "EUR"
        to_curr = "USD"
        self.conversion_handler.load_conversion_dict(from_curr, to_curr)

        df["Conversion"] = df["Datetime"].apply(
            lambda str_timestamp: self.conversion_handler.get_conversion_rate(
                from_curr,
                to_curr,
                str_timestamp[:10]
            )
        )
        df.loc[df["Pair"].str.contains("EUR"), "Funds"] *= df["Conversion"]
        df.loc[df["Pair"].str.contains("EUR"), "Fee"] *= df["Conversion"]
        df["Pair"] = df["Pair"].str.replace("EUR", "USD")
        df.drop(columns="Conversion", inplace=True)

        self.conversion_handler.save_conversion_dict(from_curr, to_curr)

        return df

    def _sanitize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[COLUMNS]
        for c in COLUMNS:
            df[c] = df[c].astype(self.transactions[c].dtype)
        return df

    def add_deposits_withdrawals_from_csv(self, file_path: str) -> None:
        if "mexc" in file_path:
            df = self._get_deposits_withdrawals_mexc(file_path)
        elif "kucoin" in file_path:
            df = self._get_deposits_withdrawals_kucoin(file_path)
        elif "bitvavo" in file_path:
            df = self._get_deposits_withdrawals_bitvavo(file_path)
        elif "bison" in file_path:
            df = self._get_deposits_withdrawals_bison(file_path)
        elif "bitget" in file_path:
            df = self._get_deposits_withdrawals_bitget(file_path)
        else:
            raise NotImplementedError(f"Broker not recognised: {file_path}")

        self._extend_transactions_dataframe(df)

    def _get_historical_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Gets historical data for all sell symbols which are not USD and puts them in
        a new df. The rows which are not USD are also returned."""
        # Get transations where sell symbol is not USD
        df_swaps_org = self.transactions[self.transactions['Pair'].apply(lambda x: x.split('-')[1]) != "USD"].copy()

        # Get historical data for those symbols
        df_hist = pd.DataFrame()
        for sym in df_swaps_org["Pair"].apply(lambda x: x.split('-')[1]).unique():
            df = pd.read_csv(os.path.join(self.history_root, sym + ".csv"), delimiter=";")
            df["sym"] = sym
            if not df_hist.empty:
                df_hist = pd.concat((df_hist, df))
            else:
                df_hist = df
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
        df_swaps_org, df_hist = self._get_historical_data()

        # Additional transactions
        df_swaps_add = df_swaps_org.copy()
        df_swaps_add["day"] = df_swaps_add["Datetime"].str[:10]
        df_swaps_add["sym"] = df_swaps_add["Pair"].apply(lambda x: x.split('-')[1])

        # Match historical data to rows from the additional transactions and keep index
        df_swaps_add = df_swaps_add.reset_index().merge(df_hist, on=["day", "sym"], how="left").set_index("index")

        # Set values for the new transactions
        df_swaps_add["price USD"] = (df_swaps_add["open"] + df_swaps_add["close"]) / 2
        df_swaps_add["Pair"] = df_swaps_add["Pair"].apply(lambda x: x.split('-')[1] + "-USD")
        df_swaps_add["Side"] = df_swaps_add["Side"].apply(lambda x: "sell" if x == "buy" else "buy")
        df_swaps_add["Size"] = df_swaps_add["Funds"]
        df_swaps_add["Funds"] = -df_swaps_add["price USD"] * df_swaps_add["Funds"]
        df_swaps_add["Fee"] = df_swaps_add["price USD"] * df_swaps_add["Fee"]

        df_swaps_add.drop(columns=["open", "close", "day", "sym", "price USD"], inplace=True)

        # Set values for original transactions
        df_swaps_org["Pair"] = df_swaps_org["Pair"].apply(lambda x: x.split('-')[0] + "-USD")
        df_swaps_org["Funds"] = -df_swaps_add["Funds"]
        df_swaps_org["Fee"] = 0.0

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
        df_no_usd, df_hist = self._get_historical_data()

        # Prepare merge
        df_no_usd["day"] = df_no_usd["Datetime"].str[:10]
        df_no_usd["sym"] = df_no_usd["Pair"].apply(lambda x: x.split('-')[1])

        # Match historical data to rows from the additional transactions and keep index
        df_no_usd = df_no_usd.reset_index().merge(df_hist, on=["day", "sym"], how="left").set_index("index")

        # Convert to usd
        df_no_usd["price USD"] = (df_no_usd["open"] + df_no_usd["close"]) / 2
        df_no_usd["Funds"] *= df_no_usd["price USD"]
        df_no_usd["Fee"] *= df_no_usd["price USD"]
        
        # Put in full df
        df = self.transactions.copy()
        df.loc[df_no_usd.index] = df_no_usd

        # Get fees per broker
        df_fees = df.groupby("Broker").agg({"Funds": "sum", "Fee": "sum"})
        df_fees["% Fee/Funds"] = (df_fees["Fee"] / df_fees["Funds"]).abs() * 100

        return df_fees
