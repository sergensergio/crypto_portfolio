import pandas as pd
from typing import List, Callable, Optional, Dict

from conversion_handler import ConversionHandler


COLUMNS = ["Datetime", "Pair", "Side", "Price", "Size", "Funds", "Fee"]


class TransactionsHandler:
    """
    Loads transactions and converts all numbers in USD.
    """
    def __init__(self, cache_root: str) -> None:
        self.transactions = pd.DataFrame(columns=COLUMNS)
        self.conversion_handler = ConversionHandler(cache_root)

    def _extend_transactions_dataframe(self, df: pd.DataFrame):
        """
        Concats df to self.transactions
        """
        if self.transactions.empty:
            self.transactions = df
        else:
            self.transactions = pd.concat((self.transactions, df))
        self.transactions.sort_values(by=["Datetime"] , inplace=True)
        self.transactions.reset_index(drop=True, inplace=True)

    def _prepare_dataframe_from_dict(self, d: Dict) -> pd.DataFrame: 
        # Check dict keywords
        keys = d.keys()
        assert all([key in COLUMNS for key in keys]), \
        f"Unrecognised keyword in dict. Use {[c for c in COLUMNS if c != 'Price']}"
        # Put values in list
        for k, v in d.items():
            d[k] = [v]
        # Create dataframe and set correct values
        df = pd.DataFrame(d)
        sign_side = 1 if df.iloc[0]["Side"] == "buy" else -1
        df["Size"] = sign_side * abs(df["Size"])
        df["Funds"] = -1 * sign_side * abs(df["Funds"])
        df["Price"] = abs(df["Funds"] / df["Size"])
        return df

    def add_transaction_manually(self, transaction_dict: Dict):

        df = self._prepare_dataframe_from_dict(transaction_dict)

        # Convert to USD
        if df.iloc[0]["Pair"].split("-")[1] == "EUR":
            from_curr = "EUR"
            to_curr = "USD"
            self.conversion_handler.load_conversion_dict(from_curr, to_curr)
            conv = self.conversion_handler.get_conversion_rate(
                from_curr,
                to_curr,
                df.iloc[0]["Datetime"][:10]
            )
            self.conversion_handler.save_conversion_dict(from_curr, to_curr)
            df["Price"] *= conv
            df["Funds"] *= conv
            df["Fee"] *= conv
            df["Pair"] = df["Pair"].str.replace("EUR", "USD")

        df = df[COLUMNS]
        self._extend_transactions_dataframe(df)

    def add_transactions_from_csv(self, file_path: str) -> None:
        if "mexc" in file_path:
            df = self._get_transactions_mexc(file_path)
        elif "kucoin" in file_path:
            df = self._get_transactions_kucoin(file_path)
        elif "bitvavo" in file_path:
            df = self._get_transactions_bitvavo(file_path)
        elif "bison" in file_path:
            df = self._get_transactions_bison(file_path)
        else:
            raise NotImplementedError(f"Broker not recognised: {file_path}")

        self._extend_transactions_dataframe(df)

    def _get_transactions(
        self,
        file_name: str,
        index_columns: List[str],
        agg_columns: List[str],
        delimiter: str = ",",
        preprocess: Optional[Callable] = None
    ) -> pd.DataFrame:
        """
        Args:
            file_name: path to transaction csv
            index_columns: column names used for the grouping of the data frame.
                Values should correspond to ["Datetime", "Pair", "Side"] in this order
            agg_columns: column names used to aggregate values for the grouping
                of the data frame. Values should correspond to ["Size", "Funds", "Fee"] in
                this order
            preprocess: Optional preprocessing function to call right after reading csv file
        Return
        """
        # Read
        df = pd.read_csv(file_name, delimiter=delimiter)
        if preprocess:
            df = preprocess(df)
        df[index_columns[1]] = df[index_columns[1]].str.replace("_", "-")
        df[index_columns[2]] = df[index_columns[2]].str.lower()
        df = df[df[index_columns[2]].isin(["buy", "sell"])]

        # Group
        df.set_index(index_columns, inplace=True)
        agg_dict = {key: "sum" for key in agg_columns}
        df = df.groupby(level=index_columns).agg(agg_dict)

        # Set average buy price
        df["Price"] = (df[agg_columns[1]] / df[agg_columns[0]]).abs()

        # Sort and reset index
        df.sort_index(inplace=True)
        df.reset_index(inplace=True)
        columns_reordered = index_columns + ["Price"] + agg_columns
        df = df[columns_reordered]
        df.columns = self.transactions.columns

        return df

    def _get_transactions_mexc(self, file_name: str) -> pd.DataFrame:
        def preprocess_mexc(df: pd.DataFrame) -> pd.DataFrame:
            df.loc[df["Seite"] == "SELL", "Ausgeführter Betrag"] *= -1
            df.loc[df["Seite"] == "BUY", "Gesamt"] *= -1
            return df
        index_columns = ["Zeit", "Paare", "Seite"]
        agg_columns = ["Ausgeführter Betrag", "Gesamt", "Gebühr"]
        df = self._get_transactions(file_name, index_columns, agg_columns, ";", preprocess_mexc)

        return df

    def _get_transactions_kucoin(self, file_name: str) -> pd.DataFrame:
        def preprocess_kucoin(df: pd.DataFrame) -> pd.DataFrame:
            df.loc[df["side"] == "sell", "size"] *= -1
            df.loc[df["side"] == "buy", "funds"] *= -1
            return df
        index_columns = ["tradeCreatedAt", "symbol", "side"]
        agg_columns = ["size", "funds", "fee"]
        df = self._get_transactions(file_name, index_columns, agg_columns, ",", preprocess_kucoin)

        return df
        
    def _get_transactions_bitvavo(self, file_name: str) -> pd.DataFrame:
        # Get conversion data to convert from EUR to USD
        from_curr = "EUR"
        to_curr = "USD"
        self.conversion_handler.load_conversion_dict(from_curr, to_curr)

        def preprocess_bitvavo(df: pd.DataFrame) -> pd.DataFrame:
            df["Time"] = df["Time"].str[:8]
            df["Datetime"] = df["Date"] + " " + df["Time"]
            df["Currency"] = df["Currency"] + "-EUR"
            return df
        index_columns = ["Datetime", "Currency", "Type"]
        agg_columns = ["Amount", "EUR received / paid", "Fee amount"]
        df = self._get_transactions(file_name, index_columns, agg_columns, ",", preprocess_bitvavo)

        print("Loading conversion rates...")
        df["Conversion"] = df["Datetime"].apply(
            lambda str_timestamp: self.conversion_handler.get_conversion_rate(
                from_curr,
                to_curr,
                str_timestamp[:10]
            )
        )
        df["Price"] *= df["Conversion"]
        df["Funds"] *= df["Conversion"]
        df["Fee"] *= df["Conversion"]
        df["Pair"] = df["Pair"].str.replace("EUR", "USD")
        df.drop(columns="Conversion", inplace=True)

        self.conversion_handler.save_conversion_dict(from_curr, to_curr)

        return df
     
    def _get_transactions_bison(self, file_name: str) -> pd.DataFrame:
        # Get conversion data to convert from EUR to USD
        from_curr = "EUR"
        to_curr = "USD"
        self.conversion_handler.load_conversion_dict(from_curr, to_curr)

        def preprocess_bison(df: pd.DataFrame) -> pd.DataFrame:
            for c in df.columns:
                if df[c].dtype == "object":
                    df[c] = df[c].str.strip()
            df["Pair"] = df[" Asset"].str.upper() + "-" + df[" Currency"].str.upper()
            df[" AssetAmount"] = pd.to_numeric(df[' AssetAmount'], errors='coerce')
            df[" EurAmount"] = pd.to_numeric(df[' EurAmount'], errors='coerce')
            df[" Fee"] = pd.to_numeric(df[' Fee'], errors='coerce')
            # Set sign
            df.loc[df["TransactionType"] == "Sell", " AssetAmount"] *= -1
            df.loc[df["TransactionType"] == "Buy", " EurAmount"] *= -1
            return df
        index_columns = [" Date", "Pair", "TransactionType"]
        agg_columns = [" AssetAmount", " EurAmount", " Fee"]
        df = self._get_transactions(file_name, index_columns, agg_columns, ";", preprocess_bison)

        print("Loading conversion rates...")
        df["Conversion"] = df["Datetime"].apply(
            lambda str_timestamp: self.conversion_handler.get_conversion_rate(
                from_curr,
                to_curr,
                str_timestamp[:10]
            )
        )
        df["Price"] *= df["Conversion"]
        df["Funds"] *= df["Conversion"]
        df["Fee"] *= df["Conversion"]
        df["Pair"] = df["Pair"].str.replace("EUR", "USD")
        df.drop(columns="Conversion", inplace=True)

        self.conversion_handler.save_conversion_dict(from_curr, to_curr)

        return df
