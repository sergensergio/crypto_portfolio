import pandas as pd

from typing import List


class BrokerInterface:

    def __init__(self, columns: List[str] = None):
        """
        Args:
            columns: column names in the transactions df
        Properties set in subclasses:
            broker: name of broker
            index_columns: column names used for the grouping of the data frame.
                Values should correspond to ["Datetime", "Pair", "Side"] in this order
            agg_columns: column names used to aggregate values for the grouping
                of the data frame. Values should correspond to ["Size", "Funds", "Fee"] in
                this order
            delimiter: delimiter used to seperate values in csv
        """
        self.columns = columns

    def get_transactions(self, file_name: str) -> pd.DataFrame:
        """
        Args:
            file_name: path to transaction csv
        Return:
            df: dataframe with transactions
        """
        # Read
        df = pd.read_csv(file_name, delimiter=self.delimiter)
        df = self._preprocess(df)
        df[self.index_columns[1]] = df[self.index_columns[1]].str.replace("_", "-")
        df[self.index_columns[2]] = df[self.index_columns[2]].str.lower()
        df = df[df[self.index_columns[2]].isin(["buy", "sell"])]

        # Group
        df.set_index(self.index_columns, inplace=True)
        agg_dict = {key: "sum" for key in self.agg_columns}
        df = df.groupby(level=self.index_columns).agg(agg_dict)

        # Sort and reset index
        df.sort_index(inplace=True)
        df.reset_index(inplace=True)
        df["Broker"] = self.broker
        df["Fee currency"] = df[self.index_columns[1]].apply(lambda x: x.split("-")[1])
        columns_reordered = self.index_columns + self.agg_columns + ["Fee currency", "Broker"]
        df = df[columns_reordered]
        df.columns = self.columns

        return df

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def get_withdrawals(self, file_path: str, columns: List[str]) -> pd.DataFrame:
        return pd.DataFrame(columns=columns)