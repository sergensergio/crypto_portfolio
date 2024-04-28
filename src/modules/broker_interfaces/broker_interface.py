import pandas as pd

from typing import List


class BrokerInterface:

    def __init__(self, columns: List[str]):
        """
        Args:
            columns: column names in the transactions df
        Properties set in subclasses:
            index_columns: column names used for the grouping of the data frame.
                Values should correspond to ["Datetime", "Pair", "Side"] in this order
            agg_columns: column names used to aggregate values for the grouping
                of the data frame. Values should correspond to ["Size", "Funds", "Fee"] in
                this order
        """
        self.columns = columns

    def get_transactions(self, file_name: str):
        pass

    def _get_transactions(self, file_name: str, delimiter: str = ",") -> pd.DataFrame:
        """
        Args:
            file_name: path to transaction csv
            delimiter: delimiter used to seperate values in csv
        Return:
            df: dataframe with transactions
        """
        # Read
        df = pd.read_csv(file_name, delimiter=delimiter)
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
        columns_reordered = self.index_columns + self.agg_columns + ["Broker"]
        df = df[columns_reordered]
        df.columns = self.columns

        return df

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        return df