import glob
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm
from typing import Dict

from transactions_handler import TransactionsHandler
from cmc_api_interface import CMCApiInterface

class Portfolio:
    def __init__(
        self,
        cache_root: str = "cache",
        api_key_path: str = "api_key.txt",
    ) -> None:
        self.transactions_handler = TransactionsHandler(cache_root)
        self.cmc_api_interface = CMCApiInterface(cache_root, api_key_path)

    def add_transactions_from_csv(self, file_path: str) -> None:
        self.transactions_handler.add_transactions_from_csv(file_path)

    def add_transaction_manually(self, transaction_dict: Dict):
        """
        Add a single transaction manually. Provide a dict
        with the following fields (example):
        t_btc = {
            "Datetime": "2021-02-25 20:06:29",
            "Pair": "BTC-EUR",
            "Side": "buy",
            "Size": 0.02380119,
            "Funds": 1000,
            "Fee": 0,
        }
        Format of the values should be according to the example.
        """
        self.transactions_handler.add_transaction_manually(transaction_dict)

    def show_portfolio(self):
        # Get transactions and split pair column
        df = self.transactions_handler.transactions.copy()
        index_columns = ["Symbol Buy", "Symbol Sell"]
        df[index_columns] = df["Pair"].str.split("-", expand=True)

        # Sum up all buy and sell sizes
        buy = df.groupby("Symbol Buy").agg({
            "Size": "sum"
        }).reset_index()
        sell = df.groupby("Symbol Sell").agg({
            "Funds": "sum"
        }).reset_index()
        buy.columns = ["Asset", "Size"]
        sell.columns = ["Asset", "Size"]
        pf = pd.concat((buy, sell)).groupby("Asset").sum()

        # Get total spent value and drop row
        total_spent = pf.loc["USD"]["Size"]
        pf.drop("USD", inplace=True)

        # Set current market prices
        pf["Current Price"] = 0.0
        for symbol in tqdm(
            pf.index,
            desc="Get current prices",
            total=len(pf.index)
        ):
            if symbol == "CHNG":
                current_price = 0.098
            else:
                data = self.cmc_api_interface.get_data_for_symbol(symbol)
                current_price = data["data"][symbol]["quote"]["USD"]["price"]
            pf.loc[symbol, "Current Price"] = current_price

        # Set current value of assets
        pf["Current Value"] = pf["Current Price"] * pf["Size"]
        pf = pf[pf["Current Value"] > 0.1]

        pf.reset_index(inplace=True)
        pf.sort_values("Current Value", inplace=True)

        # Show and plot information
        print(f"Total invested money (USD): {abs(total_spent)}")
        print(f"Total portfolio value (USD): {pf['Current Value'].sum()}")
        print(f"Total portfolio profit/loss (USD): {total_spent + pf['Current Value'].sum()}")
        plt.figure(figsize=(10, 7))
        plt.pie(
            pf['Current Value'],
            labels=pf['Asset'],
            autopct=lambda pct: "{:d}".format(int(pct*pf["Current Value"].sum()/100)),
            startangle=90,
            pctdistance=0.8
        )
        plt.title('Portfolio Value Distribution')
        plt.show()


if __name__ == "__main__":
    path_csvs = "exports"

    pf = Portfolio()
    for filename in glob.iglob(path_csvs + "/**/*.csv", recursive=True):
        pf.add_transactions_from_csv(file_path=filename)
    
    t_btc = {
        "Datetime": "2021-02-25 20:06:29",
        "Pair": "BTC-EUR",
        "Side": "buy",
        "Size": 0.02380119,
        "Funds": 1000,
        "Fee": 0,
    }
    t_eth = {
        "Datetime": "2021-02-25 21:22:55",
        "Pair": "ETH-EUR",
        "Side": "buy",
        "Size": 0.76384081,
        "Funds": 1000,
        "Fee": 0,
    }
    pf.add_transaction_manually(t_btc)
    pf.add_transaction_manually(t_eth)
    # Swaps
    s_bnb_weco = {
        "Datetime": "2023-12-03 16:58:00",
        "Pair": "WECO-BNB",
        "Side": "buy",
        "Size": 6486.648,
        "Funds": 0.008,
        "Fee": 0.001,
    }
    s_bnb_weco_2 = {
        "Datetime": "2023-12-03 21:05:00",
        "Pair": "WECO-BNB",
        "Side": "buy",
        "Size": 4310655.395,
        "Funds": 4.772,
        "Fee": 0.001,
    }
    pf.add_transaction_manually(s_bnb_weco)
    pf.add_transaction_manually(s_bnb_weco_2)
        
    pf.show_portfolio()