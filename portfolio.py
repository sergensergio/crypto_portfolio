import glob
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm

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
        print(f"Total invested money (USD): {total_spent}")
        print(f"Total portfolio value (USD): {pf['Current Value'].sum()}")
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
        
    pf.show_portfolio()