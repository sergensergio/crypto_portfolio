import glob
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm
from typing import Dict
from collections import defaultdict

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

    def show_portfolio_simple(self):
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

    def show_portfolio_2(self):
        # Get transactions and split pair column
        df = self.transactions_handler.transactions.copy()
        index_columns = ["Symbol Buy", "Symbol Sell"]
        df[index_columns] = df["Pair"].str.split("-", expand=True)
        df.drop(columns="Pair", inplace=True)

        df.sort_values("Datetime")
        # Total Size is the size of the asset inside the portfolio for a given datetime,
        # i.e. the cumultative sum of the sizes of the orders
        # TODO: Distinguish between different sell symbols?
        df["Total Size"] = df.groupby(["Symbol Buy"])["Size"].cumsum()
        df.set_index(["Symbol Buy", "Side", "Datetime"], inplace=True)
        df.sort_index(inplace=True)

        realized_profits = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))

        for sym in df.index.levels[0]:
            try:
                df_sym_sell = df.loc[sym, "sell"].copy()
            except:
                print(f"No sell orders yet for {sym}...")
                continue
            df_sym_buy = df.loc[sym, "buy"].copy()
            for dt, row in df_sym_sell.iterrows():
                # If coins are sold, substract sell amount in FIFO manner from all
                # the buy orders until sell amount is reached and compare funds paid
                # and received to get realised profit/loss
                
                # Get only past buy orders
                past = df_sym_buy.index < dt
                # Get first datetime for which the sell size is covered by the total size
                first_dt = (df_sym_buy["Total Size"] + row["Size"] >= 0).idxmax()
                # Reduce past Total Size by the sell amount
                df_sym_buy.loc[past, "Total Size"] = df_sym_buy.loc[past, "Total Size"].apply(
                    lambda x: max(0, x + row["Size"])
                )
                # Get the ratio of what is covered by the sell order in this buy order
                size_ratio = 1 - (df_sym_buy.loc[first_dt]["Total Size"] / df_sym_buy.loc[first_dt]["Size"])
                # Get the corresponding funds share
                funds_share = size_ratio * df_sym_buy.loc[first_dt]["Funds"]
                # The funds which have been paid  for the size of the sell order
                funds_paid = funds_share + df_sym_buy.loc[df_sym_buy.index < first_dt]["Funds"].sum()
                # Set past funds to 0 (necessary for subsequent iterations)
                df_sym_buy.loc[df_sym_buy.index < first_dt, "Funds"] = 0
                # Update funds for first_dt
                df_sym_buy.loc[first_dt, "Funds"] -= funds_share
                # Update funds received
                funds_received = row["Funds"]

                realized_profits[dt[:4]][sym]["Funds paid"] += funds_paid
                realized_profits[dt[:4]][sym]["Funds received"] += funds_received
                realized_profits[dt[:4]][sym]["Profit/Loss"] += funds_paid + funds_received

        # Sum up all buy and sell sizes
        buy_size = df.groupby("Symbol Buy").agg({
            "Size": "sum"
        }).reset_index()
        buy_funds = df.groupby(["Symbol Buy", "Symbol Sell"]).agg({
            "Funds": "sum"
        }).reset_index()
        sell = df.groupby("Symbol Sell").agg({
            "Funds": "sum"
        }).reset_index()
        buy_size.columns = ["Asset", "Size"]
        sell.columns = ["Asset", "Size"]
        pf = pd.concat((buy_size, sell)).groupby("Asset").sum()

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
        
    pf.show_portfolio_2()