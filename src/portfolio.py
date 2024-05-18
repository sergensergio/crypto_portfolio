import glob
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import colormaps
from tqdm import tqdm
from typing import Dict, List
from datetime import datetime

from modules.transactions_handler import TransactionsHandler
from modules.cmc_api_interface import CMCApiInterface

class Portfolio:
    def __init__(
        self,
        cache_root: str = "cache",
        history_root: str = "historical_data",
        api_key_root: str = "api_keys",
    ) -> None:
        self.transactions_handler = TransactionsHandler(cache_root, history_root)
        self.cmc_api_interface = CMCApiInterface(cache_root, api_key_root)

    def add_transactions_from_csv(self, file_path: str) -> None:
        self.transactions_handler.add_transactions_from_csv(file_path)

    def add_withdrawals_from_csv(self, file_path: str) -> None:
        self.transactions_handler.add_withdrawals_from_csv(file_path)

    def add_transactions_manually(self, transaction_list: List[Dict]):
        """
        Add a single or multiple transactions manually. Provide a dict or
        list of dicts with the following fields (example):
        t_btc = {
            "Datetime": "2021-02-25 20:06:29",
            "Pair": "BTC-EUR",
            "Side": "buy",
            "Size": 0.02380119,
            "Funds": 1000,
            "Fee": 0,
            "Fee currency": "EUR",
            "Broker": "Bison",
        }
        Format of the values should be according to the example.
        """
        if isinstance(transaction_list, dict):
            transaction_list = [transaction_list]
        self.transactions_handler.add_transactions_manually(transaction_list)

    def get_realized_profits(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates realized profits/losses, i.e. funds of all sell orders are compared
        to funds of corresponding buy orders. Returns funds paid and received and
        profits/losses for each coin for each year.
        """

        df.sort_values("Datetime")
        # Total Size is the size of the asset inside the portfolio for a given datetime,
        # i.e. the cumultative sum of the sizes of the orders
        df["Price"] = -df["Funds"] / df["Size"]
        df["Total Size"] = df.groupby(["Symbol Buy"])["Size"].cumsum()
        df.set_index(["Symbol Buy", "Side", "Datetime"], inplace=True)
        df.sort_index(inplace=True)

        realized_profits = []

        for sym in df.index.levels[0]:
            try:
                df_sym_sell = df.loc[sym, "sell"].copy()
            except:
                print(f"No sell orders yet for {sym}...")
                continue
            if any(["USD" not in elem for elem in df.loc[sym]["Symbol Sell"].unique()]):
                print(f"Warning: Different sell symbol than USD or USDT for {sym}")

            df_sym_buy = df.loc[sym, "buy"].copy()
            df_sym_buy.reset_index(inplace=True)
            df_sym_buy["Sold Size"] = 0.0
            df_sym_buy["Sold Value"] = 0.0
            df_sym_buy["Profits"] = 0.0
            df_sym_buy["Held days"] = 0
            df_sym_buy["To be taxed"] = False
            for dt, row in df_sym_sell.iterrows():
                # If coins are sold, substract sell amount in FIFO manner from all
                # the buy orders until sell amount is reached and compare funds paid
                # and received to get realised profit/loss
                
                # Get only past buy orders
                past = df_sym_buy["Datetime"] < dt
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
                # Get amount which needs to be taxed (token held less than 1 year according to German tax law)
                df_sym_buy.loc[df_sym_buy.index <= first_dt, "Sold Size"] = \
                    df_sym_buy.loc[df_sym_buy.index <= first_dt, "Size"] - \
                    df_sym_buy.loc[df_sym_buy.index <= first_dt, "Total Size"]
                df_sym_buy.loc[df_sym_buy.index <= first_dt, "Size"] -= \
                    df_sym_buy.loc[df_sym_buy.index <= first_dt, "Sold Size"]
                df_sym_buy.loc[df_sym_buy.index <= first_dt, "Sold Value"] = \
                    df_sym_buy.loc[df_sym_buy.index <= first_dt, "Sold Size"] * \
                    row["Price"]
                df_sym_buy.loc[df_sym_buy.index < first_dt, "Profits"] = \
                    df_sym_buy.loc[df_sym_buy.index < first_dt, "Sold Value"] + \
                    df_sym_buy.loc[df_sym_buy.index < first_dt, "Funds"]
                df_sym_buy.loc[first_dt, "Profits"] = \
                    df_sym_buy.loc[first_dt, "Sold Value"] + \
                    funds_share
                df_sym_buy.loc[df_sym_buy.index <= first_dt, "Held days"] = \
                    (
                        datetime.strptime(dt, "%Y-%m-%d %H:%M:%S") - \
                        df_sym_buy.loc[df_sym_buy.index <= first_dt, "Datetime"].apply(
                            lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S")
                        )
                    ).apply(lambda y: y.days)
                df_sym_buy.loc[df_sym_buy.index <= first_dt, "To be taxed"] = \
                    df_sym_buy.loc[df_sym_buy.index <= first_dt, "Held days"] <= 365
                
                # Include always negative profits to lower taxes
                profits_to_be_taxed = df_sym_buy[df_sym_buy["To be taxed"] | (df_sym_buy["Profits"] < 0)]["Profits"].sum()
                
                # Set past funds to 0 (necessary for subsequent iterations)
                df_sym_buy.loc[df_sym_buy.index < first_dt, "Funds"] = 0
                # Update funds for first_dt
                df_sym_buy.loc[first_dt, "Funds"] -= funds_share
                # Update funds received
                funds_received = row["Funds"]

                assert np.isclose(funds_paid + funds_received, df_sym_buy["Profits"].sum())

                row_new = {
                    "Datetime": dt,
                    "Symbol Buy": sym,
                    "Funds paid": funds_paid,
                    "Funds received": funds_received,
                    "Profit/Loss": funds_paid + funds_received,
                    "To be taxed": profits_to_be_taxed
                }
                realized_profits.append(row_new)

        return pd.DataFrame(realized_profits)

    def show_portfolio(self):
        # Add transactions from blockchain search to transactions
        self.transactions_handler.add_blockchain_transactions()
        # Get transactions and split pair column
        df = self.transactions_handler.get_transactions_based_on_usd()
        index_columns = ["Symbol Buy", "Symbol Sell"]
        df[index_columns] = df["Pair"].str.split("-", expand=True)
        df.drop(columns="Pair", inplace=True)

        # Create profit dataframe
        # All buy order funds
        profit_df = df[df["Side"] == "buy"].groupby("Symbol Buy").agg({"Funds": "sum"})
        profit_df[["Size", "Fee"]] = df.groupby("Symbol Buy").sum()[["Size", "Fee"]]

        # Get realized profits
        realized_profits = self.get_realized_profits(df.copy())

        # Put total profits (sum over all datetimes) in profit dataframe
        total_profits = realized_profits.groupby("Symbol Buy").sum().drop(columns="Datetime")
        profit_df[total_profits.columns] = total_profits
        profit_df.fillna(0.0, inplace=True)

        # Left funds are the funds left invested in the asset after profit taking
        # I.e. [all buy order funds] - [funds paid for realized profits]
        profit_df["Left Funds"] = profit_df["Funds"] - profit_df["Funds paid"]

        # Set current market prices
        profit_df["Current Price"] = 0.0
        for symbol in tqdm(
            profit_df.index,
            desc="Get current prices",
            total=len(profit_df.index)
        ):
            if symbol == "CHNG":
                symbol = "XCHNG"
            if symbol == "wCADAI":
                current_price = 1
            else:
                data = self.cmc_api_interface.get_data_for_symbol(symbol)
                current_price = data["data"][symbol]["quote"]["USD"]["price"]
            if symbol == "XCHNG":
                symbol = "CHNG"
            profit_df.loc[symbol, "Current Price"] = current_price

        profit_df["Current Value"] = profit_df["Current Price"] * profit_df["Size"]
        profit_df["Fully Sold"] = profit_df["Current Value"] < 0.1

        # Calculate gains
        profit_df.loc[abs(profit_df["Funds paid"]) > 0, "x realized"] = abs(profit_df["Funds received"] / profit_df["Funds paid"])
        profit_df["% realized"] = (profit_df["x realized"] - 1) * 100
        profit_df.loc[abs(profit_df["Left Funds"]) > 0, "x current"] = abs(profit_df["Current Value"] / profit_df["Left Funds"])
        profit_df["% current"] = (profit_df["x current"] - 1) * 100

        # Reorder columns
        profit_df = profit_df[[
            "Funds", "Left Funds", "Funds paid", "Funds received", "Profit/Loss",
            "To be taxed", "x realized", "% realized", "Size", "Current Price",
            "Current Value", "Fully Sold", "x current", "% current", "Fee", 
        ]]

        profit_df.reset_index(inplace=True)

        # Key info
        self.print_key_info(profit_df)

        # Current portfolio
        pf_df = profit_df.sort_values("Current Value")
        pf_df = pf_df[~pf_df["Fully Sold"]]
        pf_df = pf_df[pf_df["Current Value"] > 50]
        self.plot_portfolio_pie(pf_df)
        self.plot_bar_x(pf_df, "x current")

        # Realized profits
        pl_df = profit_df[abs(profit_df["Funds paid"]) > 0]
        self.plot_bar_x(pl_df, "x realized")

        # Fees per Broker
        fees_df = self.transactions_handler.get_fees_per_broker()
        fees_df.sort_values("% Fee/Funds", inplace=True)
        fees_df["% Fee/Funds"].plot(kind="bar")
        plt.title('Fees per Broker in %', fontsize=15)
        plt.xlabel('Broker', fontsize=14)
        plt.ylabel('Fee [%]', fontsize=14)
        plt.xticks(rotation=45)
        plt.subplots_adjust(bottom=0.25)
        plt.show()

    def print_key_info(self, profit_df: pd.DataFrame) -> None:
        print(f"Total invested money: {abs(profit_df['Left Funds'].sum()):,.0f}$")
        print(f"Total portfolio value: {profit_df['Current Value'].sum():,.0f}$")
        print(f"Sum of realized profits and losses: {profit_df['Profit/Loss'].sum():,.0f}$, to be taxed: {profit_df['To be taxed'].sum():,.0f}$")
        print(f"Total portfolio profit/loss: {(profit_df['Left Funds'].sum() + profit_df['Current Value'].sum()):,.0f}$")
        print(f"Total x: {profit_df['Current Value'].sum() / abs(profit_df['Left Funds'].sum()):.1f}x")

    def plot_portfolio_pie(self, pf_df: pd.DataFrame) -> None:
        """
        Plots pie chart for the current value of the assets in the portfolio.
        Small positions are plotted in a separate pie
        """
        
        pf_df["Sum Value"] = pf_df["Current Value"].cumsum()
        total_value = pf_df["Current Value"].sum()
        pf_df["Ratio value"] = pf_df["Sum Value"] / total_value
        large_df = pf_df[pf_df["Ratio value"] >= 1/8]
        other_df = pf_df[pf_df["Ratio value"] < 1/8]
        other_row = other_df.sum()
        other_row["Symbol Buy"] = "Other"
        large_df = pd.concat((other_row.to_frame().T, large_df))

        fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(10, 5))
        axes[0].pie(
            large_df["Current Value"],
            labels=large_df["Symbol Buy"],
            autopct=lambda pct: "{:d}".format(int(pct*large_df["Current Value"].sum()/100)),
            startangle=90,
            pctdistance=0.8
        )
        axes[0].set_title("Portfolio")
        axes[1].pie(
            other_df["Current Value"],
            labels=other_df["Symbol Buy"],
            autopct=lambda pct: "{:d}".format(int(pct*other_df["Current Value"].sum()/100)),
            startangle=90,
            pctdistance=0.8
        )
        axes[1].set_title("Other")
        fig.suptitle("Portfolio Value Distribution (in USD)")

    def plot_bar_x(self, pf_df: pd.DataFrame, col: str) -> None:
        # Plot current x values
        df_bar = pf_df.set_index("Symbol Buy")[col].sort_values()
        cmap = colormaps["RdYlGn"]
        norm_profit = plt.Normalize(vmin=-8, vmax=10, clip=True)
        norm_loss = plt.Normalize(vmin=0, vmax=2, clip=True)
        bar_colors = [cmap(norm_profit(value)) if value > 1 else cmap(norm_loss(value)) for value in df_bar]
        plt.figure()
        bar_plot = df_bar.plot.barh(color=bar_colors)
        for b in bar_plot.patches:
            bar_plot.annotate(
                f"{b.get_width():.1f}x",
                (b.get_width(), b.get_y() + b.get_height() / 2),
                ha="left",
                va="center",
                xytext=(5, 0),
                textcoords="offset points"
            )
        plt.axvline(x=1, color="r", linestyle="--", label="1x")
        plt.xlim(0, df_bar.max() + 1.5)
        plt.title((" ".join(col.split(" ")[::-1])).capitalize() + " on assets")
        plt.show()

if __name__ == "__main__":
    path_txs = "exports/transactions"
    pf = Portfolio()
    for filename in glob.iglob(path_txs + "/**/*.csv", recursive=True):
        pf.add_transactions_from_csv(file_path=filename)
    
    t_btc = {
        "Datetime": "2021-02-25 20:06:29",
        "Pair": "BTC-EUR",
        "Side": "buy",
        "Size": 0.02380119,
        "Funds": 1000,
        "Fee": 0,
        "Fee currency": "EUR",
        "Broker": "Coinbase Pro",
    }
    t_eth = {
        "Datetime": "2021-02-25 21:22:55",
        "Pair": "ETH-EUR",
        "Side": "buy",
        "Size": 0.76384081,
        "Funds": 1000,
        "Fee": 0,
        "Fee currency": "EUR",
        "Broker": "Coinbase Pro",
    }
    pf.add_transactions_manually(t_btc)
    pf.add_transactions_manually(t_eth)
    # Swaps
    swaps = [
        {
            "Datetime": "2023-12-03 16:58:00",
            "Pair": "WECO-BNB",
            "Side": "buy",
            "Size": 6486.648,
            "Funds": 0.008,
            "Fee": 0.001,
            "Fee currency": "BNB",
            "Broker": "PancakeSwap",
        },
        {
            "Datetime": "2023-12-03 21:05:00",
            "Pair": "WECO-BNB",
            "Side": "buy",
            "Size": 4310655.395,
            "Funds": 4.772,
            "Fee": 0.001,
            "Fee currency": "BNB",
            "Broker": "PancakeSwap",
        },
        {
            "Datetime": "2024-02-14 21:05:00",
            "Pair": "CHNG-USDT",
            "Side": "sell",
            "Size": 34210.22,
            "Funds": 2800,
            "Fee": 0,
            "Fee currency": "USDT",
            "Broker": "Chainge App",
        },
    ]
    pf.add_transactions_manually(swaps)

    path_deposits = "exports/deposits_withdrawals"
    for filename in glob.iglob(path_deposits + "/**/*.csv", recursive=True):
        pf.add_withdrawals_from_csv(file_path=filename)
        
    pf.show_portfolio()