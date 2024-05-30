import os
import argparse
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
        fig_path: str = "figures"
    ) -> None:
        self.transactions_handler = TransactionsHandler(cache_root, history_root)
        self.cmc_api_interface = CMCApiInterface(cache_root, api_key_root)
        self.fig_path = fig_path
        if not os.path.exists(self.fig_path):
            os.makedirs(self.fig_path)

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

        df.sort_values(["Datetime", "Side"], inplace=True)
        # Total Size is the size of the asset inside the portfolio for a given datetime,
        # i.e. the cumultative sum of the sizes of the orders
        df["Price"] = -df["Funds"] / df["Size"]
        df["Total Size"] = df.groupby(["Symbol Buy"])["Size"].cumsum()
        # Remove dummy rows
        df = df[df["Broker"] != "Dummy"]
        df.set_index(["Symbol Buy", "Side", "Datetime"], inplace=True)
        df = df.sort_index()

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
                past = df_sym_buy["Datetime"] <= dt
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

        # # Add crypto fees as dummy transactions to account for total size inside portfolio
        # fees = self.transactions_handler.withdrawals.drop(columns=["TxHash", "Address", "Chain", "Coin"])
        # fees = fees[fees["Fee"] > 0]
        # fees["Side"] = "sell"
        # fees["Size"] = -fees["Fee"]
        # fees["Symbol Buy"] = fees["Fee currency"]
        # fees["Symbol Sell"] = "USD"
        # fees["Funds"] = 0.0
        # fees["Fee"] = 0.0
        # fees["Fee currency"] = "USD"
        # fees["Broker"] = "Dummy"
        # fees = fees[df.columns]
        # df = pd.concat((df, fees))

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
                current_price = self.cmc_api_interface.get_price_for_symbol(symbol)
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
        self.plot_portfolio_pie(pf_df.copy())
        self.plot_portfolio_size(pf_df.copy())
        self.plot_bar_x(pf_df.copy(), "x current")

        # Realized profits
        pl_df = profit_df[abs(profit_df["Funds paid"]) > 0]
        self.plot_bar_x(pl_df.copy(), "x realized")

        # Fees per Broker
        fees_df = self.transactions_handler.get_fees_per_broker()
        fees_df.sort_values("% Fee/Funds", inplace=True)
        plt.figure()
        fees_df["% Fee/Funds"].plot(kind="bar")
        plt.title('Fees per Broker in %', fontsize=15)
        plt.xlabel('Broker', fontsize=14)
        plt.ylabel('Fee [%]', fontsize=14)
        plt.xticks(rotation=45)
        plt.subplots_adjust(bottom=0.25)
        plt.savefig(os.path.join(self.fig_path, "fees.png"))
        plt.show()

    def print_key_info(self, profit_df: pd.DataFrame) -> None:
        print(f"Total invested money: {abs(profit_df['Left Funds'].sum()):,.0f}$")
        print(f"Total portfolio value: {profit_df['Current Value'].sum():,.0f}$")
        print(f"Sum of realized profits: {profit_df[profit_df['Profit/Loss'] > 0]['Profit/Loss'].sum():,.0f}$")
        print(f"Sum of realized losses: {profit_df[profit_df['Profit/Loss'] < 0]['Profit/Loss'].sum():,.0f}$")
        print(f"Sum of realized profits and losses: {profit_df['Profit/Loss'].sum():,.0f}$, to be taxed: {max(0, profit_df['To be taxed'].sum()):,.0f}$")
        print(f"Total portfolio profit/loss: {(profit_df['Left Funds'].sum() + profit_df['Current Value'].sum()):,.0f}$")
        print(f"Total x: {profit_df['Current Value'].sum() / abs(profit_df['Left Funds'].sum()):.1f}x")

    def plot_portfolio_pie(self, pf_df: pd.DataFrame) -> None:
        """
        Plots pie chart for the current value of the assets in the portfolio.
        Small positions in a large portfolio are plotted in a separate pie
        """
        if len(pf_df) > 10:
            self.plot_two_pies(pf_df)
        else:
            self.plot_one_pie(pf_df)

    def plot_one_pie(self, pf_df: pd.DataFrame) -> None:
        plt.figure(figsize=(6, 5))
        plt.pie(
            pf_df["Current Value"],
            labels=pf_df["Symbol Buy"],
            autopct=lambda pct: f"{(((pct/100)*pf_df['Current Value'].sum())/1000):.1f}k",
            startangle=90,
            pctdistance=0.8
        )
        plt.title("Portfolio Value Distribution (in USD)")
        plt.savefig(os.path.join(self.fig_path, "pie.png"))

    def plot_two_pies(self, pf_df: pd.DataFrame) -> None:
        pf_df["Sum Value"] = pf_df["Current Value"].cumsum()
        total_value = pf_df["Current Value"].sum()
        pf_df["Ratio value"] = pf_df["Sum Value"] / total_value
        large_df = pf_df[pf_df["Ratio value"] >= 1/6]
        other_df = pf_df[pf_df["Ratio value"] < 1/6]
        other_row = other_df.sum()
        other_row["Symbol Buy"] = "Other"
        large_df = pd.concat((other_row.to_frame().T, large_df))

        fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(10, 5))
        axes[0].pie(
            large_df["Current Value"],
            labels=large_df["Symbol Buy"],
            autopct=lambda pct: f"{(((pct/100)*large_df['Current Value'].sum())/1000):.1f}k",
            startangle=90,
            pctdistance=0.8
        )
        axes[0].set_title("Portfolio")
        axes[1].pie(
            other_df["Current Value"],
            labels=other_df["Symbol Buy"],
            autopct=lambda pct: f"{(((pct/100)*other_df['Current Value'].sum())/1000):.1f}k",
            startangle=90,
            pctdistance=0.8
        )
        axes[1].set_title("Other")
        fig.suptitle("Portfolio Value Distribution (in USD)")
        plt.savefig(os.path.join(self.fig_path, "pie.png"))

    def plot_portfolio_size(self, pf_df: pd.DataFrame) -> None:
        # Biggest 5 positions with size held
        def custom_format(val):
            if isinstance(val, str):  # If the value is a string, return as is
                return val
            elif abs(val) < 10:  # For very small values
                return f"{val:,.4f}"
            else:  # For other values
                return f"{val:,.0f}"
        h = 4
        w = 8
        fig, ax = plt.subplots(figsize=(w, h))
        ax.axis('off')
        pf_df[["Size", "Current Value"]] = pf_df[["Size", "Current Value"]].map(custom_format)
        df_formatted = pf_df.iloc[::-1][:5]
        df_formatted = df_formatted[["Symbol Buy", "Size", "Current Value", "% current"]]
        df_formatted["% current"] = df_formatted["% current"].apply(
            lambda x: f"+ {x:.2f}%" if x > 0 else f"- {abs(x):.2f}%"
        )
        df_formatted["Current Value"] = df_formatted["Current Value"] + " $"
        table = ax.table(
            cellText=df_formatted.values,
            colLabels=df_formatted.columns,
            cellLoc='right',
            loc='center'
        )
        for key, cell in table.get_celld().items():
            cell.set_height(0.1)
            if key[0] == 0:
                cell.set_text_props(fontweight='bold')
        plt.savefig(os.path.join(self.fig_path, "table.png"))

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
        plt.savefig(os.path.join(self.fig_path, f"{col.replace(' ', '_')}.png"))


def personal_portfolio(pf: Portfolio, path_tx: str, path_w: str) -> None:
    pf = Portfolio()
    for filename in glob.iglob(path_tx + "/**/*.csv", recursive=True):
        pf.add_transactions_from_csv(file_path=filename)

    txs = [
        {
            "Datetime": "2021-02-25 20:06:29",
            "Pair": "BTC-EUR",
            "Side": "buy",
            "Size": 0.02380119,
            "Funds": 1000,
            "Fee": 0,
            "Fee currency": "EUR",
            "Broker": "Coinbase Pro",
        },
        {
            "Datetime": "2021-02-25 21:22:55",
            "Pair": "ETH-EUR",
            "Side": "buy",
            "Size": 0.76384081,
            "Funds": 1000,
            "Fee": 0,
            "Fee currency": "EUR",
            "Broker": "Coinbase Pro",
        },
        {
            "Datetime": "2024-04-10 13:53:05",
            "Pair": "USDT-EUR",
            "Side": "buy",
            "Size": 2170,
            "Funds": -1999.872000,
            "Fee": 2.17,
            "Fee currency": "USDT",
            "Broker": "Bitget",
        }
    ]
    trades = [
        {
            "Datetime": "2024-04-10 13:59:47",
            "Pair": "OP-USDT",
            "Side": "buy",
            "Size": 354.9,
            "Funds": -354.9 * (3.0126+3.0115)/2,
            "Fee": 0.6414707,
            "Fee currency": "USDT",
            "Broker": "Bitget",
        },
        {
            "Datetime": "2024-04-10 14:03:15",
            "Pair": "XAI-USDT",
            "Side": "buy",
            "Size": 1062,
            "Funds": -1062 * 1.0229,
            "Fee": 0.6517548,
            "Fee currency": "USDT",
            "Broker": "Bitget",
        },
        {
            "Datetime": "2024-04-22 02:35:47",
            "Pair": "OP-USDT",
            "Side": "sell",
            "Size": -354.9,
            "Funds": 354.9 * 2.44,
            "Fee": 0.229116,
            "Fee currency": "USDT",
            "Broker": "Bitget",
        },
        {
            "Datetime": "2024-04-22 02:36:04",
            "Pair": "XAI-USDT",
            "Side": "sell",
            "Size": -1062,
            "Funds": 1062 * 0.76,
            "Fee": 0.48430692,
            "Fee currency": "USDT",
            "Broker": "Bitget",
        },
    ]
    pf.add_transactions_manually(txs)
    pf.add_transactions_manually(trades)

    # Swaps
    swaps = [
        {
            "Datetime": "2023-12-03 16:58:00",
            "Pair": "WECO-BNB",
            "Side": "buy",
            "Size": 6486.648248643379300367,
            "Funds": 0.008,
            "Fee": 0.000412734,
            "Fee currency": "BNB",
            "Broker": "PancakeSwap",
        },
        {
            "Datetime": "2023-12-03 21:05:00",
            "Pair": "WECO-BNB",
            "Side": "buy",
            "Size": 4310655.395332922452549544,
            "Funds": 4.771801126,
            "Fee": 0.000361506,
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
        {
            "Datetime": "2024-03-16 10:53:59",
            "Pair": "WETH-USDT",
            "Side": "buy",
            "Size": 0.354733899587312852,
            "Funds": -1321.763966,
            "Fee": 0.006329632144952564,
            "Fee currency": "ETH",
            "Broker": "Uniswap",
        },
        {
            "Datetime": "2024-03-16 10:56:35",
            "Pair": "SOUTH-WETH",
            "Side": "buy",
            "Size": 77.458346295975289318,
            "Funds": -0.216,
            "Fee": 0.004939344011454846,
            "Fee currency": "ETH",
            "Broker": "Uniswap",
        },
        {
            "Datetime": "2024-03-16 10:57:59",
            "Pair": "TBANK-WETH",
            "Side": "buy",
            "Size": 430.867432058272424982,
            "Funds": -0.135,
            "Fee": 0.003334170916421401,
            "Fee currency": "ETH",
            "Broker": "Uniswap",
        },
    ]
    pf.add_transactions_manually(swaps)

    for filename in glob.iglob(path_w + "/**/*.csv", recursive=True):
        pf.add_withdrawals_from_csv(file_path=filename)
        
    pf.show_portfolio()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crypto Portfolio")
    parser.add_argument(
        "--path_tx",
        type=str,
        default="exports/transactions",
        help="Path to parent directory of transaction exports of brokers"
    )
    parser.add_argument(
        "--path_w",
        type=str,
        default="exports/deposits_withdrawals",
        help="Path to parent directory of withdrawal exports of brokers"
    )
    parser.add_argument(
        "--cache_root",
        type=str,
        default="cache",
        help="Path to cache dir"
    )
    parser.add_argument(
        "--history_root",
        type=str,
        default="historical_data",
        help="Path to dir with historical data"
    )
    parser.add_argument(
        "--api_key_root",
        type=str,
        default="api_keys",
        help="Path to parent directory api keys"
    )
    parser.add_argument(
        "--fig_path",
        type=str,
        default="figures",
        help="Path to save dir of figures"
    )
    parser.add_argument(
        "--demo",
        action=argparse.BooleanOptionalAction, 
        help="Use demo transactions"
    )
    args = parser.parse_args()

    if args.demo:
        fig_path = "demo"
    else:
        dt = datetime.strftime(datetime.now(), "%Y_%m_%d")
        fig_path = os.path.join(args.fig_path, dt)

    pf = Portfolio(
        args.cache_root,
        args.history_root,
        args.api_key_root,
        fig_path
    )

    if args.demo:
        df = pd.read_csv("demo/demo_txs.csv", delimiter=",").drop(columns="Unnamed: 0")
        pf.transactions_handler.transactions = df
        pf.show_portfolio()
    else:
        personal_portfolio(pf, args.path_tx, args.path_w)
