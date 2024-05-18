import os
import requests
import json
import pandas as pd

from typing import List, Tuple, Union, Dict
from typing import List, Tuple, Union, Dict
from collections import defaultdict
from tqdm import tqdm
from datetime import datetime
from tqdm import tqdm
from datetime import datetime


BLACKLIST_FILE = "blacklisted_addresses.txt"
WALLETS_FILE = "wallets.txt"

class BlockchainExplorer:
    def __init__(
        self,
        api_key_root: str = "api_keys",
        cache_root: str = "cache"
    ) -> None:
        api_key_path = os.path.join(api_key_root, "etherscan.txt")
        if not os.path.exists(api_key_path):
            raise FileNotFoundError(f"{api_key_path} not found.")
        with open(api_key_path, "r") as file:
            self.api_key = file.read()

        self.cache_path = os.path.join(cache_root, "explorer")
        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)

        self.blacklist = set()
        self.blacklist = set()
        if os.path.exists(os.path.join(self.cache_path, BLACKLIST_FILE)):
            with open(os.path.join(self.cache_path, BLACKLIST_FILE), "r") as file:
                lines = file.readlines()
            self.blacklist = set([line.strip() for line in lines])
            self.blacklist = set([line.strip() for line in lines])

        self.wallets = set()
        self.wallets = set()
        if os.path.exists(os.path.join(self.cache_path, WALLETS_FILE)):
            with open(os.path.join(self.cache_path, WALLETS_FILE), "r") as file:
                lines = file.readlines()
            self.wallets = set([line.strip() for line in lines])
            self.wallets = set([line.strip() for line in lines])

    def search_blockchain(self, addr_list: List[str]) -> List[str]:
        if isinstance(addr_list, str):
            addr_list = [addr_list]
        for addr in tqdm(addr_list, desc="Searching blockchain", total=len(addr_list)):
            tqdm.write("#######################################################")
            self._search(addr)
            tqdm.write("#######################################################\n")
        with open(os.path.join(self.cache_path, BLACKLIST_FILE), "w") as file:
            for a in self.blacklist:
                file.write(a + "\n")
        with open(os.path.join(self.cache_path, WALLETS_FILE), "w") as file:
            for a in self.wallets:
                file.write(a + "\n")
        return list(self.wallets)
        return list(self.wallets)
        
    def _search(self, addr: str):
        tqdm.write(f"Address {addr}:")
        if (addr.lower() in self.wallets) or (addr.lower() in self.blacklist):
            tqdm.write(f"Known address, skip...")
            return

        tqdm.write(f"Get transactions")
        res, status = self._get_api_response(addr)
        if int(status) == 0:
            if res:
                tqdm.write(res)
            return
        if len(res) > 100:  # Addresses with many transactions are blacklisted
            tqdm.write(f"Add to blacklist")
            self.blacklist.add(addr)
            return
        tqdm.write(f"Add to wallets")
        self.wallets.add(addr)

        for elem in res:
            self._search(elem["from"])
            self._search(elem["to"])
            self._search(elem["from"])
            self._search(elem["to"])

    def get_transactions_and_withdrawals_for_address_list(
        self,
        addr_list: List[str],
        columns: List[str],
        columns_w: List[str]
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Iterates over addr_list and looks for swaps and puts them in a df.
        TODO: More chains (currently only Ethereum)
        """
        if isinstance(addr_list, str):
            addr_list = [addr_list]

        df_tx = pd.DataFrame(columns=columns)
        df_w = pd.DataFrame(columns=columns_w)
        for addr in tqdm(addr_list, desc="Collecting transactions", total=len(addr_list)):
            df_tx_a, df_w_a = self.get_transactions_and_withdrawals_for_address(addr, columns, columns_w)
            if not df_tx_a.empty:
                if df_tx.empty:
                    df_tx = df_tx_a
                else:
                    df_tx = pd.concat((df_tx, df_tx_a))
            if not df_w_a.empty:
                if df_w.empty:
                    df_w = df_w_a
                else:
                    df_w = pd.concat((df_w, df_w_a))

        return df_tx, df_w

    def get_transactions_and_withdrawals_for_address(
        self,
        addr: str,
        columns: List[str],
        columns_w: List[str]
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        res, _ = self._get_api_response(addr)
        grouped_tx = self._get_grouped_txs(res)
        df_tx_a = pd.DataFrame(columns=columns)
        df_w_a = pd.DataFrame(columns=columns_w)
        for g in grouped_tx.values():
            if len(g) < 2:
                if g[0]["from"] in self.wallets:
                    df_w_t = self._process_transfer(g[0], addr)
                    if df_w_a.empty:
                        df_w_a = df_w_t
                    else:
                        df_w_a = pd.concat((df_w_a, df_w_t))
                continue

            df_tx_s = self._process_swap(g, addr)
            if df_tx_a.empty:
                df_tx_a = df_tx_s
            else:
                df_tx_a = pd.concat((df_tx_a, df_tx_s))

        return df_tx_a, df_w_a

    def _get_datetime(self, timestamp: int) -> str:
        dt = datetime.strftime(
            datetime.fromtimestamp(timestamp),
            "%Y-%m-%d %H:%M:%S"
        )
        return dt

    def _calc_gas(self, t: Dict) -> float:
        return (float(t["gasUsed"]) / 1e9) * (float(t["gasPrice"]) / 1e9)

    def _process_transfer(self, t: Dict, addr: str) -> pd.DataFrame:
        dt = self._get_datetime(int(t["timeStamp"]))
        fee = self._calc_gas(t)

        transfer = {
            "Datetime": dt,
            "Coin": t["tokenSymbol"],
            "Chain": "ETH",
            "Address": t["to"],
            "TxHash": t["hash"],
            "Fee": fee,
            "Fee currency": "ETH",
        }

        return pd.DataFrame([transfer])

    def _process_swap(self, g: List[Dict], addr: str) -> pd.DataFrame:
        def calc_amount(tx: Dict) -> float:
            dec = int(tx["tokenDecimal"])
            return float(tx["value"]) / (10**dec)

        dt = self._get_datetime(int(g[0]["timeStamp"]))

        sell_sym = g[0]["tokenSymbol"] if g[0]["from"] == addr else g[1]["tokenSymbol"]
        buy_sym = g[1]["tokenSymbol"] if g[0]["from"] == addr else g[0]["tokenSymbol"]
        sell_amount = calc_amount(g[0]) if g[0]["from"] == addr else calc_amount(g[1])
        buy_amount = calc_amount(g[1]) if g[0]["from"] == addr else calc_amount(g[0])
        fee_eth = self._calc_gas(g[0])
        swap = {
            "Datetime": dt,
            "Pair": buy_sym + "-" + sell_sym,
            "Side": "buy",
            "Size": buy_amount,
            "Funds": -sell_amount,
            "Fee": fee_eth,
            "Fee currency": "ETH",
            "Broker": "DEX"  # TODO: Get DEX
        }
        return pd.DataFrame([swap])

    def _get_grouped_txs(self, txs):
        grouped = defaultdict(list)
        for tx in txs:
            grouped[tx["hash"]].append(tx)
        return grouped

    def _get_api_response(self, addr: str) -> Tuple[Union[List, str], int]:
        req = "https://api.etherscan.io/api?module=account&action=tokentx&address={}&sort=asc&apikey={}".format(
            addr,
            self.api_key
        )
        response = requests.get(req)
        response = json.loads(response.text)
        return response["result"], int(response["status"])
