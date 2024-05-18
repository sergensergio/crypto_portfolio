import os
import requests
import json
import pandas as pd

from typing import List, Tuple, Union, Dict
from collections import defaultdict
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
        if os.path.exists(os.path.join(self.cache_path, BLACKLIST_FILE)):
            with open(os.path.join(self.cache_path, BLACKLIST_FILE), "r") as file:
                lines = file.readlines()
            self.blacklist = set([line.strip() for line in lines])

        self.wallets = set()
        if os.path.exists(os.path.join(self.cache_path, WALLETS_FILE)):
            with open(os.path.join(self.cache_path, WALLETS_FILE), "r") as file:
                lines = file.readlines()
            self.wallets = set([line.strip() for line in lines])

    def search_blockchain(self, addr_list: List[str]) -> List[str]:
        if isinstance(addr_list, str):
            addr_list = [addr_list]
        for addr in tqdm(addr_list, desc="Searching blockchain", total=len(addr_list)):
            self._search(addr)
        with open(os.path.join(self.cache_path, BLACKLIST_FILE), "w") as file:
            for a in self.blacklist:
                file.write(a + "\n")
        with open(os.path.join(self.cache_path, WALLETS_FILE), "w") as file:
            for a in self.wallets:
                file.write(a + "\n")
        return list(self.wallets)
        
    def _search(self, addr: str):
        print(f"Address {addr}:")
        if (addr.lower() in self.wallets) or (addr.lower() in self.blacklist):
            print(f"Skip")
            return

        print(f"Get transactions")
        res, status = self._get_api_response(addr)
        if int(status) == 0:
            print(res)
            return
        if len(res) > 100:  # Addresses with many transactions are blacklisted
            print(f"Add to blacklist")
            self.blacklist.add(addr)
            return
        print(f"Add to wallets")
        self.wallets.add(addr)

        for elem in res:
            self._search(elem["from"])
            self._search(elem["to"])

    def get_transactions_for_address_list(self, addr_list: List[str], columns: List[str]) -> pd.DataFrame:
        """
        Iterates over addr_list and looks for swaps and puts them in a df.
        TODO: More chains (currently only Ethereum)
        """
        if isinstance(addr_list, str):
            addr_list = [addr_list]

        df = pd.DataFrame(columns=columns)
        for addr in tqdm(addr_list, desc="Collecting transactions", total=len(addr_list)):
            tx_df = self.get_transactions_for_address(addr, columns)
            if tx_df.empty:
                continue
            if df.empty:
                df = tx_df
            else:
                df = pd.concat((df, tx_df))

        return df

    def get_transactions_for_address(self, addr: str, columns: List[str]) -> pd.DataFrame:
        res, _ = self._get_api_response(addr)
        grouped_tx = self._get_grouped_txs(res)
        df = pd.DataFrame(columns=columns)
        for g in grouped_tx.values():
            if len(g) < 2:
                continue
            def calc_amount(tx: Dict) -> float:
                dec = int(tx["tokenDecimal"])
                return float(tx["value"]) / (10**dec)

            dt = datetime.strftime(
                datetime.fromtimestamp(int(g[0]["timeStamp"])),
                "%Y-%m-%d %H:%M:%S"
            )
            sell_sym = g[0]["tokenSymbol"] if g[0]["from"] == addr else g[1]["tokenSymbol"]
            buy_sym = g[1]["tokenSymbol"] if g[0]["from"] == addr else g[0]["tokenSymbol"]
            sell_amount = calc_amount(g[0]) if g[0]["from"] == addr else calc_amount(g[1])
            buy_amount = calc_amount(g[1]) if g[0]["from"] == addr else calc_amount(g[0])
            fee_eth = (float(g[0]["gasUsed"]) / 1e9) * (float(g[0]["gasPrice"]) / 1e9)
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
            df_tx = pd.DataFrame([swap])
            if df.empty:
                df = df_tx
            else:
                df = pd.concat((df, df_tx))

        return df

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

if __name__ == "__main__":
    explorer = BlockchainExplorer()
    addr = ""
    addr_list = explorer.search_blockchain(addr)
    test = explorer.get_transactions_for_address(addr)