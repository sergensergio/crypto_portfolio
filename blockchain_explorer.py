import os
import requests
import json
import pandas as pd

from typing import List
from collections import defaultdict


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

        self.blacklist = []
        if os.path.exists(os.path.join(self.cache_path, BLACKLIST_FILE)):
            with open(os.path.join(self.cache_path, BLACKLIST_FILE), "r") as file:
                lines = file.readlines()
            self.blacklist = [line.strip() for line in lines]

        self.wallets = []
        if os.path.exists(os.path.join(self.cache_path, WALLETS_FILE)):
            with open(os.path.join(self.cache_path, WALLETS_FILE), "r") as file:
                lines = file.readlines()
            self.wallets = [line.strip() for line in lines]

    def bc_search(self, addr: str) -> List[str]:
        self._search(addr)
        with open(os.path.join(self.cache_path, BLACKLIST_FILE), "w") as file:
            for a in self.blacklist:
                file.write(a + "\n")
        with open(os.path.join(self.cache_path, WALLETS_FILE), "w") as file:
            for a in self.wallets:
                file.write(a + "\n")
        return self.wallets
        
    def _search(self, addr: str):
        print(f"Address {addr}:")
        if (addr.lower() in self.wallets) or (addr.lower() in self.blacklist):
            print(f"Skip")
            return

        print(f"Get transactions")
        req = "https://api.etherscan.io/api?module=account&action=tokentx&address={}&sort=asc&apikey={}".format(
            addr,
            self.api_key
        )
        response = requests.get(req)
        response = json.loads(response.text)
        res = response["result"]
        if len(res) > 100:  # Addresses with many transactions are blacklisted
            print(f"Add to blacklist")
            self.blacklist.append(addr)
            return
        print(f"Add to wallets")
        self.wallets.append(addr)

        for elem in res:
            self.search(elem["from"])
            self.search(elem["to"])

    def get_transactions_for_address(self, addr: str) -> pd.DataFrame:
        req = "https://api.etherscan.io/api?module=account&action=tokentx&address={}&sort=asc&apikey={}".format(
            addr,
            self.api_key
        )
        response = requests.get(req)
        response = json.loads(response.text)
        res = response["result"]
        
        # req = "https://api.etherscan.io/api?module=proxy&action=eth_getTransactionByHash&txhash={}&apikey={}".format(
        #     res[0]["hash"],
        #     self.api_key
        # )
        # response = requests.get(req)
        # response = json.loads(response.text)
        # res_tx = response["result"]

        grouped_tx = self._get_grouped_txs(res)
        p

    def _get_grouped_txs(self, txs):
        grouped = defaultdict(list)
        for tx in txs:
            grouped[tx["hash"]].append(tx)
        return grouped


if __name__ == "__main__":
    explorer = BlockchainExplorer()
    addr = ""
    addr_list = explorer.bc_search(addr)
    test = explorer.get_transactions_for_address(addr)