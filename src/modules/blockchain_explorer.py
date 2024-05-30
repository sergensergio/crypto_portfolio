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

        self.tx_hashes: List[str] = []

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
        
    def _search(self, addr: str):
        # TODO: Directly search for TxHashes?
        tqdm.write(f"Address {addr}:")
        if (addr in self.wallets) or (addr in self.blacklist):
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
        res, status = self._get_api_txs_for_address(addr)
        df_tx_a = pd.DataFrame(columns=columns)
        df_w_a = pd.DataFrame(columns=columns_w)
        if not status:
            return df_tx_a, df_w_a
        for tx in res:
            if tx["hash"] in self.tx_hashes:
                continue
            func = tx["functionName"].split("(")[0]
            if func == "":
                df_w_row = self._process_eth_tx(tx)
            elif func == "approve":
                df_w_row = self._process_approve(tx)
            elif func == "transfer":
                df_w_row = self._process_transfer(tx)
            elif func == "execute":
                df_tx_row, df_w_row = self._process_swap(tx, addr)
            elif func == "swap":
                df_tx_row, df_w_row = self._process_swap(tx, addr)
                pass
            else:
                raise NotImplementedError(f"Blockchain function not recognised: {func}")
            self.tx_hashes.append(tx["hash"])

            if func in ["", "approve", "transfer"]:
                if df_w_a.empty:
                    df_w_a = df_w_row
                else:
                    df_w_a = pd.concat((df_w_a, df_w_row))
            if func in ["execute", "swap"]:
                pass
                if df_tx_a.empty:
                    df_tx_a = df_tx_row
                else:
                    df_tx_a = pd.concat((df_tx_a, df_tx_row))

        return df_tx_a, df_w_a

    def _get_datetime(self, timestamp: int) -> str:
        dt = datetime.strftime(
            datetime.fromtimestamp(timestamp),
            "%Y-%m-%d %H:%M:%S"
        )
        return dt

    def _calc_gas(self, t: Dict) -> float:
        return (float(t["gasUsed"]) / 1e9) * (float(t["gasPrice"]) / 1e9)

    def _get_addr_no_padding(self, h: str) -> str:
        return "0x" + h[-40:]

    def _process_eth_tx(self, tx: Dict) -> pd.DataFrame:
        """
        Transactions which are a simple transfer of ETH from
        one wallet to another.
        """
        transfer = {
            "Datetime": self._get_datetime(int(tx["timeStamp"])),
            "Coin": "ETH",
            "Chain": "ETH",
            "Address": tx["to"],
            "TxHash": tx["hash"],
            "Fee": self._calc_gas(tx),
            "Fee currency": "ETH",
        }
        return pd.DataFrame([transfer])

    def _process_approve(self, tx: Dict) -> pd.DataFrame:
        """
        Token approvals on the Ethereum blockchain.
        """
        approval = {
            "Datetime": self._get_datetime(int(tx["timeStamp"])),
            "Coin": "ETH",
            "Chain": "ETH",
            "Address": tx["to"],
            "TxHash": tx["hash"],
            "Fee": self._calc_gas(tx),
            "Fee currency": "ETH",
        }
        return pd.DataFrame([approval])

    def _process_transfer(self, tx: Dict) -> pd.DataFrame:
        dt = self._get_datetime(int(tx["timeStamp"]))
        receipt = self._get_api_receipt_for_hash(tx["hash"])
        coin = self._get_api_symbol(receipt["logs"][0]["address"])
        to = self._get_addr_no_padding(receipt["logs"][-1]["topics"][2])
        fee = self._calc_gas(tx)

        transfer = {
            "Datetime": dt,
            "Coin": coin,
            "Chain": "ETH",
            "Address": to,
            "TxHash": tx["hash"],
            "Fee": fee,
            "Fee currency": "ETH",
        }

        return pd.DataFrame([transfer])

    def _process_uncompleted_swap(
        self,
        df_chain: pd.DataFrame,
        tx: Dict,
        addr_wallet: str,
        mode: str,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        dt = self._get_datetime(int(tx["timeStamp"]))
        tx_hash = tx["hash"]
        def search_chain(
            addr: str,
            sym_old: str,
            data_old: float,
            df: pd.DataFrame,
            swaps: List[Dict],
            fees: List[Dict],
        ):
            df_from_addr = df[df["from"] == addr]
            for i, row in df_from_addr.iterrows():
                if row["symbol"] == sym_old:
                    # If this address makes a tx with the same symbol as previous address, then
                    # it is either passing the crypto to the next address or making a fee payment
                    if (df["from"] == row["to"]).any():
                        # Check next address
                        swaps, fees = search_chain(
                            row["to"],
                            row["symbol"],
                            row["data"],
                            df[df["from"] != addr].copy(),
                            swaps,
                            fees
                        )
                    else:
                        # Dead end -> fee payment
                        # Special case: if wallet address is recipent of the tx,
                        # then its considered as already swapped
                        if row["to"] == addr_wallet:
                            continue
                        fee = {
                            "Datetime": dt,
                            "Coin": row["symbol"],
                            "Chain": "ETH",
                            "Address": row["to"],
                            "TxHash": tx_hash,
                            "Fee": row["data"],
                            "Fee currency": row["symbol"]
                        }
                        fees.append(fee)
                else:
                    # If this address makes a tx with another symbol than previous address, then
                    # it is a swap
                    if (df_from_addr["to"] == addr_wallet).any():
                        # Special case: if wallet address is recipent of the uncompleted swap,
                        # then check if wallet address is under to-addresses. In that case
                        # The swap is the tx with wallet and other txs are fee payments
                        if row["to"] == addr_wallet:
                            swap = {
                                "Datetime": dt,
                                "Pair": row["symbol"] + "-" + sym_old,
                                "Side": "buy",
                                "Size": row["data"],
                                "Funds": -data_old,
                                "Fee": 0,
                                "Fee currency": "ETH",
                                "Broker": "DEX"
                            }
                            swaps.append(swap)
                        else:
                            fee = {
                                "Datetime": dt,
                                "Coin": row["symbol"],
                                "Chain": "ETH",
                                "Address": row["to"],
                                "TxHash": tx_hash,
                                "Fee": row["data"],
                                "Fee currency": row["symbol"]
                            }
                            fees.append(fee)
                    else:
                        swap = {
                            "Datetime": dt,
                            "Pair": row["symbol"] + "-" + sym_old,
                            "Side": "buy",
                            "Size": row["data"],
                            "Funds": -data_old,
                            "Fee": 0,
                            "Fee currency": "ETH",
                            "Broker": "DEX"
                        }
                        swaps.append(swap)
                    if (df["from"] == row["to"]).any():
                        # Check next address
                        swaps, fees = search_chain(
                            row["to"],
                            row["symbol"],
                            row["data"],
                            df[df["from"] != addr].copy(),
                            swaps,
                            fees
                        )
            return swaps, fees

        if mode == "full" or mode == "sender":
            addr_start = addr_wallet
        else:
            addr_start = df_chain[~df_chain["from"].isin(df_chain["to"])]["from"].values
            assert len(addr_start) == 1
            addr_start = addr_start[0]
        sym_start = df_chain[df_chain["from"] == addr_start]["symbol"].values[0]
        data_start = df_chain[df_chain["from"] == addr_start]["data"].values[0]
        swaps = []
        fees = []
        swaps, fees = search_chain(
            addr_start,
            sym_start,
            data_start, 
            df_chain.copy(),
            swaps,
            fees
        )
        return pd.DataFrame(swaps), pd.DataFrame(fees)

    def _process_swap(self, tx: Dict, addr: str) -> pd.DataFrame:
        receipt = self._get_api_receipt_for_hash(tx["hash"])
        chain = []
        for log in receipt["logs"]:
            if (len(log["data"]) > 66) or (len(log["topics"]) < 3):
                continue
            fr = self._get_addr_no_padding(log["topics"][1])
            to = self._get_addr_no_padding(log["topics"][2])
            dec = self._get_api_decimals(log["address"])
            data = int(log["data"], 16) / 10**dec
            sym = self._get_api_symbol(log["address"])
            row = {
                "from": fr,
                "to": to,
                "data": data,
                "symbol": sym
            }
            chain.append(row)
        df_chain = pd.DataFrame(chain)
 
        if any(df_chain["from"] == addr) and any(df_chain["to"] == addr):
            df_swaps, df_fees = self._process_uncompleted_swap(df_chain, tx, addr, mode="full")
        elif any(df_chain["from"] == addr):
            df_swaps, df_fees = self._process_uncompleted_swap(df_chain, tx, addr, mode="sender")
        elif any(df_chain["to"] == addr):
            df_swaps, df_fees = self._process_uncompleted_swap(df_chain, tx, addr, mode="recipent")
        else:
            raise NotImplementedError("???")

        return df_swaps, df_fees

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

    def _get_api_symbol(self, addr: str) -> str:
        req = "https://api.etherscan.io/api?module=proxy&action=eth_call&to={}&data=0x95d89b41&tag=latest&apikey={}".format(
            addr,
            self.api_key
        )
        response = requests.get(req)
        response = json.loads(response.text)
        sym = bytes.fromhex(
            response["result"][2:]
        ).decode("utf-8")
        for s in ["\x06", "\x00", "\x05", "\x04", "\x03", " "]:
            sym = sym.replace(s, "")
        return sym

    def _get_api_decimals(self, addr: str) -> str:
        req = "https://api.etherscan.io/api?module=proxy&action=eth_call&to={}&data=0x313ce567&tag=latest&apikey={}".format(
            addr,
            self.api_key
        )
        response = requests.get(req)
        response = json.loads(response.text)
        num_decimals = int(response["result"], 16)
        return num_decimals

    def _get_api_receipt_for_hash(self, tx_hash: str) -> Dict:
        req = "https://api.etherscan.io/api?module=proxy&action=eth_getTransactionReceipt&txhash={}&apikey={}".format(
            tx_hash,
            self.api_key
        )
        response = requests.get(req)
        response = json.loads(response.text)
        return response["result"]
    
    def _get_api_txs_for_address(self, addr: str) -> Tuple[Union[List, str], int]:
        req = "https://api.etherscan.io/api?module=account&action=txlist&address={}&sort=asc&apikey={}".format(
            addr,
            self.api_key
        )
        response = requests.get(req)
        response = json.loads(response.text)
        return response["result"], int(response["status"])

if __name__ == "__main__":
    bc = BlockchainExplorer()

    COLUMNS = ["Datetime", "Pair", "Side", "Size", "Funds", "Fee", "Fee currency", "Broker"]
    COLUMNS_W = ["Datetime", "Coin", "Chain", "Address", "TxHash", "Fee", "Fee currency"]

    # tx = ""
    ad = "".lower()
    res = bc.get_transactions_and_withdrawals_for_address_list(ad, COLUMNS, COLUMNS_W)
    res