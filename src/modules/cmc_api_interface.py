import os
import json
import time
from typing import Dict, Union
from datetime import datetime
from requests import Session
from tqdm import tqdm


URL_CMC = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"


class CMCApiInterface:
    def __init__(
        self,
        cache_root: str = "cache",
        api_key_root: str = "api_keys",
    ) -> None:
        self.cache_path = cache_root + "/symbols"
        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)
        api_key_path = os.path.join(api_key_root, "cmc.txt")
        if not os.path.exists(api_key_path):
            raise FileNotFoundError(f"{api_key_path} not found.")
        with open(api_key_path, "r") as file:
            api_key = file.read()
        
        self.session = Session()
        headers = {
            "Accepts": "application/json",
            "X-CMC_PRO_API_KEY": api_key
        }
        self.session.headers.update(headers)

    def _get_data_from_cache(self, symbol: str) -> Union[Dict, None]:
        """
        Check cache if requested symbol has been fetched already today.
        If available, return cached data. Otherwise return None
        """
        file_name = symbol + ".json"
        path = os.path.join(self.cache_path, file_name)
        if os.path.exists(path):
            with open(path, 'r') as file:
                symbol_data = json.load(file)
            now = datetime.strftime(datetime.utcnow(), "%Y-%m-%d")
            cache_time = symbol_data["status"]["timestamp"][:10]
            if now == cache_time:
                return symbol_data

        return None

    def _get_data_from_api(self, symbol: str) -> Dict:
        """
        Fetch data for crypto via API call and save it in the cache.
        """
        parameters = {"symbol": symbol, "convert": "USD"}

        response = self.session.get(URL_CMC, params=parameters)
        new_data = json.loads(response.text)

        # Error 1008 occurs when api acces rate per minute got reached
        while new_data["status"]["error_code"] == 1008:
            print(new_data["status"]["error_message"])
            for _ in tqdm(range(60), desc="Waiting 1 minute..."):
                time.sleep(1)
            response = self.session.get(URL_CMC, params=parameters)
            new_data = json.loads(response.text)

        file_name = symbol + ".json"
        path = os.path.join(self.cache_path, file_name)
        with open(path, 'w') as file:
            json.dump(new_data, file, indent=4)
        
        return new_data

    def get_price_for_symbol(self, symbol: str) -> Dict:
        """
        Returns price from Coinmarketcap API response for a cryptocurrency.
        Saves data in cache to reduce API calls. If the data has been
        fetched today already the cached data is returned.

        Args:
            symbol: Symbol of currency, eg. BTC or ETH
        """

        data = self._get_data_from_cache(symbol)
        if data:
            return data["data"][symbol]["quote"]["USD"]["price"]

        new_data = self._get_data_from_api(symbol)
        
        return new_data["data"][symbol]["quote"]["USD"]["price"]
