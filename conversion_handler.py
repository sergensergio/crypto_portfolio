import os
import json
from typing import Dict
from forex_python.converter import CurrencyRates
from datetime import datetime


class ConversionHandler:
    def __init__(self, cache_root: str) -> None:
        self.cache_path = cache_root + "/conversion"
        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)
        self.converter = CurrencyRates()
        self.conversion_data = None

    def get_conversion_rate(
            self,
            from_curr: str,
            to_curr: str,
            str_timestamp: str,
        )-> float:
        if not str_timestamp in self.conversion_data.keys():
            timestamp = datetime.strptime(str_timestamp, "%Y-%m-%d")
            conversion = self.converter.get_rates(from_curr, timestamp)[to_curr]
            self.conversion_data[str_timestamp] = conversion
        return self.conversion_data[str_timestamp]

    def load_conversion_dict(self, from_curr: str, to_curr: str):
        # Load conversion dict from file or create new dict
        file_name = "_".join([from_curr, to_curr]) + ".json"
        path = os.path.join(self.cache_path, file_name)
        if os.path.exists(path):
            with open(path, 'r') as file:
                self.conversion_data = json.load(file)
        else:
            self.conversion_data = dict()
    
    def save_conversion_dict(self, from_curr: str, to_curr: str):
        file_name = "_".join([from_curr, to_curr]) + ".json"
        path = os.path.join(self.cache_path, file_name)
        with open(path, 'w') as file:
            json.dump(self.conversion_data, file, indent=4)
        
        self.conversion_data = None        
