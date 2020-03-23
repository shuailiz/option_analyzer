"""
 Util functions for data manipulation
"""
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib import rcParams

import pandas as pd
import numpy as np

import os
import sys
import logging
from datetime import datetime
import time

folder_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(folder_path)
from data_api_base import INTERVAL_TYPE, INTRA_DAY_TYPE, TI_TYPE
from alpha_vantage_handle import AlphaVantageHandle

class DataManager(object):
    TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(self, api):
        self.log = logging.getLogger("Data Manager")

        self.api_handle = None
        if api == "alpha_vantage":
            self.api_handle = AlphaVantageHandle(skip_data_init=True)
        else:
            self.log.error("API type %s has not been implemented!", api)
            raise NotImplementedError

    @staticmethod
    def get_data_store_path(interval=None, symbol=""):
        base_folder_path = os.path.dirname(
            os.path.dirname(os.path.realpath(__file__)))
        if interval is None:
            self.log.warn("No interval type specified. Returning the root folder")
            return os.path.join(base_folder_path, "data")
        elif interval not in INTERVAL_TYPE:
            self.log.error("Invalid interval type %s!", interval)
            raise Exception("Invalid interval type!")
        elif interval in INTRA_DAY_TYPE:
            return os.path.join(base_folder_path, "data", "intra_day", symbol)
        else:
            return os.path.join(base_folder_path, "data", interval, symbol)
    
    def get_historical_data(self,
                            symbol,
                            interval,
                            attrib=[],
                            start=None,
                            end=None,
                            fetch_data=False,
                            drop_na_rows=False,
                            save_fetched=False):
        """
        Get the historical data of a symbol
        input:
            symbol (str): The symbol to get data for
            interval (str): The interval to get data for
            attrib (array): The attributes to get for the current symbol
            start (str): The start date. expected mm/dd/yyyy
            end (str): The end date. expected mm/dd/yyyy
            fetch_data (bool): Whether or not to fetch data from the data api
            drop_na_rows (bool): Whether or not to drop rows that contains Nan
            save_fetched (bool): Whether or not to save the fetched data
        output:
            hist_data (arry): The historical data of the symbol from stat to end
        """
        if interval in INTRA_DAY_TYPE:
            self.log.error("Cannot get historical data for intra day!")
            return None

        if len(symbol) < 1:
            self.log.error("Invalid symbol: %s", symbol)
            raise Exception("Invalid symbol!")

        # Convert symbol to all capital letters
        symbol = symbol.upper()
        
        # Get the raw data
        raw_data = None
        if not fetch_data:
            raw_data = self._load_data(symbol=symbol, interval=interval)
        # Fetch data from api if nessesary
        if raw_data is None:
            fetch_data = True
            # Fetch raw data through the api
            self.api_handle.update_symbol_interval(symbol=symbol,
                                                   interval=interval,
                                                   force_update=True)
            raw_data = self.api_handle.get_combined_data()

        all_col = False
        if len(attrib) == 0:
            self.log.warn("No attributes specified. Returning all columns!")
            all_col = True

        if start == None:
            self.log.warn("No start date specified. Returning from the first row!")
            start = raw_data.index[0].strftime(self.TIME_FORMAT)

        if end == None:
            self.log.warn("No end date specified. Returning till the last row!")
            end = raw_data.index[-1].strftime(self.TIME_FORMAT)

        if pd.Timestamp(start) < raw_data.index[0] or \
               pd.Timestamp(end) > raw_data.index[-1]:
            self.log.error("Invaid start and end for historical data")
            return None

        freq = ""
        if interval == "daily":
            freq = 'D'
        elif interval == "weekly":
            freq = 'W'
        elif interval == "monthly":
            freq = 'M'

        # Get the range for the rows
        row_range = pd.date_range(start=start, end=end, freq=freq)

        data = []
        if all_col:
            data = raw_data.loc[row_range].copy()
        else:
            data = raw_data.loc[row_range, attrib].copy()

        # Drop the nan rows if nessesary
        if drop_na_rows:
            data.dropna(inplace=True)

        if save_fetched and fetch_data:
            self._save_data(raw_data, symbol=symbol, interval=interval)
        return data

    def _load_data(self, symbol, interval, latest=True):
        """
        Load data of the symbol with the interval
        """
        if not latest:
            self.log.error("Retriving archived data that is not the latest has not been implemented")
            raise NotImplementedError

        # Get all the data file for symbol and interval
        data_storage_path = DataManager.get_data_store_path(interval=interval, symbol=symbol)
        if not os.path.exists(data_storage_path):
            self.log.warn("No saved data found for %s with interval %s", symbol, interval)
            return None
        saved_data_files = [f for f in os.listdir(data_storage_path)
                           if os.path.isfile(os.path.join(data_storage_path, f))]
        # Latest data first
        saved_data_files.sort(reverse=True)
        if len(saved_data_files) == 0:
            self.log.warn("No data files saved under %s", data_storage_path)
            return None
        
        # Load the lastest and return the data
        data = pd.read_pickle(os.path.join(data_storage_path, saved_data_files[0]))
        return data
        
    def _save_data(self, data, symbol, interval):
        """
        Save the data to the data store
        """
        if interval not in INTERVAL_TYPE:
            self.log.error("Interval type %s is not supported while saving the data!", interval)
            raise Exception("Interval type not supported!")

        self.log.info("Saving data with:\n"
                      "  symbol: %s\n"
                      "  interval: %s", symbol, interval)
        cur_ts_str = datetime.now().strftime(self.TIME_FORMAT)
        file_name = cur_ts_str
        data_storage_path = DataManager.get_data_store_path(interval=interval, symbol=symbol)
        if interval in INTRA_DAY_TYPE:
            file_name += "_" + interval

        if len(data_storage_path) > 0 and not os.path.exists(data_storage_path):
            os.makedirs(data_storage_path)
        file_path = os.path.join(data_storage_path, file_name)
        data.to_pickle(file_path)
        self.log.info("Data saved to %s", file_path)
