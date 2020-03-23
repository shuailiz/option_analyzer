from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.techindicators import TechIndicators

import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib import rcParams

import pandas as pd
import numpy as np

import os
import sys
import logging
from datetime import datetime
from Queue import Queue
import time

folder_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(folder_path)
from data_api_base import DataAPIBase, INTERVAL_TYPE, INTRA_DAY_TYPE, TI_TYPE


class AlphaVantageHandle(DataAPIBase):
    RAPIDAPI_KEY = "de7dba73d8msh6207eff68e3f134p10e7d1jsn87fd4c7d47de"
    ALPHA_VANTAGE_KEY = "EL89QWXFIKG17CEH"
    ALPHA_VANTAGE_PRIMIUM_KEY = "BO5YJYOVZPVLML38"
    API_TIME_LIMIT = 70  # The limit unit for trading in seconds
    API_CALL_LIMIT = 30   # The limit of number of calls per limit unit
    
    def __init__(self,
                 interval=None,
                 symbol=None,
                 use_rapid=False,
                 skip_data_init=False,
                 outputsize="full"):
        super(AlphaVantageHandle, self).__init__()
        self.log = logging.getLogger("Alpha Vantage API")
        
        self._outputsize = outputsize
        self.api_time_queue = Queue()
        # Initialize alpha vantage time series
        if use_rapid:
            self.time_series = TimeSeries(key=self.RAPIDAPI_KEY,
                                          output_format="pandas",
                                          rapidapi=True)
        else:
            self.time_series = TimeSeries(key=self.ALPHA_VANTAGE_PRIMIUM_KEY,
                                          output_format="pandas")

        self.tech_indicators = TechIndicators(key=self.ALPHA_VANTAGE_PRIMIUM_KEY,
                                              output_format='pandas')

        if not skip_data_init:
            # Set the symbol and interval for the handle
            self.update_symbol_interval(symbol=symbol, interval=interval)
            # Fetch time series data
            self.log.info("Downloading time series and technical indicator data from alpha vantage:\n "
                          "   symbol: %s\n"
                          "   interval: %s\n", self._symbol, self._interval)
            self.update_all_data()
            self.log.info("Data downloaded")

    def update_symbol_interval(self, symbol=None, interval=None, force_update=False):
        """
        Update the symbol and interval for the handle
        """
        if symbol == None or (symbol != "keep" and len(symbol) <= 2):
            self.log.error("Invalid symbol: %s", symbol)
            raise("Invalid symbol!")
        if self._symbol is not None:
            force_update = True
        if symbol != "keep":
            self._symbol = symbol

        if interval not in INTERVAL_TYPE and interval != "keep":
            self.log.error("Interval type %s is invalid. You must provide a type:\n  " + \
                           "".join(iv + "\n  " for iv in INTERVAL_TYPE), interval)
            raise("Invalid interval type!")

        if self._interval is not None:
            force_update = True
        if interval != "keep":
            self._interval = interval
            
        if force_update:
            self.update_all_data()

    def update_all_data(self):
        self.log.info("Updating time series and tech indicators data")
        self.fetch_ts_data()
        self.fetch_ti_data()

    def _limited_api_wrapper(self, func):
        time_queue = self.api_time_queue
        time_limit = self.API_TIME_LIMIT
        call_limit = self.API_CALL_LIMIT
        def limited_api(*args, **kwargs):
            if time_queue.empty():
                time_queue.put(time.time())
            else:
                cur_time = time.time()
                while not time_queue.empty():
                    if time_queue.queue[0] < cur_time - time_limit:
                        time_queue.get()
                    else:
                        break
                if time_queue.qsize() >= call_limit:
                    time_delta = time.time() - time_queue.queue[0]
                    time.sleep(time_delta)
                    time_queue.get()
            time_queue.put(time.time())
            return func(*args, **kwargs)
        return limited_api

    def fetch_ts_data(self):
        """
        Fetch time series data from alpha vantage based on the current data type
        """
        if self._interval not in INTERVAL_TYPE:
            self.log.error("Interval type %s is not supported!", self._interval)
            raise("Interval type not supported!")

        if self._interval in INTRA_DAY_TYPE:
            self.ts_data, metadata = self.time_series.get_intraday(symbol=self._symbol,
                                                                   interval=self._interval,
                                                                   outputsize=self._outputsize)
        elif self._interval == "daily":
            self.ts_data, metadata = self.time_series.get_daily_adjusted(symbol=self._symbol,
                                                                         outputsize=self._outputsize)
        elif self._interval == "weekly":
            self.ts_data, metadata = self.time_series.get_weekly_adjusted(symbol=self._symbol,
                                                                          outputsize=self._outputsize)
        elif self._interval == "monthly":
            self.ts_data, metadata = self.time_series.get_monthly_adjusted(symbol=self._symbol,
                                                                           outputsize=self._outputsize)
        else:
            self.log.error("Interval type %s not implemented!", self._interval)
            raise NotImplementedError

        self.log.info("Fetched data with:\n"
                      "  information: %(1. Information)s\n"
                      "  symbol: %(2. Symbol)s\n"
                      "  last refreshed: %(3. Last Refreshed)s"%metadata)

    def fetch_ti_data(self):
        """
        Fetch time indicator data from alpha vantage based on the current data type
        """
        if self._interval not in INTERVAL_TYPE:
            self.log.error("Interval type %s is not supported!", self._interval)
            raise("Interval type not supported!")
        for ti_type in TI_TYPE:
            self.log.info("Fetching ti data for %s", ti_type)
            ti_data = self._fetch_ti_data_helper(ti_type)
            if not ti_data.empty:
                self.ti_data = pd.concat([self.ti_data, ti_data],
                                         axis=1)

    def _fetch_ti_data_helper(self, ti_type):
        """
        Helper function to get data for a specific technical indicator
        We are currently getting high and low for each interval
        """
        ti_data = pd.DataFrame()
        if ti_type == "SMA":
            get_sma = self._limited_api_wrapper(self.tech_indicators.get_sma)
            data_high, metadata = get_sma(symbol=self._symbol,
                                          interval=self._interval,
                                          series_type="high")
            data_low, metadata = get_sma(symbol=self._symbol,
                                         interval=self._interval,
                                         series_type="low")
            ti_data = pd.concat([data_high, data_low], axis=1)
            ti_data.columns = ["SMA_H", "SMA_L"]
        elif ti_type == "EMA":
            get_ema = self._limited_api_wrapper(self.tech_indicators.get_ema)
            data_high, metadata = get_ema(symbol=self._symbol,
                                          interval=self._interval,
                                          series_type="high")
            data_low, metadata = get_ema(symbol=self._symbol,
                                         interval=self._interval,
                                         series_type="low")
            ti_data = pd.concat([data_high, data_low], axis=1)
            ti_data.columns = ["EMA_H", "EMA_L"]
        elif ti_type == "VWAP":
            if self._interval not in INTRA_DAY_TYPE:
                self.log.error("VWAP only support intra day data. No data gathered")
            else:
                get_vwap = self._limited_api_wrapper(self.tech_indicators.get_vwap)
                data, metadata = get_vwap(symbol=self._symbol, interval=self._interval)
                ti_data = data.copy()
        elif ti_type == "MACD":
            get_macd = self._limited_api_wrapper(self.tech_indicators.get_macd)
            data_high, metadata = get_macd(symbol=self._symbol,
                                           interval=self._interval,
                                           series_type="high")
            data_low, metadata = get_macd(symbol=self._symbol,
                                          interval=self._interval,
                                          series_type="low")

            ti_data = pd.concat([data_high[["MACD_Signal", "MACD"]],
                                 data_low[["MACD_Signal", "MACD"]]], axis=1)
            ti_data.columns = ["MACD_Signal_H", "MACD_H", "MACD_Signal_L", "MACD_L"]
        elif ti_type == "STOCH":
            get_stoch = self._limited_api_wrapper(self.tech_indicators.get_stoch)
            data, metadata = get_stoch(symbol=self._symbol, interval=self._interval)
            ti_data = data.copy()
        elif ti_type == "RSI":
            get_rsi = self._limited_api_wrapper(self.tech_indicators.get_rsi)
            data_high, metadata = get_rsi(symbol=self._symbol,
                                          interval=self._interval,
                                          series_type="high")
            data_low, metadata = get_rsi(symbol=self._symbol,
                                         interval=self._interval,
                                         series_type="low")
            ti_data = pd.concat([data_high, data_low], axis=1)
            ti_data.columns = ["RSI_H", "RSI_L"]
        elif ti_type == "ADX":
            get_adx = self._limited_api_wrapper(self.tech_indicators.get_adx)
            data, metadata = get_adx(symbol=self._symbol,
                                     interval=self._interval)
            ti_data = data.copy()
        elif ti_type == "CCI":
            get_cci = self._limited_api_wrapper(self.tech_indicators.get_cci)
            data, metadata = get_cci(symbol=self._symbol,
                                     interval=self._interval)
            ti_data = data.copy()
        elif ti_type == "AROON":
            get_aroon = self._limited_api_wrapper(self.tech_indicators.get_aroon)
            data_high, metadata = get_aroon(symbol=self._symbol,
                                            interval=self._interval,
                                            series_type="high")
            data_low, metadata = get_aroon(symbol=self._symbol,
                                           interval=self._interval,
                                           series_type="low")
            ti_data = pd.concat([data_high, data_low], axis=1)
            ti_data.columns = ["Aroon_Down_H", "Aroon_Up_H", "Aroon_Down_L", "Aroon_Up_L"]
        elif ti_type == "BBANDS":
            get_bbands = self._limited_api_wrapper(self.tech_indicators.get_bbands)
            # We want both SMA and EMA features
            data_high_sma, metadata = get_bbands(symbol=self._symbol,
                                                 interval=self._interval,
                                                 series_type="high")
            data_low_sma, metadata = get_bbands(symbol=self._symbol,
                                                interval=self._interval,
                                                series_type="low")

            data_high_ema, metadata = get_bbands(symbol=self._symbol,
                                                 interval=self._interval,
                                                 series_type="high",
                                                 matype=1)
            data_low_ema, metadata = get_bbands(symbol=self._symbol,
                                                interval=self._interval,
                                                series_type="low",
                                                matype=1)
            ti_data = pd.concat([data_high_sma, data_high_ema, data_low_sma, data_low_ema], axis=1)
            ti_data.columns = ["BBANDS_Upper_SMA_H",
                               "BBANDS_Lower_SMA_H",
                               "BBANDS_Middle_SMA_H",
                               "BBANDS_Upper_EMA_H",
                               "BBANDS_Lower_EMA_H",
                               "BBANDS_Middle_EMA_H",
                               "BBANDS_Upper_SMA_L",
                               "BBANDS_Lower_SMA_L",
                               "BBANDS_Middle_SMA_L",
                               "BBANDS_Upper_EMA_L",
                               "BBANDS_Lower_EMA_L",
                               "BBANDS_Middle_EMA_L"]
        elif ti_type == "AD":
            get_ad = self._limited_api_wrapper(self.tech_indicators.get_ad)
            data, metadata = get_ad(symbol=self._symbol,
                                    interval=self._interval)
            ti_data = data.copy()
        elif ti_type == "OBV":
            get_obv = self._limited_api_wrapper(self.tech_indicators.get_obv)
            data, metadata = get_obv(symbol=self._symbol,
                                     interval=self._interval)
            ti_data = data.copy()
        else:
            self.log.error("Technical indicator type %s not implemented!", ti_type)
            raise NotImplementedError

        return ti_data
