import pandas as pd
import numpy as np

import os
import sys
import logging
from datetime import datetime
import time

from abc import ABCMeta, abstractmethod

TS_TYPE = {
    "daily",
    "weekly",
    "monthly"
}  # Types for time series

INTRA_DAY_TYPE = {
    "1min",
    "5min",
    "15min",
    "30min",
    "60min"
}  # Types of intervals for intra day

INTERVAL_TYPE = TS_TYPE.union(INTRA_DAY_TYPE)

TI_TYPE = {
    "SMA",    # simple moving average
    "EMA",    # exponential moving average
    "VWAP",   # volume weighted average price. INTRA DAY ONLY!!
    "MACD",   # moving average convergence
    "STOCH",  # stochastic oscillator
    "RSI",    # relative strength index
    "ADX",    # average directional movement
    "CCI",    # commodity channel index
    "AROON",  # aroon
    "BBANDS", # Bollinger bands
    "AD",     # Chaikin A/D line
    "OBV",    # balance volume
}  # Types of technical indictors

class DataAPIBase():
    __metaclass__ = ABCMeta
    
    def __init__(self):
        self.log = logging.getLogger("Data API")
        self._interval = None
        self._symbol = None
        # Intialize all data to empty
        self.ts_data = pd.DataFrame()
        self.ti_data = pd.DataFrame()

    @abstractmethod
    def update_symbol_interval(self, symbol=None, interval=None, force_update=False):
        pass

    @abstractmethod
    def fetch_ts_data(self):
        pass

    @abstractmethod
    def fetch_ti_data(self):
        pass

    def get_combined_data(self):
        if self._interval not in INTERVAL_TYPE:
            self.log.error("Interval type %s is not supported!", self._interval)
            raise("Interval type not supported!")
        assert self.ts_data.index.equals(self.ti_data.index), \
            "The time series and technical indicator data are not consistent!"
        data = pd.concat([self.ts_data, self.ti_data], axis=1)
        return data
