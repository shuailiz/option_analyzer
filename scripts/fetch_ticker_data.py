import sys
import os
import pandas as pd
import numpy as np


base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))) #change the file name here
sys.path.append(base_path)
from data_utils.data_utils import DataManager


import logging
logging.basicConfig(format='%(levelname)s - %(name)s -: %(asctime)s %(message)s',level=logging.INFO)


#Import local ticker file
data_path = os.path.join(base_path, 'data')
ticker_file = 'Ticker_List.csv'
ticker_list = pd.read_csv(os.path.join(data_path,ticker_file), header = 0)

ticker = ticker_list['Ticker'].to_numpy()


def download_data(ticker):
	data_manager = DataManager(api="alpha_vantage")
	imported_data = data_manager.get_historical_data(symbol=symbol_string,
                                                     interval='daily')

for i in (len(ticker)):
	download_data(ticker[i])