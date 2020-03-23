#!/usr/bin/python
import sys
import os
base_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(base_path)
from data_utils.data_utils import DataManager

import logging

if __name__ == "__main__":

  logging.basicConfig(format='%(levelname)s - %(name)s -: %(asctime)s %(message)s',
                      level=logging.INFO)
  try:
    symbol_string = ""
    while len(symbol_string) <= 2:
      symbol_string = raw_input("Enter the stock symbol: ")

    data_manager = DataManager(api="alpha_vantage")
    imported_data = data_manager.get_historical_data(symbol=symbol_string,
                                                     interval='daily',
                                                     fetch_data=True,
                                                     save_fetched=True)
    print(imported_data.head())
    
  except Exception as e:
    print "Error" 	
    print e
