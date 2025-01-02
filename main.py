from pybit.unified_trading import HTTP
from pybit.exceptions import InvalidRequestError
import time
import pandas as pd
import numpy as np
import datetime
from tools import check_signs, perp_names, windows, shortperp_thresholds ,tg_message, round_down, round_up, bybit_fundrate_fetcher,datetime_to_unix_converter, unix_to_datetime_converter, bybit_fundrate_fetchers, get_current_utc
import os
import requests
from typing import Optional
import plotly_express as px
from FRAbacktestor import backtesting_zscore
import warnings
import logging
import sys

warnings.filterwarnings("ignore")  # Ignore warnings for cleaner output

# Time frames: backtesting and walk forward (IS: In-Sample, OOS: Out-of-Sample)
TIME_FRAMES = {
        'IS': {
            'since': "2020-01-01 00:00:00",
            'until': "2024-01-01 00:00:00"
        },
        'OOS': {
            'since': "2024-01-01 00:00:00",
            'until': get_current_utc()
        },}

# intro:
    # loop 3 params, for loop should be simpler than Glassnode, then save csv of tokens that pass bt and wf base on token name
    # different param for different model

#when testing:
# specify perp before program, e.g:
# categories = ['defi'] 
# beware of data type (string/float/int?)

#backtest period (IS)
bt_start= "2020-01-01 00:00:00"
bt_end = "2024-06-01 00:00:00"

#walk foward period (OOS) 
wf_start= "2024-06-01 00:00:00"
wf_end= get_current_utc()

def main():

    # Get the base directory where the script is being run
    based_location = os.getcwd()

    # Iterate through each FRA tokens (e.g.:'WIF')
    for perp in perp_names:

        funding_rate_data = {}  # Initialize a list to store backtesting results
        # get funding rate data for each token in perp_name list
        df = bybit_fundrate_fetcher(perp,bt_start,bt_end)
        funding_rate_data[perp] = df
        
        # Create a directory structure for storing results
        perp_folder = os.path.join(based_location, f"results/{perp}")
        os.makedirs(perp_folder, exist_ok=True)
  
        results = []  # Initialize a list to store backtesting results

        result_file_path = os.path.join(perp_folder, f"{perp}.csv")

        for window in windows:
            for shortperp_threshold in shortperp_thresholds:
                try:
                    result= backtesting_zscore(df, window, shortperp_threshold)
                    # perform backtesting
                    if result is not None:
                        # TODO: Perform walk-forward here
                        results.append(result)
                except Exception as e:
                        # Log any errors that occur and exit
                    logging.error(f"Error: {e} | {perp} | {window} | {shortperp_threshold}")
                    sys.exit()

                # # Skip processing if there isn't enough factor data, ''' not sure if useful here '''
                # if len(df) <= min_rows[resolution]:
                #     continue

            # Save backtesting results to a CSV file
            if results:
                result_df = pd.DataFrame(results).sort_values(by='sharpe', ascending=False)
                result_df.to_csv(result_file_path, index=False)
                logging.info(f"Saved results: {perp}")
            else:
            # Log if no results were generated
                with open(result_file_path, 'w') as f:
                    f.write(f"No results: {perp}\n")
                logging.info(f"No results: {perp}")

            logging.info(f"Completed: {perp}")


if __name__ == "__main__":
    main()












