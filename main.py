from pybit.unified_trading import HTTP
from pybit.exceptions import InvalidRequestError
import time
import pandas as pd
import numpy as np
import datetime
from tools import check_signs, perp_names, windows, shortperp_thresholds ,tg_message, round_down, round_up, bybit_fundrate_fetcher,datetime_to_unix_converter, unix_to_datetime_converter, bybit_fundrate_fetchers, get_current_utc
import os
from pprint import pprint
import requests
from typing import Optional
import plotly_express as px
from FRAbacktestor import backtesting_zscore
import warnings
from tqdm import tqdm

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
    #loop 3 params, for loop should be simpler than Glassnode, then save csv of tokens that pass bt and wf base on token name
    # keep rows and csv with sharpe*calmar>4, >10% AR and low mdd 

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

        # need to find a way to take each dataframe out again from dict to use backtestor function
        # find ways to call and loop the keys (token names) in funding_rate_data dictionary

        for window in windows:
            for shortperp_threshold in shortperp_thresholds:
                
                 # Create a directory structure for storing results
                perp_folder = os.path.join(based_location, f"results/{perp}")
                os.makedirs(perp_folder, exist_ok=True)
                
                # result = backtesting_zscore(df: pd.DataFrame, window: int, shortperp_threshold: float , plot: bool = False) -> Optional[pd.Series]:




                    # Skip processing if there isn't enough factor data
                    if len(bt_factor_df) <= min_rows[resolution]:
                        continue

                    logging.info(f"Backtesting: {factor_name} | {factor_token} | {resolution}")

                    # Progress bar for price tokens
                    with tqdm(total=len(PRICE_TOKENS), 
                              desc=f"Processing Price Tokens ({factor_name} | {resolution} | {factor_token})") as pbar:
                                         

                            # Convert price data into a DataFrame
                            price_df = pd.read_json(StringIO(price_res.text), convert_dates=['t'])

                            # Extract in-sample (IS) and out-of-sample (OOS) data for the price
                            bt_price_df = df_filter(df=price_df, start_date=TIME_FRAMES[resolution]['IS']['since'],
                                                        until_date=TIME_FRAMES[resolution]['IS']['until'])
                            
                            # Skip processing if there isn't enough price data
                            if len(bt_price_df) <= min_rows[resolution]:
                                pbar.update(1)  # Update progress bar
                                continue
                            
                            # Iterate over each movement type and parameter combination
                            for movement in MOVEMENTS:
                                for window, upper_thres, lower_thres in PARAM_ITERATIONS:
                                    try:
                                        # Perform backtesting
                                        result = backtestor(
                                            factor_name, category, factor_token, bt_factor_df, price_token, bt_price_df,
                                            resolution, window, upper_thres, lower_thres, movement
                                        )
                                        if result is not None:
                                            # TODO: Perform walk-forward here
                                            results.append(result)
                                    except Exception as e:
                                        # Log any errors that occur and exit
                                        logging.error(f"Error: {e} | {factor_name} | {factor_token} | {resolution} | {price_token}")
                                        sys.exit()
                            # Update progress bar after completing the current price token
                            pbar.update(1)

                    # Save backtesting results to a CSV file
                    if results:
                        result_df = pd.DataFrame(results).sort_values(by='sharpe', ascending=False)
                        result_df.to_csv(result_file_path, index=False)
                        logging.info(f"Saved results: {factor_name} | {factor_token} | {resolution}")
                    else:
                        # Log if no results were generated
                        with open(log_file_path, 'w') as f:
                            f.write(f"No results: {factor_name} | {factor_token} | {resolution}\n")
                        logging.info(f"No results: {factor_name} | {factor_token} | {resolution}")

                    logging.info(f"Completed: {factor_name} | {factor_token} | {resolution}")


if __name__ == "__main__":
    main()












