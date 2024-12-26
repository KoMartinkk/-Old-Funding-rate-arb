from pybit.unified_trading import HTTP
from pybit.exceptions import InvalidRequestError
import time
import pandas as pd
import numpy as np
import datetime
from tools import check_signs, perp_names,tg_message, round_down, round_up, bybit_fundrate_fetcher,datetime_to_unix_converter, unix_to_datetime_converter, bybit_fundrate_fetchers, get_current_utc
import os
from pprint import pprint
import requests
from typing import Optional
import plotly_express as px
from FRAbacktestor import backtesting_zscore
import warnings

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



def main():

    # Get the base directory where the script is being run
    based_location = os.getcwd()

    # Iterate through each FRA tokens (e.g.:'WIF')
    for perp in perp_names:
        factor_api_path = endpoint['path']  # API path for factor data
        factor_name = endpoint['name']  # Name of the factor
        factor_tokens = endpoint['assets']  # Associated tokens for the factor
        resolutions = endpoint['resolutions']  # Available resolutions for the factor data

        # Create a directory structure for storing results
        factor_folder = os.path.join(based_location, f"results/{category}/{factor_name}")
        os.makedirs(factor_folder, exist_ok=True)

        # Process data for each resolution
        for resolution in resolutions:
            resolution_folder = os.path.join(factor_folder, resolution)
            os.makedirs(resolution_folder, exist_ok=True)

            # Iterate through each token for the factor
            for factor_token in factor_tokens:
                result_file_path = os.path.join(resolution_folder, f"{factor_token}_{resolution}.csv")
                log_file_path = os.path.join(resolution_folder, f"{factor_token}_{resolution}.log")

                # Skip processing if results already exist
                if os.path.exists(result_file_path) or os.path.exists(log_file_path):
                    logging.info(f"Skipped: {factor_name} | {factor_token} | {resolution}")
                    continue

                results = []  # Initialize a list to store backtesting results

                # Fetch factor data from the API
                factor_res = get_data(
                    api_path=factor_api_path,
                    token=factor_token,
                    resolution=resolution,
                    since=utc_to_unix(TIME_FRAMES[resolution]['IS']['since']),
                    until=utc_to_unix(TIME_FRAMES[resolution]['OOS']['until']),
                    API_KEY=API_KEY
                    )

                # Skip processing if no data was returned
                 if factor_res is None:
                        continue

                    # Convert factor data into a DataFrame
                    factor_df = pd.read_json(StringIO(factor_res.text), convert_dates=['t'])

                    # Extract in-sample (IS) and out-of-sample (OOS) data for the factor
                    bt_factor_df = df_filter(df=factor_df, start_date=TIME_FRAMES[resolution]['IS']['since'],
                                                until_date=TIME_FRAMES[resolution]['IS']['until'])
                    
                    # Skip processing if there isn't enough factor data
                    if len(bt_factor_df) <= min_rows[resolution]:
                        continue

                    logging.info(f"Backtesting: {factor_name} | {factor_token} | {resolution}")

                    # Progress bar for price tokens
                    with tqdm(total=len(PRICE_TOKENS), 
                              desc=f"Processing Price Tokens ({factor_name} | {resolution} | {factor_token})") as pbar:
                        # Iterate over each price token for backtesting
                        for price_token in PRICE_TOKENS:
                            # Fetch price data from the API
                            price_res = get_data(
                                api_path=PRICE_API_PATH,
                                token=price_token,
                                resolution=resolution,
                                since=utc_to_unix(TIME_FRAMES[resolution]['IS']['since']),
                                until=utc_to_unix(TIME_FRAMES[resolution]['OOS']['until']),
                                API_KEY=API_KEY
                            )

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












