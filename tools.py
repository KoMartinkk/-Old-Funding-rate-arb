import pandas as pd
import requests
import pandas as pd
import numpy as np
import math
from binance.cm_futures import CMFutures
from urllib.parse import quote_plus
from datetime import datetime, timezone

windows = np.arange(5,60,5)

shortperp_thresholds = np.arange(0,2.2,0.2)

""""""""
#simplified perp list for testing
perp_names = [
    "FIL", "ONE"
]

#complete perp list
# perp_names = [
#     "WIF", "KAS", "1000BONK", "1000PEPE", "POPCAT", "FXS", "C98", "AR", 
#     "INJ", "1000SATS", "FIL", "RVN", "APT", "DYDX", "LDO", "ANKR", 
#     "RUNE", "1000LUNC", "NEAR", "GMX", "EGLD", "KAVA", "ARB", "FLOKI", 
#     "CAKE", "ONE"
# ]

# rolling function to check if consecutive days in window are +ve/-ve
def check_signs(window: int) -> int:

    if all(x > 0 for x in window):
        return -1
    elif all(x < 0 for x in window):
        return 1  
    else:
        return 0 
    
# tg message
def tg_message(message: str):
    """
    frarb_bot
    """
    base_url = 'https://api.telegram.org/bot7526783420:AAHVgjl_yYsuz3GGSsSrJ8UoCQn8OvhAY9M/sendMessage?chat_id=-4579402553&text='
    message = quote_plus(message) # special characters encoding

    requests.post(base_url + message)

def round_up(value: float, decimal_places: int) -> str:
    factor = 10 ** decimal_places
    return str(math.ceil(value * factor) / factor)

def round_down(value: float, decimal_places: int) -> str:
    factor = 10 ** decimal_places
    return str(math.floor(value * factor) / factor)

def get_current_utc() -> str:
    """
    Get the current UTC time as a string in the format 'YYYY-MM-DD HH:MM:SS'.
    """
    # Get the current UTC time
    now_utc = datetime.now(timezone.utc)
    
    # Format the UTC time as a string
    utc_time_str = now_utc.strftime("%Y-%m-%d %H:%M:%S")
    
    return utc_time_str

def utc_to_unix(utc_time_str) -> int:
    """
    Convert a UTC time string in %Y-%m-%d %H:%M:%S (e.g. 2020-01-01 00:00:00) string to a Unix timestamp.
    """
    try:
        # Parse the UTC time string into a datetime object
        utc_time = datetime.strptime(utc_time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        
        # Convert the datetime object to a Unix timestamp
        unix_timestamp = int(utc_time.timestamp())
        
        return unix_timestamp
    except ValueError:
        raise ValueError("Invalid UTC time format. Use 'YYYY-MM-DD HH:MM:SS'.")

import pandas as pd
from binance.cm_futures import CMFutures
from tools import utc_to_unix

def binance_fundrate_fetcher(token: str, start: str, until: str) -> pd.DataFrame:
    """
    Fetch funding rate data from Binance Futures API.

    Args:
        token (str): The token symbol (e.g., 'BTC').
        start (str): The start time in UTC (e.g., '2020-09-01 00:00:00').
        until (str): The end time in UTC (e.g., '2024-09-01 00:08:00').

    Returns:
        pd.DataFrame: A DataFrame containing funding time and funding rate.
    """
    # Convert start and until times to Unix timestamps in milliseconds
    start = utc_to_unix(start) * 1000
    until = utc_to_unix(until) * 1000
    eight_hours_ms = 8 * 60 * 60 * 1000  # 8 hours in milliseconds

    # Initialize Binance Futures client
    cm_futures_client = CMFutures()

    # Initialize an empty DataFrame to store the funding rate data
    funding_rate = pd.DataFrame()

    # Fetch funding rate data in chunks
    while True:
        # Fetch data using the Binance API
        new_funding_rate = pd.DataFrame(
            cm_futures_client.funding_rate(
                f"{token}USD_PERP", 
                **{"limit": 1000, "startTime": start, "endTime": until}
            )
        )

        # Select relevant columns
        new_funding_rate = new_funding_rate[['fundingTime', 'fundingRate']]

        # Append the new data to the existing DataFrame
        funding_rate = pd.concat([funding_rate, new_funding_rate], ignore_index=True)

        # Update the start time for the next API call to avoid overlapping data
        new_start = new_funding_rate['fundingTime'].iloc[-1] + eight_hours_ms

        # Check if the new start time exceeds the until time
        if new_start < until - eight_hours_ms:
            start = new_start
        else:
            break

    funding_rate.columns = ['datetime', 'funding_rate']
    # Convert fundingTime to datetime and format it
    funding_rate['datetime'] = pd.to_datetime(funding_rate['datetime'], unit='ms').dt.strftime('%Y-%m-%d %H:%M:%S')

    return funding_rate

# def bybit_fundrate_fetcher(token: str, start: str, end: str) -> pd.DataFrame:
#     """
#     Fetch historical funding rates for a token from the Bybit API.

#     Parameters:
#         coin (str): token symbol (e.g., "BTC").
#         start (str): Start datetime (inclusive).
#         end (str): End datetime (inclusive).

#     Returns:
#         pd.DataFrame: DataFrame with columns 'datetime' and 'funding_rate' of that token. 
#     """

#     session = HTTP()

#     start_date = pd.to_datetime(start)
#     end_date = pd.to_datetime(end)

#     start_unix = datetime_to_unix_converter(start_date)
#     end_unix = datetime_to_unix_converter(end_date)

#     fund_rate = []

#     while True:
        
#         response = session.get_funding_rate_history(
#                     category="linear",
#                     symbol=f"{token}USDT",
#                     startTime=start_unix,
#                     endTime=end_unix
#                     )

#         result = response['result']['list']

#         if len(result) == 0:
#             break

#         for i in range(len(result)):
#             data = [unix_to_datetime_converter(result[i]['fundingRateTimestamp']), result[i]['fundingRate']]
#             fund_rate.append(data)

#         if fund_rate[-1][0] > start_date:
#             end_unix = datetime_to_unix_converter(fund_rate[-1][0])
#             end_unix -= 28800000
#         else:
#             break
    
#     # Create DataFrame
#     fund_rate = pd.DataFrame(fund_rate, columns=['datetime', 'funding_rate'])
    
#     # Reverse the DataFrame and reset the index
#     fund_rate = fund_rate.iloc[::-1].reset_index(drop=True)

#     fund_rate['funding_rate'] = fund_rate['funding_rate'].astype(float)

#     return fund_rate

def get_current_utc() -> str:
    """
    Get the current UTC time as a string in the format 'YYYY-MM-DD HH:MM:SS'.
    """
    # Get the current UTC time
    now_utc = datetime.now(timezone.utc)
    
    # Format the UTC time as a string
    utc_time_str = now_utc.strftime("%Y-%m-%d %H:%M:%S")
    
    return utc_time_str

# def bybit_fundrate_fetchers(tokens: list[str], start: str, end: str) -> dict[str, pd.DataFrame]:

#     """
#     Fetch funding rates for multiple cryptocurrencies from the Bybit API.
#     Note that the timeframe of bybit funding rate data is 8 hours. 

#     Parameters:
#         tokens (list[str]): List of cryptocurrency symbols (e.g., ["BTC", "ETH"]).
#         start (str): Start datetime (inclusive).
#         end (str): End datetime (inclusive).

#     Returns:
#         dict[str, pd.DataFrame]: A dictionary where keys are coin symbols and values are DataFrames
#                                  containing datetime and funding rates.
#     """

#     token_fundrates = {}

#     fundrate_dfs = [bybit_fundrate_fetcher(token = token, start=start, end=end) for token in tokens]
    
#     token_fundrates = {token: fundrate_df for token, fundrate_df in zip(tokens, fundrate_dfs)}

#     return token_fundrates