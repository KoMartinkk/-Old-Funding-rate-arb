import requests
from pybit.unified_trading import HTTP
import pandas as pd
import math
from urllib.parse import quote_plus
from datetime import datetime, timezone

""""""""
perp_names = [
    "WIF", "KAS", "1000BONK", "1000PEPE", "POPCAT", "FXS", "C98", "AR", 
    "INJ", "1000SATS", "FIL", "RVN", "APT", "DYDX", "LDO", "ANKR", 
    "RUNE", "1000LUNC", "NEAR", "GMX", "EGLD", "KAVA", "ARB", "FLOKI", 
    "CAKE", "ONE"
]

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

def unix_to_datetime_converter(unix_time: str) -> pd.to_datetime:

    """
    Convert a Unix timestamp in milliseconds to a Pandas Timestamp.

    Parameters:
        unix_time (str): A string representing a Unix timestamp in milliseconds.

    Returns:
        pd.Timestamp: A Pandas Timestamp object corresponding to the given Unix timestamp.
    """

    unix_time = int(unix_time)
    return pd.to_datetime(unix_time, unit='ms')

def datetime_to_unix_converter(datetime: pd.to_datetime) -> str:

    """
    Convert a Pandas Timestamp to a Unix timestamp in milliseconds.

    Parameters:
        datetime (pd.Timestamp): A Pandas Timestamp object to be converted.

    Returns:
        str: A string representing the Unix timestamp in milliseconds corresponding to the given Timestamp.
    """

    unix_time = (datetime - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')
    return unix_time * 1000

def bybit_fundrate_fetcher(token: str, start: str, end: str) -> pd.DataFrame:

    """
    Fetch historical funding rates for a token from the Bybit API.

    Parameters:
        coin (str): token symbol (e.g., "BTC").
        start (str): Start datetime (inclusive).
        end (str): End datetime (inclusive).

    Returns:
        pd.DataFrame: DataFrame with columns 'datetime' and 'funding_rate' of that token. 
    """

    session = HTTP()

    start_date = pd.to_datetime(start)
    end_date = pd.to_datetime(end)

    start_unix = datetime_to_unix_converter(start_date)
    end_unix = datetime_to_unix_converter(end_date)

    fund_rate = []

    while True:
        
        response = session.get_funding_rate_history(
                    category = "linear",
                    symbol = f"{token}USDT",
                    startTime = start_unix,
                    endTime = end_unix
                    )

        result = response['result']['list']

        if len(result) == 0:
            break

        for i in range(len(result)):
            data = [unix_to_datetime_converter(result[i]['fundingRateTimestamp']), result[i]['fundingRate']]
            fund_rate.append(data)

        if fund_rate[-1][0] > start_date:
            end_unix = datetime_to_unix_converter(fund_rate[-1][0])
            end_unix -= 28800000
        else:
            break
    
    fund_rate = pd.DataFrame(fund_rate, columns = ['datetime', 'funding_rate'])
    fund_rate = fund_rate.iloc[::-1].reset_index(drop=True)

    return fund_rate

def get_current_utc() -> str:
    """
    Get the current UTC time as a string in the format 'YYYY-MM-DD HH:MM:SS'.
    """
    # Get the current UTC time
    now_utc = datetime.now(timezone.utc)
    
    # Format the UTC time as a string
    utc_time_str = now_utc.strftime("%Y-%m-%d %H:%M:%S")
    
    return utc_time_str


def bybit_fundrate_fetchers(tokens: list[str], start: str, end: str) -> dict[str, pd.DataFrame]:

    """
    Fetch funding rates for multiple cryptocurrencies from the Bybit API.
    Note that the timeframe of bybit funding rate data is 8 hours. 

    Parameters:
        tokens (list[str]): List of cryptocurrency symbols (e.g., ["BTC", "ETH"]).
        start (str): Start datetime (inclusive).
        end (str): End datetime (inclusive).

    Returns:
        dict[str, pd.DataFrame]: A dictionary where keys are coin symbols and values are DataFrames
                                 containing datetime and funding rates.
    """

    token_fundrates = {}

    fundrate_dfs = [bybit_fundrate_fetcher(token = token, start=start, end=end) for token in tokens]
    
    token_fundrates = {token: fundrate_df for token, fundrate_df in zip(tokens, fundrate_dfs)}

    return token_fundrates