import pandas as pd
from datetime import datetime

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