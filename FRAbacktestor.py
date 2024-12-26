from pybit.unified_trading import HTTP
from pybit.exceptions import InvalidRequestError
import time
import pandas as pd
import numpy as np
import datetime
from tools import check_signs, perp_names,tg_message, round_down, round_up, bybit_fundrate_fetcher,datetime_to_unix_converter, unix_to_datetime_converter, bybit_fundrate_fetchers
import os
from pprint import pprint
import requests
from typing import Optional
import plotly_express as px

# arbitrage account, need to change api key
session = HTTP(
    api_key="ktDiLh7gzoyUw3nbdf",
    api_secret="IVc62GgFgpF5bcQOCwsrZ8INGYlymtxV5h2v",
)

#different param for different model
# window = np.arange(5,60,5)
# shortperp_threshold = np.arange(-2,2.2,0.2)


#rolling z-score model; param : window,z score threshold
def backtesting_zscore(df: pd.DataFrame, window: int, shortperp_threshold: float , plot: bool = False) -> Optional[pd.Series]:

    # Convert 'funding_rate' to numeric, replacing non-numeric values with NaN, then fill NaN cells into 0
    df['funding_rate'] = pd.to_numeric(df['funding_rate'], errors='coerce')
    df['funding_rate'].fillna(0, inplace=True)

    df['funding_ma'] = df['funding_rate'].rolling(window).mean()
    df['funding_sd'] = df['funding_rate'].rolling(window).std()
    df['funding_z'] = (df['funding_rate'] - df['funding_ma']) / df['funding_sd']

    # Initialize positions
    df['perp_pos'] = 0
    df['spot_pos'] = 0

    # Calculate perp_pos
    df['perp_pos'] = np.where(
    (df['perp_pos'].shift(1) == -1) & (df['funding_rate'] >= 0),  # Maintain pos if previous was -1 and funding_rate >= 0
    -1,
    np.where(
        (df['funding_z'] > shortperp_threshold) & (df['funding_rate'] >= 0 ),  # Open position if funding_z > threshold
        -1,
        0  # else, = 0    
    )
    )

    #cal spot pos
    df['spot_pos'] = np.where(
    (df['spot_pos'].shift(1) == 1) & (df['funding_rate'] >= 0),  # Maintain pos if previous was -1 and funding_rate >= 0
    1,
    np.where(
        (df['funding_z'] > shortperp_threshold) & (df['funding_rate'] >= 0),  # Open position if funding_z > threshold
        1,
        0  # else, = 0
    )
    )

      # Calculate trade columns
    df['trade_perp'] = abs(df['perp_pos'].diff().fillna(0))  # Changes in perpetual futures position
    df['trade_spot'] = abs(df['spot_pos'].diff().fillna(0))  # Changes in spot position
    df['trade'] = df['trade_perp'] + df['trade_spot']       # Combined changes in positions

    # Calculate transaction costs
    df['transaction_cost'] = (df['trade_perp'] * 0.055 / 100) + (df['trade_spot'] * 0.1 / 100)

    # Initialize PnL column
    df['pnl'] = 0.0

    # Define the PnL calculation logic
    df['pnl'] = np.where(
        df['trade'] > 0,  # If there is change in pos (trade occurred)
        -df['transaction_cost']+ df['funding_rate'],  # - transaction cost + funding rate received
        np.where(
            (df['perp_pos'] == -1) & (df['spot_pos'] == 1) & (df['trade'] == 0),  # Positions active, no trade
            df['funding_rate'],  # Add corresponding funding rate
            0  # No change in position, PnL remains 0
        )
    )
    
    df['cumu'] = df['pnl'].cumsum()
    df['dd'] = df['cumu'].cummax() - df['cumu']

    annual_return = round(df['pnl'].mean() * 3 * 365, 2)

    if df['pnl'].std() != 0:
        sharpe = round((df['pnl'].mean() / df['pnl'].std()) * np.sqrt(3 * 365), 2)
    else:
        sharpe = np.nan

    mdd = round(df['dd'].max(), 3)
    
    # avoid division of zero
    if mdd != 0:
        calmar = round(annual_return / mdd,2)
    else:
        calmar = np.nan

    if plot:
      print(pd.Series([window, sharpe, calmar, annual_return, mdd],index= ['window', 'sharpe', 'calmar', 'annual_return', 'mdd']))
      fig = px.line(df,x='datetime', y=['cumu'])
      fig.show()
      return

    return pd.Series([window,shortperp_threshold, sharpe, calmar, annual_return, mdd],
                     index= ['window', 'short_perp_threshold','sharpe', 'calmar', 
                             'annual_return', 'mdd'])



#rolling z-score walk forward
#def walkforward_zscore(df: pd.DataFrame, window: int, plot: bool = False) -> Optional[pd.Series]:



#other models



#other model walk forward














