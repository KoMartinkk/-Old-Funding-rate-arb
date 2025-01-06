from pybit.unified_trading import HTTP
from pybit.exceptions import InvalidRequestError
import time
import pandas as pd
import numpy as np
import datetime
from tools import check_signs,tg_message,perp_names, round_down, round_up, bybit_fundrate_fetcher,datetime_to_unix_converter, unix_to_datetime_converter, bybit_fundrate_fetchers
import os
from pprint import pprint
import requests
from typing import Optional
import plotly.express as px

# arbitrage account, need to change api key
session = HTTP(
    api_key="ktDiLh7gzoyUw3nbdf",
    api_secret="IVc62GgFgpF5bcQOCwsrZ8INGYlymtxV5h2v",
)

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

    # Iterate through rows to propagate the perp position correctly
    for i in range(1, len(df)):
        if df.loc[i, 'funding_z'] > shortperp_threshold and df.loc[i, 'funding_rate'] >= 0:
            df.loc[i, 'perp_pos'] = -1  # Open or maintain short position
        elif df.loc[i - 1, 'perp_pos'] == -1 and df.loc[i, 'funding_rate'] >= 0:
            df.loc[i, 'perp_pos'] = -1  # Continue short position
        else:
            df.loc[i, 'perp_pos'] = 0  # Close position

    # Iterate through rows to propagate the spot position correctly
    for i in range(1, len(df)):
        if df.loc[i, 'funding_z'] > shortperp_threshold and df.loc[i, 'funding_rate'] >= 0:
            df.loc[i, 'spot_pos'] = 1  # Open or maintain long position
        elif df.loc[i - 1, 'spot_pos'] == 1 and df.loc[i, 'funding_rate'] >= 0:
            df.loc[i, 'spot_pos'] = 1  # Continue long position
        else:
            df.loc[i, 'spot_pos'] = 0  # Close position

      # Calculate trade columns
    df['trade_perp'] = abs(df['perp_pos'].diff().fillna(0))  # Changes in perpetual futures position
    df['trade_spot'] = abs(df['spot_pos'].diff().fillna(0))  # Changes in spot position
    df['trade'] = df['trade_perp'] + df['trade_spot']       # Combined changes in positions

    # Calculate transaction costs
    df['transaction_cost'] = (df['trade_perp'] * 0.055 / 100) + (df['trade_spot'] * 0.1 / 100)

    # Initialize PnL column
    df['pnl'] = 0.0

    # Define the PnL calculation logic 
    for i in range(1, len(df)):  # Start from the second row (i = 1) since we're checking the previous row
        trade = df.loc[i, 'trade']
        transaction_cost = df.loc[i, 'transaction_cost']
        funding_rate = df.loc[i, 'funding_rate']
        
        # Get positions from the previous row
        prev_perp_pos = df.loc[i - 1, 'perp_pos']
        prev_spot_pos = df.loc[i - 1, 'spot_pos']
        
        # Get positions from the current row
        curr_perp_pos = df.loc[i, 'perp_pos']
        curr_spot_pos = df.loc[i, 'spot_pos']
        
        # Case 1: Trade > 0, previous perp_pos = -1, previous spot_pos = 1
        if trade > 0 and prev_perp_pos == -1 and prev_spot_pos == 1:
            df.loc[i, 'pnl'] = -transaction_cost + funding_rate
        
        # Case 2: Trade > 0, previous perp_pos = 0, previous spot_pos = 0
        elif trade > 0 and prev_perp_pos == 0 and prev_spot_pos == 0:
            df.loc[i, 'pnl'] = -transaction_cost
        
        # Case 3: Trade = 0, previous perp_pos = -1, previous spot_pos = 1
        elif trade == 0 and prev_perp_pos == -1 and prev_spot_pos == 1:
            df.loc[i, 'pnl'] = funding_rate
        
        # Case 4: Trade = 0, current perp_pos = 0, current spot_pos = 0
        elif trade == 0 and curr_perp_pos == 0 and curr_spot_pos == 0:
            df.loc[i, 'pnl'] = 0  # Explicitly set PnL to 0 (can be omitted since default is 0)

    
    df['cumu'] = df['pnl'].cumsum()
    df['dd'] = df['cumu'].cummax() - df['cumu']

    annual_return = round(df['pnl'].mean() * 3 * 365, 2)

    if annual_return<0.1:
        return None

    if df['pnl'].std() != 0:
        sharpe = round((df['pnl'].mean() / df['pnl'].std()) * np.sqrt(3 * 365), 2)
    else:
        sharpe = np.nan

    if sharpe < 3 : # discard low sharpe 
        return None
    
    mdd = round(df['dd'].max(), 3)

    if mdd > 0.1:
        return None
    
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
# change dataframe time period = doing walk forward


#rolling z-score walk forward


#def backtesting_model(df: pd.DataFrame, window: int, shortperp_threshold: float , plot: bool = False) -> Optional[pd.Series]:


#other models


















