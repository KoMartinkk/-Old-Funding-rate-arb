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

# arbitrage account
session = HTTP(
    api_key="ktDiLh7gzoyUw3nbdf",
    api_secret="IVc62GgFgpF5bcQOCwsrZ8INGYlymtxV5h2v",
)

#rolling z-score model; param : window,z score threshold
# need edit

#print(bybit_fundrate_fetcher('WIF', "2020-01-01", "2023-12-21"))


def backtesting_zscore(df: pd.DataFrame, window: int, shortperp_threshold: float , plot: bool = False) -> Optional[pd.Series]:

    df['funding_ma'] = df['funding_rate'].rolling(window).mean()
    df['funding_sd'] = df['funding_rate'].rolling(window).std()
    df['funding_z'] = (df['funding_rate'] - df['funding_ma']) / df['funding_sd']

    # Identify where funding_rate > funding_z over the rolling window
    condition = df['funding_rate'].rolling(window).apply(lambda x: (x > df['funding_z']).all(), raw=False)

    # Update positions using np.where
    df['perp_pos'] = np.where(condition, -1, 0)  # Short perpetual futures
    df['spot_pos'] = np.where(condition, 1, 0)   # Long spot to offset

    # Maintain positions until funding_rate turns negative
    df['perp_pos'] = np.where((df['funding_rate'] < 0) & (df['perp_pos'] == -1), 0, df['perp_pos'])
    df['spot_pos'] = np.where((df['funding_rate'] < 0) & (df['spot_pos'] == 1), 0, df['spot_pos'])

    # Forward-fill positions to maintain the state
    df['perp_pos'] = df['perp_pos'].replace(0, np.nan).ffill().fillna(0)
    df['spot_pos'] = df['spot_pos'].replace(0, np.nan).ffill().fillna(0)

    # Calculate trades based on changes in positions
    df['trade'] = abs(df['perp_pos'].diff().fillna(0)) + abs(df['spot_pos'].diff().fillna(0))

    # Calculate PnL based on funding_rate collection logic
    df['pnl'] = 0
    summing = False
    pnl_sum = 0

    for i in range(len(df)):
        if df['trade'].iloc[i] > 0:  # Deduct transaction cost when trades occur
            pnl_sum -= df['trade'].iloc[i] * 0.05 / 100

        if df['perp_pos'].iloc[i] == -1 and df['spot_pos'].iloc[i] == 1 and not summing:
            # Start summing funding rates when positions are set
            summing = True
            pnl_sum = 0
        if summing:
            pnl_sum += df['funding_rate'].iloc[i]
            df.loc[i, 'pnl'] = pnl_sum
        if df['perp_pos'].iloc[i] == 0 and df['spot_pos'].iloc[i] == 0 and summing:
            # Stop summing when positions revert to 0
            summing = False

    
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

    return pd.Series([window, sharpe, calmar, annual_return, mdd],index= ['window', 'sharpe', 'calmar', 'annual_return', 'mdd'])



#def walkforward_zscore(df: pd.DataFrame, window: int, plot: bool = False) -> Optional[pd.Series]:




#print(backtesting())

#other model


















