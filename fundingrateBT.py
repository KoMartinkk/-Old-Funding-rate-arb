from pybit.unified_trading import HTTP
from pybit.exceptions import InvalidRequestError
import time
import pandas as pd
import numpy as np
import datetime
from tools import check_signs, perp_names,tg_message, bybit_fundrate_fetcher,datetime_to_unix_converter
import os
from pprint import pprint
import requests
from typing import Optional

# arbitrage account
session = HTTP(
    api_key="ktDiLh7gzoyUw3nbdf",
    api_secret="IVc62GgFgpF5bcQOCwsrZ8INGYlymtxV5h2v",
)


#rolling z-score model

# need edit

def backtesting(df: pd.DataFrame, window: int, plot: bool = False) -> Optional[pd.Series]:

    df = df.copy()
    df['perp_pos'] = df['funding_rate'].rolling(window).apply(check_signs).shift(1).fillna(0)
    df['perp_pos_t+1'] = df['perp_pos'].shift(-1)
    df['trade'] = np.where(
                    df['perp_pos_t+1'].isna() & df['perp_pos'].notna(), # if previous day is nan + today is not nan => we do two trades (long/short perp + short/long spot)
                    2,
                    np.where(
                        df['perp_pos_t+1'].notna() & df['perp_pos'].notna(), # previous day and today are both not nan
                        abs(df['perp_pos_t+1'] - df['perp_pos']) * 2, # check if pervious day and today have different positions
                                                                      # e.g. if previous day is -1 & today is 0 => we do two trades as closing short perp + long spot positions
                        np.nan # if previous day and today are both nan, trade is nan. This happens to days in initial window as we start rolling()
                    )
                )

    df['pnl'] =  - (df['funding_rate'] * df['perp_pos']) - df['trade']*0.05/100
    df['cumu'] = df['pnl'].cumsum()
    df['dd'] = df['cumu'].cummax() - df['cumu']

    annual_return = round(df['pnl'].mean() * 3 * 365, 2)

    if df['pnl'].std() != 0:
        sharpe = round((df['pnl'].mean() / df['pnl'].std()) * np.sqrt(3 * 365), 2)
    else:
        sharpe = np.nan

    mdd = round(df['dd'].max(), 5)

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
























