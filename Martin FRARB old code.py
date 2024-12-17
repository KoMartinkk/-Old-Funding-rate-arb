import ccxt
import time
import pandas as pd
import numpy as np
import datetime
from Fetcher_fundingrateBybit import bybit_fundrate_fetcher
import os
from pprint import pprint
import requests

exchange = ccxt.bybit({
    'apiKey': 'tGnj6inSl01LwTWnZ8',  
    'secret': 'qgQdKexzvAkzYxlastwA0FAQP3Rqi4aYL16X',
    'options': {
        'adjustForTimeDifference': True,
    }
})

exchange.load_time_difference()
exchange.load_markets()

# custom function for rolling 
def check_signs(window: int):
    if all(x > 0 for x in window):
        return -1  
    elif all(x < 0 for x in window):
        return 1  
    else:
        return 0 

### trading signal ###
def signal(df: pd.DataFrame, window: int) -> int:

    df['perp_pos'] = df['funding_rate'].rolling(window).apply(check_signs)
    perp_pos = df['perp_pos'].iloc[-1]

    return int(perp_pos)

## fetch current perp position ## 
def fetch_perp_pos(coin: str) -> float:

    current_perp_pos = 0

    if exchange.fetchPosition(f'{coin}USDT')['info']['side'] == 'Buy':
        current_perp_pos = float(exchange.fetchPosition(f'{coin}USDT')['info']['size'])
        
    elif exchange.fetchPosition(f'{coin}USDT')['info']['side'] == 'Sell':
        current_perp_pos = -1 * float(exchange.fetchPosition(f'{coin}USDT')['info']['size'])

    return current_perp_pos

## fetch current spot position ## 
def fetch_spot_pos(coin: str) -> float:

    try:
        spot_pos = exchange.fetchBalance()[coin]['total']
    except KeyError:
        spot_pos = 0.0

    return spot_pos

## send tg message ##
def tg_message(message: str):

    base_url = 'https://api.telegram.org/bot6504164540:AAGeUq29zHsB6t02Q4dSKYFTbL6LJdf-ogU/sendMessage?chat_id=-4253585126&text='
    message = message

    requests.get(base_url + message)


### trade ###
def trade(coin: str, perp_signal: float, bet_size: float):
    '''
    perp_signal is trading signal of perp
    '''
    ### get spot and perp position before trade ###
    current_perp_pos = fetch_perp_pos(coin)
    current_spot_pos = fetch_spot_pos(coin)

    print(f"{coin}:")
    print('before trade')
    print(f'current {coin} perp pos', current_perp_pos)
    print(f'current {coin} spot pos', current_spot_pos)

    # current = no postition
    if current_perp_pos == 0:
        if perp_signal == -1: # if trading signal of perp is -1
            exchange.create_order(symbol = f'{coin}USDT', type = 'market', side = 'sell', amount = bet_size) #short perp
            time.sleep(1)
            exchange.create_order(symbol = f'{coin}/USDT', type = 'market', side = 'buy', amount = bet_size) #long spot
            time.sleep(1)
            order = f'{coin} liquidity: short ed {bet_size} perp and long ed {bet_size} spot'
        elif perp_signal == 1:
            exchange.create_order(symbol = f'{coin}USDT', type = 'market', side = 'buy', amount = bet_size)
            time.sleep(1)
            exchange.create_order(symbol = f'{coin}/USDT', type = 'market', side = 'sell', amount = bet_size, params = {"isLeverage": "1"})
            time.sleep(1)
            order = f'{coin} liquidity: long ed {bet_size} perp and short ed {bet_size} spot'
        else: # perp_signal == 0 
            order = f'{coin}: no liquidity'
            pass

    # current = short perp + long spot
    if current_perp_pos < 0:
        if perp_signal == 0:
            exchange.create_order(symbol = f'{coin}USDT', type = 'market', side = 'buy', amount = abs(current_perp_pos), params={'reduce_only': True})
            time.sleep(1)
            exchange.create_order(symbol = f'{coin}/USDT', type = 'market', side = 'sell', amount = current_spot_pos, params={'reduce_only': True})
            time.sleep(1)
            order = f'{coin} liquidity: closed {current_perp_pos} perp and {current_spot_pos} spot'
        elif perp_signal == -1:
            order = f'{coin}: no liquidity'
            pass

'''No Need to Long perp and Short spot'''
    # # current = long perp + short spot
    # if current_perp_pos > 0:
    #     if perp_signal == 0:
    #         exchange.create_order(symbol = f'{coin}USDT', type = 'market', side = 'sell', amount = current_perp_pos, params={'reduce_only': True})
    #         time.sleep(1)
    #         exchange.create_order(symbol = f'{coin}/USDT', type = 'market', side = 'buy', amount = abs(current_perp_pos), params = {'reduce_only': True, "isLeverage": "1"})
    #         time.sleep(1)
    #         order = f'{coin} liquidity: closed {current_perp_pos} perp and {current_spot_pos} spot'
    #     elif perp_signal == 1:
    #         order = f'{coin}: no liquidity'
    #         pass

    ### get spot and perp position after trade ###
    print('after trade')
    print(f'current {coin} perp pos', fetch_perp_pos(coin = coin))
    print(f'current {coin} spot pos', fetch_spot_pos(coin = coin))
    print("")
    print(order)
    print("")
    tg_message(order)

### param ###
btc_window = 10 # optimized windows 
eth_window = 10
sol_window = 2 
btc_bet_size = 0.001  # e.g. 0.001 btc
eth_bet_size = 0.01
sol_bet_size = 0.1

while True:

    # TODO check if any position liquidated
    # can try async

    if datetime.datetime.now(datetime.timezone.utc).hour in [0, 8, 16]:

        current_utcdatetime = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        # pick interval from 2024-08-20 00:00:00 to current utc time
        fundrates = bybit_fundrate_fetcher(coins = ['BTC', 'ETH', 'SOL'], start = '2024-08-20 00:00:00', end = current_utcdatetime)

        print(f"Funding rate arbitrage {current_utcdatetime}:")
        print("")
        tg_message(f"Funding rate arbitrage {current_utcdatetime}:")

        btc_signal = signal(df = fundrates[0], window = btc_window)
        eth_signal = signal(df = fundrates[1], window = eth_window)
        sol_signal = signal(df = fundrates[2], window = sol_window)

        trade(coin="BTC", perp_signal = btc_signal, bet_size = btc_bet_size)
        trade(coin="ETH", perp_signal = eth_signal, bet_size = eth_bet_size)
        trade(coin="SOL", perp_signal = sol_signal, bet_size = sol_bet_size)

        print('nav:', exchange.fetch_balance()['total']) # print out total balance of bybit account
        tg_message('nav: ' + str(exchange.fetch_balance()['total']))
        print("<---------------------------------------------------------->")
        tg_message("<---------------------------->")

        time.sleep(27000)