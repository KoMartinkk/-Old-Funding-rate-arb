import pandas as pd
from pybit.unified_trading import HTTP
from pybit.exceptions import InvalidRequestError
from external_functions import tg_message, check_signs, round_down, round_up

# arbitrage account
session = HTTP(
    api_key="ktDiLh7gzoyUw3nbdf",
    api_secret="IVc62GgFgpF5bcQOCwsrZ8INGYlymtxV5h2v",
)

## signals ## 
def signals(token_params: dict[str, int], token_fundrates: dict[str, pd.DataFrame]) -> dict[str, int]:

    """
    Generate signals (1/0/-1) of perpetual futures of corresponding tokens.

    Parameters:
        token_params : dict[str, int]
            A dictionary with token identifiers as keys and optimized rolling window sizes as values.

        token_fundrates : dict[str, pd.DataFrame]
            A dictionary with token identifiers as keys and DataFrames containing funding rates as values.

    Returns:
        dict[str, int]
            A dictionary with token identifiers as keys and signals as values. 
    """

    token_signals = {}

    for token, fundrate in token_fundrates.items():
        perp_pos = fundrate["funding_rate"].rolling(token_params[token]).apply(check_signs)
        token_signals[token] = int(perp_pos.iloc[-1])

    return token_signals

## fetch info ##
def fetch_perp(symbol: str) -> float:

    """
    Fetch the real-time and current position of perpetural futures given a symbol, e.g., BTCUSDT & DOGEUSDT. 
    """

    get_position = session.get_positions(category = "linear", symbol = symbol)["result"]["list"][0]

    if get_position["side"] == "Buy":
        pos = float(get_position["size"])
    elif get_position["side"] == "Sell":
        pos = -1 * float(get_position["size"])
    else:
        pos = 0.0

    return pos

def fetch_spot(token: str) -> float:

    """
    Fetch the real-time and current equity of a token, e.g., BTC & SOL. 
    """

    return float(session.get_wallet_balance(accountType = "UNIFIED", coin = token)["result"]["list"][0]["coin"][0]["equity"])

def fetch_balance() -> float:

    """
    Fetch the total value of the wallet.
    """

    balance = float(session.get_wallet_balance(accountType="UNIFIED")["result"]["list"][0]["totalEquity"])

    return round(balance, 2)

## trade ##
def trades(token_bets: dict[str, float], token_signals: dict[str, int]):

    """
    Place order on Bybit exchange given token bets (qty) and signals of perpetual futures for funding rate arbitrage

    Parameters:
        token_bets: dict[str, float]
            A dictionary with token identifiers as keys and bets as values.

        token_signals: dict[str, int]
            A dictionary with token identifiers as keys and signals of perpetual futures as values.
    """

    for token, signal in token_signals.items():

        token_sym = f"{token}USDT"
        token_bet = token_bets[token]

        # no current perp pos
        if fetch_perp(token_sym) == 0:
            
            # long spot & short perp
            if signal == -1:
                try: 
                    session.place_order(category = "spot", symbol = token_sym, side = "Buy", orderType = "Market", 
                            qty = str(token_bet), marketUnit = "baseCoin") # marketUnit => order by qty (default order by value in UTA Spot account)
                    session.place_order(category = "linear", symbol = token_sym, side = "Sell", orderType = "Market", qty = str(token_bet))
                except InvalidRequestError:
                    tg_message(f"{token}: Not enough balance")

            # short spot & long perp
            elif signal == +1:
                try: 
                    session.place_order(category = "spot", symbol = token_sym, side = "Sell", orderType = "Market", isLeverage = 1,
                            qty = str(token_bet), marketUnit = "baseCoin")
                    session.place_order(category = "linear", symbol = token_sym, side = "Buy", orderType = "Market", qty = str(token_bet))
                except InvalidRequestError:
                    tg_message(f"{token}: Not enough balance")
            
            elif signal == 0:
                pass

        # +ve curent perp pos
        elif fetch_perp(token_sym) == token_bet:

            # close all position
            if signal == 0:
                buy_qty = round_up(abs(fetch_spot(token)), 2) # round up 2 decimals. E.g. -3.172 SOL => buy_qty is 3.18 SOL 
                try: 
                    session.place_order(category = "spot", symbol = token_sym, side = "Buy", orderType = "Market", 
                            qty = buy_qty, marketUnit = "baseCoin") 
                    session.place_order(category = "linear", symbol = token_sym, side = "Sell", orderType = "Market", qty = str(token_bet))
                except InvalidRequestError:
                    tg_message(f"{token}: Not enough balance")

            elif signal == +1:
                pass

        # -ve curent perp pos
        elif fetch_perp(token_sym) == -token_bet:

            # close all position
            if signal == 0:
                sell_qty = round_down(abs(fetch_spot(token)), 2) # round down 2 decimals. E.g. +3.176 SOL => sell_qty is 3.17 SOL 
                try: 
                    session.place_order(category = "spot", symbol = token_sym, side = "Sell", orderType = "Market", 
                            qty = sell_qty, marketUnit = "baseCoin") 
                    session.place_order(category = "linear", symbol = token_sym, side = "Buy", orderType = "Market", qty = str(token_bet))
                except InvalidRequestError:
                    tg_message(f"{token}: Not enough balance")

            elif signal == -1:
                pass

        if fetch_perp(token_sym) == 0:
            tg_message(f"{token}: no holding")
        elif fetch_perp(token_sym) > 0:
            tg_message(f"{token}: short {fetch_perp(token_sym)} spot + long {fetch_perp(token_sym)} perp")
        elif fetch_perp(token_sym) < 0:
            tg_message(f"{token}: long {-fetch_perp(token_sym)} spot + short {-fetch_perp(token_sym)} perp")
            
    tg_message(f"Arbitrage account's portfolio value: {fetch_balance()} USDT")