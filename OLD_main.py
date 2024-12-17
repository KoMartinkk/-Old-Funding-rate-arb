from datetime import datetime, timezone
from external_functions import bybit_fundrate_fetchers, tg_message
from trades import signals, trades
import time

tokens = ["WIF", "LINK", "ETH", "SOL"]
token_bets = {"WIF": 60, "LINK": 9, "ETH": 0.04, "SOL": 0.7} # bets in qty. ensure that bets should not be too small
token_params = {"WIF": 6, "LINK": 22, "ETH": 20, "SOL": 6} # optimized windows

## manually set leverages!!!

def main():

    while True:

        current_utc = datetime.now(timezone.utc)

        if current_utc.strftime("%H:%M:%S") in ["00:00:00", "08:00:00", "16:00:00"]:

            tg_message(current_utc.strftime('%Y-%m-%d %H:%M:%S'))
            token_fundrates = bybit_fundrate_fetchers(tokens, "2024-08-01 00:00:00", current_utc.strftime('%Y-%m-%d %H:%M:%S'))
            token_signals = signals(token_params, token_fundrates)
            trades(token_bets, token_signals)

            time.sleep(1)

if __name__ == "__main__":
    main()