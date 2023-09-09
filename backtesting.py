import pandas as pd
from sqlalchemy import create_engine

DATABASE_URL = "postgresql://postgres:admin@localhost:5432/crypto_data"
engine = create_engine(DATABASE_URL)


def backtest_strategy(symbol, initial_capital, investment_fraction):
    # Load signals from the database
    buy_signals = pd.read_sql(
        "SELECT * FROM buy_signals WHERE symbol = '{}'".format(symbol), engine
    )
    sell_signals = pd.read_sql(
        "SELECT * FROM sell_signals WHERE symbol = '{}'".format(symbol), engine
    )

    buy_signals = buy_signals.sort_values(by="date")
    sell_signals = sell_signals.sort_values(by="date")

    cash = initial_capital
    position = 0

    for _, row in buy_signals.iterrows():
        # Calculate the amount to invest in this trade
        trade_investment = cash * investment_fraction
        # Buying as much as the trade investment allows
        units_to_buy = trade_investment // row["price"]
        cash -= units_to_buy * row["price"]
        position += units_to_buy

    for _, row in sell_signals.iterrows():
        # Selling all our position from the respective buy signal
        cash += position * row["price"]
        position = 0

    # Final value of our portfolio
    total_value = cash + (
        position * (buy_signals["price"].iloc[-1] if position > 0 else 0)
    )
    return total_value


if __name__ == "__main__":
    symbols = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "XRP/USDT", "ADA/USDT"]
    initial_capital = 50000
    investment_fraction = 0.4

    for symbol in symbols:
        final_value = backtest_strategy(symbol, initial_capital, investment_fraction)
        print(
            f"Backtest for {symbol}: Initial Capital: {initial_capital}, Final Portfolio Value: {final_value:.2f}"
        )
