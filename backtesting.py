import pandas as pd
from sqlalchemy import create_engine
from config import symbols, DATABASE_URL

engine = create_engine(DATABASE_URL)


def process_signal(signal_df, is_buying, cash, position, investment_fraction):
    for _, row in signal_df.iterrows():
        if is_buying:
            trade_investment = cash * investment_fraction
            units_to_buy = trade_investment // row["price"]
            cash -= units_to_buy * row["price"]
            position += units_to_buy
        else:
            cash += position * row["price"]
            position = 0
    return cash, position


def backtest_strategy(symbol, initial_capital, investment_fraction):
    # Load signals from the database
    buy_signals = pd.read_sql(
        f"SELECT * FROM buy_signals WHERE symbol = '{symbol}'", engine
    ).sort_values(by="date")

    sell_signals = pd.read_sql(
        f"SELECT * FROM sell_signals WHERE symbol = '{symbol}'", engine
    ).sort_values(by="date")

    cash = initial_capital
    position = 0

    cash, position = process_signal(
        buy_signals, True, cash, position, investment_fraction
    )
    cash, position = process_signal(
        sell_signals, False, cash, position, investment_fraction
    )

    # Final value of our portfolio
    total_value = cash + (
        position * (buy_signals["price"].iloc[-1] if position > 0 else 0)
    )
    return total_value


if __name__ == "__main__":
    initial_capital = 50000
    investment_fraction = 0.4

    for symbol in symbols:
        final_value = backtest_strategy(symbol, initial_capital, investment_fraction)
        print(
            f"Backtest for {symbol}: Initial Capital: {initial_capital}, Final Portfolio Value: {final_value:.2f}"
        )
