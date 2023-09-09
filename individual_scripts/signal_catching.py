import pandas as pd
from sqlalchemy import create_engine
import numpy as np

DATABASE_URL = "postgresql://postgres:admin@localhost:5432/crypto_data"
engine = create_engine(DATABASE_URL)


def get_ta_data(symbol):
    df = pd.read_sql(
        f"SELECT * FROM ta_data WHERE symbol='{symbol}' ORDER BY timestamp ASC",
        engine,
    )
    return df


def generate_signals(df):
    signals = pd.DataFrame(index=df.index)
    signals["price"] = df["close"]
    signals["date"] = pd.to_datetime(df["timestamp"], unit="ms")
    signals["symbol"] = df["symbol"]

    # SMA Strategy
    signals["short_mavg"] = df["sma_10"]
    signals["long_mavg"] = df["sma_50"]
    signals["sma_signal"] = np.where(
        signals["short_mavg"] > signals["long_mavg"],
        1,
        np.where(signals["short_mavg"] < signals["long_mavg"], -1, 0),
    )

    # Bollinger Bands Strategy
    signals["bb_signal"] = np.where(
        (df["close"] < df["bb_bbl"]) & (df["rsi"] < 30),
        1,
        np.where((df["close"] > df["bb_bbh"]) & (df["rsi"] > 70), -1, 0),
    )

    # MACD Strategy
    signals["macd_signal_line_cross"] = np.where(df["macd"] > df["macd_signal"], 1, -1)

    # Composite signal
    signals["composite_signal"] = (
        signals["sma_signal"] + signals["bb_signal"] + signals["macd_signal_line_cross"]
    )
    signals["buy_signal"] = signals["composite_signal"] > 1
    signals["sell_signal"] = signals["composite_signal"] < -1

    # Adding reasons
    signals["reason"] = np.where(
        signals["buy_signal"],
        "Multiple positive indicators",
        np.where(signals["sell_signal"], "Multiple negative indicators", ""),
    )

    return signals


def save_signals_to_database(df, table_name):
    df.to_sql(table_name, engine, if_exists="append", index=False)


if __name__ == "__main__":
    symbols = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "XRP/USDT", "ADA/USDT"]

    for symbol in symbols:
        df = get_ta_data(symbol)
        signals = generate_signals(df)

        # Filter rows with buy/sell signals
        buy_signals = signals[signals["buy_signal"]]
        sell_signals = signals[signals["sell_signal"]]

        # Save to the database
        save_signals_to_database(buy_signals, "buy_signals")
        save_signals_to_database(sell_signals, "sell_signals")

        print(f"Saved signals for {symbol} to the database.")
