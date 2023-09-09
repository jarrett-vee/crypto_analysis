import ccxt
import psycopg2
from datetime import datetime
import time
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from ta import add_all_ta_features
from ta.utils import dropna
from ta.volatility import BollingerBands
from ta.momentum import RSIIndicator
from ta.trend import MACD, SMAIndicator

DATABASE_URL = "postgresql://postgres:admin@localhost:5432/crypto_data"
engine = create_engine(DATABASE_URL)

exchange = ccxt.binanceus(
    {
        "rateLimit": 1200,
        "timeout": 30000,
        "enableRateLimit": True,
    }
)

symbols = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "XRP/USDT", "ADA/USDT"]


def convert_timestamp(timestamp_in_millis):
    timestamp_in_seconds = timestamp_in_millis / 1000
    return datetime.utcfromtimestamp(timestamp_in_seconds).strftime("%Y-%m-%d %H:%M:%S")


def fetch_historical_data(symbol):
    start_time = time.time()

    print(f"Fetching data for {symbol}...")

    # Start date: September 1st, 2018
    since = exchange.parse8601("2018-09-01T00:00:00Z")

    # End date: September 1st, 2023
    end_date = exchange.parse8601("2023-09-01T00:00:00Z")

    limit = 1000
    ohlcv = []

    while True:
        print(f"Fetching from timestamp {since}...")
        try:
            candle_data = exchange.fetch_ohlcv(symbol, "30m", since, limit)
            if len(candle_data) < 1:
                print(f"No more data for {symbol} after timestamp {since}.")
                break

            last_candle_timestamp = candle_data[-1][0]
            if last_candle_timestamp is None:
                print(
                    f"Error: Unable to parse timestamp {candle_data[-1][0]} for {symbol}."
                )
                break
            if last_candle_timestamp >= end_date:
                print(
                    f"Reached or surpassed the end date with {symbol}. Stopping data fetch."
                )
                ohlcv += [candle for candle in candle_data if candle[0] < end_date]
                break

            ohlcv += candle_data
            print(f"Fetched {len(candle_data)} data points for {symbol}.")
            since = last_candle_timestamp + 1

        except Exception as e:
            print(f"Error fetching data for {symbol} due to {e}")
            break

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Fetching data for {symbol} took {elapsed_time:.2f} seconds.")
    return ohlcv


historical_data = {symbol: fetch_historical_data(symbol) for symbol in symbols}


def create_table():
    print("Connecting to database...")
    connection = psycopg2.connect(DATABASE_URL)
    cursor = connection.cursor()
    print("Creating table if not exists...")
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS crypto_data (
        id SERIAL PRIMARY KEY,
        symbol TEXT NOT NULL,
        timestamp BIGINT NOT NULL,
        open DECIMAL(20,10) NOT NULL,
        high DECIMAL(20,10) NOT NULL,
        low DECIMAL(20,10) NOT NULL,
        close DECIMAL(20,10) NOT NULL,
        volume DECIMAL(20,10) NOT NULL
    );
    """
    )
    connection.commit()
    connection.close()
    print("Table creation/check complete.")


def insert_data(symbol, data):
    start_time = time.time()

    print(f"Inserting data for {symbol}...")
    connection = psycopg2.connect(DATABASE_URL)
    cursor = connection.cursor()
    for index, candle in enumerate(data, 1):
        cursor.execute(
            """
        INSERT INTO crypto_data (symbol, timestamp, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
            (symbol, candle[0], candle[1], candle[2], candle[3], candle[4], candle[5]),
        )
        if index % 1000 == 0:
            print(f"Inserted {index} data points for {symbol}.")

    connection.commit()
    connection.close()

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Inserting data for {symbol} took {elapsed_time:.2f} seconds.")


create_table()
for symbol, data in historical_data.items():
    insert_data(symbol, data)

# technical indicators


def get_data(symbol):
    df = pd.read_sql(
        f"SELECT * FROM crypto_data WHERE symbol='{symbol}' ORDER BY timestamp ASC",
        engine,
    )
    return df


def add_indicators(df):
    print(f"Adding indicators for {df['symbol'].iloc[0]}")
    # Add Bollinger Bands
    bollinger = BollingerBands(df["close"])
    df["bb_bbm"] = bollinger.bollinger_mavg()
    df["bb_bbh"] = bollinger.bollinger_hband()
    df["bb_bbl"] = bollinger.bollinger_lband()

    # Add RSI
    df["rsi"] = RSIIndicator(df["close"]).rsi()

    # Add MACD
    macd = MACD(df["close"])
    df["macd"] = macd.macd()
    df["macd_signal"] = macd.macd_signal()
    df["macd_diff"] = macd.macd_diff()

    # SMAs
    df["sma_10"] = SMAIndicator(df["close"], 10).sma_indicator()
    df["sma_50"] = SMAIndicator(df["close"], 50).sma_indicator()
    df["sma_200"] = SMAIndicator(df["close"], 200).sma_indicator()

    print(f"Finished adding indicators for {df['symbol'].iloc[0]}")
    return df


for symbol in symbols:
    df = get_data(symbol)
    print(f"Got data for {symbol}")
    df_with_indicators = add_indicators(df)
    df_with_indicators.to_sql("ta_data", engine, if_exists="append", index=False)

# generate trading signals


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


for symbol in symbols:
    df = get_ta_data(symbol)
    signals = generate_signals(df)

    # Filter rows with buy/sell signals
    buy_signals = signals[signals["buy_signal"]]
    sell_signals = signals[signals["sell_signal"]]

    # Save to the database
    buy_signals.to_sql("buy_signals", engine, if_exists="append", index=False)
    sell_signals.to_sql("sell_signals", engine, if_exists="append", index=False)

    print(f"Saved signals for {symbol} to the database.")
