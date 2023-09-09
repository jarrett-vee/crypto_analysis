import ccxt
import psycopg2
from datetime import datetime
import time

# --- Step 2: Set up CCXT and BinanceUS instance ---

exchange = ccxt.binanceus(
    {
        "rateLimit": 1200,
        "timeout": 30000,
        "enableRateLimit": True,
    }
)

symbols = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "XRP/USDT", "ADA/USDT"]

# --- Step 3: Fetch historical data ---


def convert_timestamp(timestamp_in_millis):
    timestamp_in_seconds = timestamp_in_millis / 1000
    return datetime.utcfromtimestamp(timestamp_in_seconds).strftime("%Y-%m-%d %H:%M:%S")


def fetch_historical_data(symbol):
    start_time = time.time()

    print(f"Fetching data for {symbol}...")

    # Start date: January 1st, 2022
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

# --- Step 4: Connect to PostgreSQL and store the data ---

DATABASE_URL = "postgresql://postgres:admin@localhost:5432/crypto_data"


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


# Call functions
create_table()
for symbol, data in historical_data.items():
    insert_data(symbol, data)
