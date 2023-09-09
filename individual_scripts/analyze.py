import pandas as pd
from sqlalchemy import create_engine
from ta import add_all_ta_features
from ta.utils import dropna
from ta.volatility import BollingerBands
from ta.momentum import RSIIndicator
from ta.trend import MACD, SMAIndicator

DATABASE_URL = "postgresql://postgres:admin@localhost:5432/crypto_data"
engine = create_engine(DATABASE_URL)


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


def save_to_database(df, table_name):
    print(f"Saving data for {df['symbol'].iloc[0]}")
    df.to_sql(table_name, engine, if_exists="append", index=False)
    print(f"Saved data for {df['symbol'].iloc[0]}")


if __name__ == "__main__":
    symbols = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "XRP/USDT", "ADA/USDT"]

    for symbol in symbols:
        df = get_data(symbol)
        print(f"Got data for {symbol}")
        df_with_indicators = add_indicators(df)
        save_to_database(df_with_indicators, "ta_data")
