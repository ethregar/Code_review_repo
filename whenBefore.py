import pandas as pd
import numpy as np
import datetime as date_time

import yfinance as yf

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base


#initializing engine
Base = declarative_base()
db_password = "admin"  # This is NOT safe
engine = create_engine('postgresql://postgres:{}@localhost/stockData'.format(db_password))
Base.metadata.create_all(bind=engine)


#WARNING, Remove quotation marks with caution
#One time ONLY parse to create the tables on the database or second time if something goes wrong
"""
from create_DB import create_db
create_db(engine)
"""


def stock_to_db(df, flag=1):
    """inserts df into the DB"""

    # insert statement
    insert_part1 = "INSERT INTO daily_prices (symbol, date, volume, open, close, high, low, adjclose) VALUES"

    try:
        symbol = df['symbol'].iloc[0]
    except:
        return 0
    # the values (python 3.5, I need to remake it with f'string')
    inesrt_part2 = ",".join(["""('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')""".format(
        symbol, row.date, row.volume, row.open, row.close, row.high, row.low, row.adjclose
    ) for indx, row in df.iterrows()])

    # protection from duplicate entries on the same date (does not apply for minute chart)
    if flag == 1:
        inesrt_part3 = """ON CONFLICT (symbol, date)
                    DO UPDATE 
                    SET
                    volume = EXCLUDED.volume,
                    open = EXCLUDED.open,
                    close = EXCLUDED.close,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    adjclose = EXCLUDED.adjclose;
                    """
    else:
        inesrt_part3 = ";"

    # executes query
    query = insert_part1 + inesrt_part2 + inesrt_part3

    engine.execute(query)


def updatedb(ticker, df_for_base, flag):
    """Formats data for the query"""
    # formating multyindex dataframe to remove NaNs but avoid removing unmatching dates between the two tickets

    df = df_for_base.xs(ticker, axis=1, level=1)
    df = df.dropna()

    df.reset_index(level=0, inplace=True)

    # transformations to match the tables
    df.columns = df.columns.str.lower()
    df.rename(columns={'adj close': 'adjclose'}, inplace=True)
    df = df.reindex(columns=
                    ['symbol', 'date', 'volume', 'open', 'close', 'high', 'low', 'adjclose'])
    df['symbol'] = ticker

    stock_to_db(df, flag)


def ifnot_updated(ticker):
    """Checks the database for the last time a selected ticker was updated.
    If the date defers from the last avaiable, DB gets updated with the latest information"""

    # checks if the assets exists in DB and/or it's latest update
    # flag = 1: asset exists in DB
    flag = 1
    try:
        last_update = \
        pd.read_sql_query(f"SELECT date FROM daily_prices WHERE SYMBOL='{ticker}' ORDER BY date DESC LIMIT 1;",
                          engine).loc[0][0]
        if date_time.date.today() == last_update:
            return 0
    except KeyError:
        last_update = None
        flag = 0

    tickers = ["AAME", ticker]  # default ticker to keep the last update of DB in some asset
    tickersstring = " ".join(tickers)

    # download updates and loop them in updating the database
    df_for_base = yf.download(tickersstring, start=last_update)

    # first to update the last date DB was updated, second to update the asset we request
    updatedb("AAME", df_for_base, flag=1)
    updatedb(ticker, df_for_base, flag)


def whenbefore(ticker, late_days, prc):
    """Examines when was the last time that an asset demostrated similar graph moevements
    for a set number of days with a possible set error (plus/minus) for each day
    (can be expanded to other kind of graphs)"""

    ifnot_updated(ticker)  # updates local database to the last day

    # querries DB for the close price and converts to PERCENTAGE CHANGE of asset price
    df = pd.read_sql_query(f"SELECT close,date FROM daily_prices WHERE SYMBOL='{ticker}';", engine, index_col=['date'])
    df = np.log(df.close / df.close.shift(1)).to_frame()
    df.dropna(inplace=True)

    # how many of the late day movements are being compared to history
    dftemp = df[-late_days:]
    result = pd.concat([dftemp - prc, dftemp, dftemp + prc], axis=1, join="inner")
    result.columns = ["low", "close", "high"]

    index_pairs = []

    # scans the loaded frame day[+days forward] by day for the condition
    for i in range(len(df) - late_days + 1):
        count = 0
        for y in range(late_days):
            if result["low"][y] < df["close"][i + y] < result["high"][y]: count += 1

        # if the final match exceed 90% success, the result is saved
        if count > late_days * 0.90:
            index_pairs.append((df['close'].index[i], df['close'].index[i + y]))

    index_pairs.pop(-1)
    # returns the pairs
    return index_pairs, df


# bionano genomics, repeating the movements of the last 5 days
# with an error of 0.02% plus minus for each day
option = 'BNGO'
days = 5
prc_error = 0.02
index_pairs, df = whenbefore(option, days, prc_error)
print(index_pairs)
