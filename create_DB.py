import yfinance as yf

#default ticker download and prepare to create DB table
def create_db(engine):
    df = yf.download("AAME")
    df.reset_index(level=0, inplace=True)
    df.columns= df.columns.str.lower() #lowercase cause tables
    df.rename(columns={'adj close': 'adjclose'},inplace=True) #prevents disagreements with space of SQL and ORM
    df = df.reindex(columns=['symbol', 'date', 'volume', 'open', 'close', 'high', 'low', 'adjclose'])
    df['symbol']="AAME"

    # Create a SQL table directly from a dataframe, first itteration with preferably 1 ticker
    df.volume = df.volume.astype(float) #IMPORTANT, later for SQL not to error us with wrong type (double vs bigint)

    # Write the data into the database, this is so fucking cool
    df.to_sql('daily_prices', engine, if_exists='replace', index=False)

    # Create a primary key on the table
    query = """ALTER TABLE daily_prices 
                ADD PRIMARY KEY (symbol, date);"""
    engine.execute(query)  
   

