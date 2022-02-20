
from zlib import Z_BEST_COMPRESSION
import sqlalchemy as sq
import sqlite3
import pandas as pd
import yfinance as yf

conn = sqlite3.connect('intradaystocks.db')
cursor = conn.cursor()
# cursor.execute("DROP TABLE AMD")


tickersymb = "AMD"
engine = sq.create_engine("sqlite:///intradaystocks.db")

df = yf.download(tickersymb, start = "2021-01-01")

Dates = []
Key = []
LocalIndex = []
Symb = []
TF = [] #set timeframe let A mean daily
for x in range(len(df)):
    Date = df.iloc[x].name
    Dates.append(Date)
    LocalIndex.append(x)
    Symb.append(tickersymb)
    TF.append("A")
    Date = str(Date)
    Key.append(10000*int(Date[0:4]) + 100*int(Date[5:7]) + int(Date[8:10]))

# set rounding rules #
#df['Open'].round(decimals=3)
#
df["Key"] = Key
df["Datetime"] = Dates
df["Lindex"] = LocalIndex
df["Symb"] = Symb
df["TF"] = TF     #TF means timeframe
df = df[['Lindex', "Symb", "TF",'Datetime', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume', 'Key']]
# print(df.columns)

#df.to_sql('AMD2', engine)

query = """ 
SELECT Symb, Datetime, TF, count(*) from engine.AMD
GROUP BY FirstName, LastName
"""

z = pd.read_sql("""SELECT * FROM AMD2 where TF = 'A' """, engine)
print(z)

# z = pd.read_sql('AMD', engine)
# print("Printing*******************************")
# print(z)
