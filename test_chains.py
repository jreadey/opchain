import logging
import sys
import opchain


loglevel = logging.INFO
symbol = 'TSLA'
logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)
logging.info(f" loglevel: {loglevel}")
"""
#data = opchain.get_chains(symbol)
#print(f"got {len(data.keys())} keys")
df = opchain.get_dataframe(symbol, putCall="PUT")
if df is None:
    print("no dataframe returned!")
    sys.exit(1)
print(f"got df with: {len(df)} rows")
for k in df.attrs:
    v = df.attrs[k]
    print(f"df attr {k}: {v}")

days = df['daysToExpiration']
unique_days = list(set(list(days.values)))
unique_days.sort()
print(f"unique days: {unique_days}")
daysToExpiration=45
df = opchain.get_dataframe(symbol, putCall="PUT", daysToExpiration=daysToExpiration) 
if df is None:
    print("no dataframe returned!")
    sys.exit(1)
print(f"got df with: {len(df)} rows for daysToExpiration={daysToExpiration}")

candidates = opchain.get_candidates(df)
print(f"got {len(candidates)} candidates")
for row in candidates.itertuples():
    row_dict = row._asdict()
    for k in row_dict:
        v = row_dict[k]
        print(f"{k}: {v}")

print("e_u's..")
for row in candidates.itertuples():
    print(f"row {row.Index}: {row.e_u:.3f}")
"""

"""
symbol = 'TSLA'
df = opchain.get_dataframe(symbol, putCall="PUT", daysToExpiration=18)
df = df.sort_values(by='strikePrice')
print(f"got {len(df)} contracts")
candidates = opchain.get_candidates(df, buy_range=(.09, .38), sell_range=(.2, .45))
print(f"got {len(candidates)} candidates")
"""
symbol = "$NDX.X"
logging.debug(f"symbol: {symbol}")
#df = opchain.get_dataframe(symbol, putCall="CALL", run_date="2021-04-08", daysToExpiration=45)
df = opchain.get_dataframe(symbol, putCall="CALL") #, run_date="2021-04-08")  #, daysToExpiration=45)
print(f"got {len(df)} option rows")
print("mmm:", df.attrs["mmm"])
days = df['daysToExpiration']
days = list(set(list(days.values)))
days.sort()
print(days)

candidates = opchain.get_candidates(df, daysToExpiration=24)
print(f"got {len(candidates)} candidate rows")
width = candidates['width']
print(f"min width: {width.min()}")
print(f"max width: {width.max()}")
print(candidates.columns)
print("candidate attributes")
for k in candidates.attrs:
    v = candidates.attrs[k]
    print(f"{k}: {v}")


