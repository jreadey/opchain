
import sys
import os
import pandas as pd
import opchain

DAYS_TO_EXPIRATION=45
PUTCALL="PUT"

def getBestEu(df):
    row = {}
    candidates = opchain.get_candidates(df)
    if candidates is not None and len(candidates) > 0:
        candidates = candidates.sort_values(by="e_u", ascending=False)
        candidate = candidates.iloc[0]
        
        for k in candidates.attrs:
            v = candidates.attrs[k]
            row[k] = v
        for k in candidate.keys():
            v = candidate[k]
            row[k] = v
    return row
 


def getBestEus(stocklist_file, rows):
    if not os.path.isfile(stocklist_file):
        print(f"{stocklist_file} not found")
        sys.exit(1)

    symbols = []
    failcount = 0
    with open(stocklist_file, "r") as f:
        line = f.readline()
        while line:
            fields = line.strip().split(',')
            if not fields:
                line = f.readline()
                continue
            symbol = fields[0]
            if not symbol.isupper():
                print(f"ignoring symbol: {symbol}")
                line = f.readline()
                continue
            df = opchain.get_dataframe(symbol, putCall=PUTCALL, daysToExpiration=DAYS_TO_EXPIRATION)
            if df is None or len(df) == 0:
                print("unable to get data for {symbol}")
                failcount += 1
                if failcount == 3:
                    print("too many failures, quitting")
                    sys.exit()
            else:
                row = getBestEu(df)
                if row:
                    rows.append(row)
                symbols.append(symbol)
                failcount = 0  # reset
         
            line = f.readline()
        
    print(f"got data for {len(symbols)} symbols from file: {stocklist_file}")


#
# main
#
if len(sys.argv) < 2 or sys.argv[1] in ('-h', '--help'):
    print("usage: python get_all.py  [stocklist_file1] [stocklist_file2]")
    sys.exit(0)

rows = []

for file_list in sys.argv:
    if file_list.endswith(".py"):
        continue
    getBestEus(file_list, rows)
     
if not rows:
    print("no rows found!")
row = rows[0]
columns = list(row.keys())
df = pd.DataFrame(rows, columns=columns)
output = df.to_csv()
print(output)
