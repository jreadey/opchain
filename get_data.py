
import sys
import os
import opchain
 

def get_data(stocklist_file):
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
            print(symbol)
            df = opchain.get_dataframe(symbol, reload=True)
            if df is None or len(df) == 0:
                print("unable to get data for {symbol}")
                failcount += 1
                if failcount == 10:
                    print("too many failures, quitting")
                    sys.exit()
            else:
                print(f"got {len(df)} rows for symbol {symbol}")
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

for file_list in sys.argv:
    if file_list.endswith(".py"):
        continue
    get_data(file_list)

print('done!')