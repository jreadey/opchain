import sys
import os
import logging
import pandas as pd
import opchain
import time

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

MIN_DMU = 0.0
MIN_DMU2 = -8.0
MIN_DME_U = -999.0
MIN_MG = 0.5
ODD_DAY_SYMBOLS = ["DJI.C", "DIA", "$SPX.X", "SPY", "$NDX.X", "QQQ", "$RUT.X", "IWM", "$VIX.X", "VXX", "EWZ"] #brazil
MIN_DAY = 40
MAX_DAY = 80
MIN_EW = -0.5
MIN_DME_W = 0.0
NUM_EWS = 100


BEST_EW_COLUMNS = ["symbol",
                   #"underlyingPrice",
                   "exp_date",
                   "days_exp",
                   "putcall",
                   "s_strikePrice",
                   "b_strikePrice",
                   # "s_last",
                   # "b_last",
                   "s_delta",
                   "b_delta",
                   #"mtp",
                   "mg",
                   "eml",
                   #"dme",
                   #"DME_u",
                   "dme_w",
                   "e_w",
                   "mg_w",
                   "pop",
                   # "popt",
                   "width",
                   #"e_u",
                   #"eml_u",
                   #"ml_u",
                   #"mg_u",
                   "e",
                   "ml",
                   "mmm",
                   "dmu",
                   "dmu2",
                   "s_description",
                   "b_description"
                   ]
# removed     "mmm2","dm","dm2",
RENAME_COLUMNS = {"underlyingPrice": "underlying", 
                  "s_strikePrice": "s_strike", 
                  "b_strikePrice": "b_strike",
                  "days_exp": "days"}

def getMMM(candidates, target):
    mmm = 0.0
    if target is None:
        target = 7
    if "mmm" in candidates.attrs and candidates.attrs["mmm"]:
        mmm_map = candidates.attrs["mmm"]
        mmm_diff = 999
        for day in mmm_map:
            if abs(day - target) < mmm_diff:
                mmm_diff = abs(day - target)
                mmm = mmm_map[day]
    return mmm

def getExpDateFromDesc(desc):
    # "CMG Dec 31 2021 1745 Put (Weekly)" -> DEC 31 21"
    fields = desc.split()
    month = fields[1]
    day = fields[2]
    year = fields[3]
    exp_date = f"{day} {month.upper()} {year[-2:]}"
    return exp_date

def getBestEw(df, daysToExpiration=None, count=1):
    rows = []
    candidates = opchain.get_candidates(df)
    if candidates is None or len(candidates) == 0:
        return rows
    candidates = candidates.sort_values(by="e_w", ascending=False)
    for i in range(count):
        if i >= len(candidates):
            return rows
        candidate = candidates.iloc[i]
        mmm = getMMM(candidates, 7)
        row = {}
        row["exp_date"] = getExpDateFromDesc(candidate["s_description"])
        row["mmm"] = mmm
        mmm2 = getMMM(candidates, daysToExpiration)
        row["mmm2"] = mmm2

        underlying = candidates.attrs["underlyingPrice"]
              
        s_strike = candidate["s_strikePrice"]
            
        desc = candidate["s_description"]
        if desc.find("Put") >= 0:
            dm = underlying - mmm - s_strike
            dm2 = underlying - mmm2 - s_strike
        else:
            dm = s_strike - underlying - mmm
            dm2 = s_strike - underlying - mmm2
        row["dm"] = dm
        row["dm2"] = dm2
        dmu = (dm/underlying)*100.0
        row["dmu"] = dmu
        dmu2 = (dm2/underlying)*100.0
        row["dmu2"] = dmu2

        for k in candidates.attrs:
            if k == "mmm":
                continue
            v = candidates.attrs[k]
            row[k] = v
        for k in candidate.keys():
            v = candidate[k]            
            row[k] = v
        row['days_exp'] = daysToExpiration
        rows.append(row)
    return rows
 

def getBestEUs(stocklist_file, rows, run_date=None, exp_days=None):
    if not os.path.isfile(stocklist_file):
        eprint(f"{stocklist_file} not found")
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
            if not symbol or not symbol.isupper() or symbol[0] == '#':
                eprint(f"ignoring symbol: {symbol}")
                line = f.readline()
                continue
            add_count = 0
            for putCall in ("PUT", "CALL"):
                df = opchain.get_dataframe(symbol, putCall=putCall, run_date=run_date, daysToExpiration=exp_days)
                if df is None or len(df) == 0:
                    eprint(f"unable to get data for {symbol}")
                    failcount += 1
                    if failcount == 3:
                        eprint("too many failures, quitting")
                        sys.exit()
                else:
                    if exp_days is None:
                        # iterate through all the days of expiration
                        days = df['daysToExpiration']
                        days = list(set(list(days.values)))
                        days.sort()
                        for day in days:
                            days_df = df.drop(df[df.daysToExpiration != day].index)
                            bestews = getBestEw(days_df, daysToExpiration=day, count=NUM_EWS)
                            rows.extend(bestews)
                            add_count += len(bestews)
                            failcount = 0  # reset
                    else:
                        row = getBestEw(df, daysToExpiration=exp_days)
                        if row:
                            rows.append(row)
                            add_count += 1
                        failcount = 0  # reset
            if add_count:
                symbols.append(symbol)
            line = f.readline()
       
    eprint(f"got data for {len(symbols)} symbols {symbols} from file: {stocklist_file}")


def minmaxFilter(df, use_odd_day_symbols=False):
    logging.info(f"df minmaxstart:  {len(df)} rows")
    df = df[df.days_exp >= MIN_DAY]
    df = df[df.days_exp <= MAX_DAY]
    logging.info(f"len after min/max days: {len(df)}")
    df = df[df.e_w > MIN_EW]
    logging.info(f"len after > MIN_EW: {len(df)}")
    #df = df.drop(df[df.dmu <= MIN_DMU].index) 
    df = df[df.dmu >= MIN_DMU]
    logging.info(f"len after <= MIN_DMU: {len(df)}")
    # df = df.drop(df[df.dmu2 <= MIN_DMU2].index)
    df = df[df.dmu2 >= MIN_DMU2]
    logging.info(f"len after <= MIN_DMU2: {len(df)}")
    # df = df.drop(df[df.dme_u <= MIN_DME_U].index)
    logging.debug(f"dme_w max: {df.dme_w.max()}")
    logging.debug(f"dme_w min: {df.dme_w.min()}")
    #df = df.drop(df[df.dme_w <= MIN_DME_W].index)
    df = df[df.dme_w > MIN_DME_W]
    logging.info(f"len after > MIN_DME_W: {len(df)}")
    # df = df.drop(df[df.mg <= MIN_MG].index)
    df = df[df.mg >= MIN_MG]
    logging.info(f"len after <= MIN_MG: {len(df)}")
    if len(df) == 0:
        logging.info("no rows, returning empty dataframe")
        return df
           
    #eprint("symbols:", df['symbol'])
    eprint("df minmaxstart start filter symbols:", len(df))
    if use_odd_day_symbols:
        df = df[df.symbol.isin(ODD_DAY_SYMBOLS)]
    else:
        eprint("filtered symbols")
        df = df[~df.symbol.isin(ODD_DAY_SYMBOLS)]

    eprint("trimed df:", len(df))
    
    df = df.sort_values(by="dme_w", ascending=False)  # DME_u
    return df
     

# main
#
if len(sys.argv) < 2 or sys.argv[1] in ('-h', '--help'):
    print("usage: python get_besteu.py [--rundate YYYY-MM-DD ] [--expdays DD] [--outdir dir] [stocklist_file1] [stocklist_file2]")
    sys.exit(0)


run_date = None
exp_days = None
out_dir = None
csv_files = []

for arg in sys.argv:
    if arg.endswith(".py"):
        continue
    if run_date is None and arg == "--rundate":
        run_date = arg
    elif run_date == "--rundate":
        run_date = arg
    elif exp_days is None and arg == "--expdays":
        exp_days = arg
    elif exp_days == "--expdays":
        exp_days = int(arg)
    elif out_dir is None and arg == "--outdir":
        out_dir = "--outdir"
    elif out_dir == "--outdir":
        out_dir = arg
    else:
        csv_files.append(arg)

loglevel = logging.ERROR
logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)

# assume it's a csv file of symbols
rows = []
start_time = time.time()
eprint("getBestEUs start")
for csv_file in csv_files:
    getBestEUs(csv_file, rows, run_date=run_date, exp_days=exp_days)
eprint(f"getBestEUs done - {int(time.time() - start_time)}")     
if not rows:
    eprint("no rows found!")
    sys.exit()
# row = rows[0]
# columns = list(row.keys())
df = pd.DataFrame(rows, columns=BEST_EW_COLUMNS, )

days = df['days_exp']
print(df.columns)
print("days:", days)
print("row count:", len(df))
days = list(set(list(days.values)))
days.sort()

if out_dir:
    original_stdout = sys.stdout # Save a reference to the original standard output
    eprint(f"days start  - {int(time.time() - start_time)}")     

    for day in days:  
        if day < MIN_DAY:
            #eprint(f"{day} less than {MIN_DAY}, skipping")
            continue
        if day > MAX_DAY:
            #eprint(f"{day} greater than {MAX_DAY}, skipping")
            continue
        logging.info(f"running day: {day}")
        df_day = df[df.days_exp == day]
        logging.info(f"df_day: {len(df_day)} rows")
        if len(df_day.index) == 0:
            logging.info("no rows")
            continue # no rows
        df_day = minmaxFilter(df_day)
         
        if len(df_day.index) > 0:
            filename = f"{out_dir}/best_ew_{run_date}_{day}.csv"
            df_day = df_day.rename(columns=RENAME_COLUMNS)
            with open(filename, 'w') as f:
                sys.stdout = f # Change the standard output to the file we created.
                output = df_day.to_csv(float_format="%.2f")
                print(output)
    eprint(f"days done  - {int(time.time() - start_time)}")     

    # odd days file
    filename = f"{out_dir}/best_ew_{run_date}_index.csv"
    df = pd.DataFrame(rows, columns=BEST_EW_COLUMNS)
    df_odd = minmaxFilter(df, use_odd_day_symbols=True)
    df_odd = df_odd.rename(columns=RENAME_COLUMNS)
    with open(filename, 'w') as f:
        sys.stdout = f # Change the standard output to the file we created.
        output = df_odd.to_csv(float_format="%.2f")
        print(output)
    eprint(f"odds done  - {int(time.time() - start_time)}")     


    sys.stdout = original_stdout # Reset the standard output to its original value
else:
    print(df)
