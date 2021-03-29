from datetime import datetime
import os
import time
import json
import logging
import requests
import pandas as pd
"""
OPTION_PROPS = ("description", "symbol", "putCall", "strikePrice", "bid", "ask", "last", "mark", "bidAskSize",
    "highPrice", "lowPrice", "openPrice", "closePrice", "totalVolume", "expirationDate", "daysToExpiration", 
    "netChange", "volatility", "delta", "gamma", "theta", "vega", "openInterest", "timeValue",
    "theoreticalOptionValue")
"""
OPTION_PROPS = ["description", "symbol", "putCall", "last", "mark", "bidAskSize", "delta", "openInterest", "theoreticalOptionValue", "strikePrice", "expirationDate", "daysToExpiration", "totalVolume"]

MIN_VAL = -999.0 
DEFAULT_DAYS = 45
CSR_CS_DELTA_RANGE = (0.2, 0.45)     
CSR_CB_DELTA_RANGE = (0.09, 0.38)      
PSR_PS_DELTA_RANGE = (0.2, 0.45)    
PSR_PB_DELTA_RANGE = (0.09, 0.38)    

def get_today():
    now = time.time()
    dt_now = datetime.fromtimestamp(now)
    run_date = f"{dt_now.year}-{dt_now.month:02d}-{dt_now.day:02d}"
    return run_date
    

def get_chains(symbol, run_date=None, dt_min=None, dt_max=None, reload=False):
    now = time.time()
    if dt_min is None:
        # use current time
        dt_min = datetime.fromtimestamp(now)
    if dt_max is None:
        # use current time + 1 year
        then = time.time() + 365*24*60*60
        dt_max = datetime.fromtimestamp(then)
    if run_date is None:
        # no date_str, use current day
        run_date = get_today()
        
    filepath = f"data/{symbol}/{symbol}-{run_date}.json"
    if not reload and os.path.isfile(filepath):
        logging.info(f"returning data from: {filepath}")
        with open(filepath) as json_file:
            data = json.load(json_file)
        return data

    # we'll need an auth token to call td ameritrade     
    with open("auth_token", "r") as f:
        data = f.read().strip()
    headers = {"Authorization": "Bearer " + data}

    params = {}
    params["symbol"] = symbol
    params["strikeCount"] = 200
    params["includeQuotes"] = True
    params["strategy"] = "ANALYTICAL"
    params["interval"] = 1
    params["fromDate"] = f"{dt_min.year}-{dt_min.month}-{dt_min.day}"
    params["toDate"] = f"{dt_max.year}-{dt_max.month}-{dt_max.day}"
    logging.info(f"fromDate: {dt_min.year}-{dt_min.month}-{dt_min.day}")
    logging.info(f"toDate: {dt_max.year}-{dt_max.month}-{dt_max.day}")
    # params["daysToExpiration"] = 45
    req = "https://api.tdameritrade.com/v1/marketdata/chains"
    rsp = requests.get(req, params=params, headers=headers)
    if rsp.status_code != 200:
        logging.error(f"got bad status code: {rsp.status_code}")
        return None
    data = rsp.json()
    if data["status"] == "FAILED":
        logging.error(f"got FAILED status: {data}")
        return None
    #logging.info(rsp_json)

    # save result if data dir exists
    if os.path.isdir("data"):
        if not os.path.isdir(f"data/{symbol}"):
           os.mkdir(f"data/{symbol}")
        with open(filepath, 'w') as json_file:
            json.dump(data, json_file)
        
    return data

def _descIsCall(desc):
    if desc.find("Call") > 0:
        return True
    else:
        return False

def _descIsPut(desc):
    if desc.find("Put") > 0:
        return True
    else:
        return False

def _descIsPM(desc):
    if desc.find("(PM)") > 0:
        return True
    else:
        return False

def _get_mapdata(option_map, rows, underlying=None):
    if underlying is None:
        msg = "underlying not supplied"
        logging.error(msg)
        raise ValueError(msg)
    for expDate in option_map:
        bundle = option_map[expDate]
        for strikePrice in bundle:
            options = bundle[strikePrice]
            for option in options:
                logging.debug(f"in: {option}")
                delta = option["delta"]
                if  not isinstance(delta, float) or delta == MIN_VAL:
                    logging.debug(f"skipping delta value: {delta}")
                    continue  
                description = option["description"]
                #if option_symbol and option["symbol"] != option_symbol:
                #    continue
                #if not option_symbol:
                #    option_symbol = optiaon["symbol"]
                option_symbol = option["symbol"]
                logging.info(f"description: {description}, option_symbol: {option_symbol}")
        
                strike = float(strikePrice)
                if _descIsPM(description):
                    logging.info(f"skip PM option: {description}")
                    continue
                elif _descIsPut(description):
                    if underlying <= strike:
                        logging.info(f"skip put, underlying: {underlying} strike: {strike}")
                        continue
                elif _descIsCall(description):
                    if underlying >= strike:
                        logging.info(f"skip call, underlying: {underlying} strike: {strike}")
                        continue
                else:
                    msg = f"unexpected description: {description}"
                    logging.error(msg)
                    raise ValueError(msg)
                 
                logging.debug(f"adding option: {option}")
                row = []
                for propname in OPTION_PROPS:
                    if propname in option:
                        row.append(option[propname])
                    else:
                        row.append('')
                rows.append(row)


def get_working_days(df, daysToExpiration=DEFAULT_DAYS):
    days = df['daysToExpiration']
    unique_days = list(set(list(days.values)))
    unique_days.sort()
    closest = None
    for day in unique_days:
        if closest is None:
            closest = day
        elif abs(day - daysToExpiration) < abs(closest - daysToExpiration):
            closest = day
    return closest

def get_dataframe(symbol, putCall=None, run_date=None, reload=False, daysToExpiration=None):
    chains = get_chains(symbol, run_date=run_date, reload=reload)
    if not chains:
        logging.error(f"no data found for symbol: {symbol}")
        return None

    if run_date is None:
        run_date = get_today()

    df_rows = []
    underlying = chains["underlyingPrice"]
    volatility = chains["volatility"]
    interestRate = chains["interestRate"]
    if not putCall or putCall.upper() == "PUT":
        _get_mapdata(chains["putExpDateMap"], df_rows, underlying=underlying)
    if not putCall or putCall.upper() == "CALL":
        _get_mapdata(chains["callExpDateMap"], df_rows, underlying=underlying)
    logging.info(f"get_dataframe, got: {len(df_rows)}")

    # construct pandas dataframe
    df =  pd.DataFrame(df_rows, columns=OPTION_PROPS)
    df.attrs["underlyingPrice"] = underlying
    df.attrs["volatility"] = volatility
    df.attrs["interestRate"] = interestRate
    df.attrs["runDate"] = run_date
    df.attrs["symbol"] = symbol

    if daysToExpiration:
        # return just those rows that are closest to desired daysToExpiration
        filterDays = get_working_days(df, daysToExpiration=daysToExpiration)
        logging.info(f"filter contracts for daysToExpiration={daysToExpiration}")
        df = df.drop(df[df.daysToExpiration != filterDays].index)

    # add derived columns
    
    meg = df['last'] * (1.0 - abs(df['delta']))
    df['meg'] = meg
    megu = meg / underlying
    df['megu'] = megu
    # pom - prob out of money = 1.0 - delta
    pom = 1.0 - abs(df['delta'])
    df['pom'] = pom

    return df

def get_prb(value, options=None):
    if options is None:
        logging.warn("get_prb, no options")
        return None
    if len(options) < 2:
        logging.warn("get_prb, expecting at least two options")
        return None
     
    p1 = None
    p2 = None
    for i, option in options.iterrows():
        if option.strikePrice == value:
            return abs(option.delta)
        elif option.strikePrice < value:
            if p1 is None or  option.strikePrice > p1.strikePrice:
                p1 = option
        else:
            if p2 is None or option.strikePrice < p2.strikePrice:
                p2 = option
    
    if p1 is None or p2 is None:
        logging.warn("get_prb, no option_list strike values in range")
        return None
    #logging.debug("option_low:", p1)
    #logging.debug("value:", value)
    #logging.debug("option_high:", p2)
    
    def linear(x, x1, x2, y1, y2):
        return y1 + (x - x1)*(y2 - y1)/(x2 - x1)
   
    prb = linear(value, p1.strikePrice, p2.strikePrice, p1.delta, p2.delta)
 
    return abs(prb)

def get_derived(row, putCall=None, underlying=1.0, options=None):
    row["e"] = MIN_VAL
    row["mg"] = MIN_VAL
    row["width"] = MIN_VAL
    row["mg_w"] = MIN_VAL
    row["mg_u"] = MIN_VAL
    row["pop"] = MIN_VAL
    row["e_u"] = MIN_VAL
    row["ml"] = 0.0
    row["ml_u"] = 0.0
    row['e'] = 0.0

    if putCall not in ("PUT", "CALL"):
        raise ValueError("putCall should be either PUT or CALL")
    
    if putCall == "PUT":
        buy_prefix = "b_"
        sell_prefix = "s_"
    else:
        buy_prefix = "b_"
        sell_prefix = "s_"
    
    #logging.debug(f"ps: [{row['ps_description'].strip()}]")
    #logging.debug(f"pb: [{row['pb_description'].strip()}]")
    
    sell_price = row[sell_prefix+"mark"]
    buy_price = row[buy_prefix+"mark"]
    sell_strike = row[sell_prefix+"strikePrice"]
    buy_strike = row[buy_prefix+"strikePrice"]
    sell_delta = row[sell_prefix+"delta"]
    buy_delta = row[buy_prefix+"delta"]
    
    npr = sell_price - buy_price
    width = abs(sell_strike - buy_strike)

    if npr <= 0:
        return False
    if width <= 0:
        return False
    mg = npr 
    ml = -(abs(sell_strike - buy_strike) - npr)
    ml_u = 100 * ml / underlying

    if putCall == "CALL": 
        be_strike = sell_strike + npr
    else:
        be_strike = sell_strike - npr
      
    be_delta = get_prb(be_strike, options=options)
    if be_delta is None:
        return False
    be_prb = 1 - be_delta

    pop = 100.0 * be_prb

    a_prb = 1 - abs(sell_delta)  
    b_prb = abs(sell_delta - be_delta)
    c_prb = abs(buy_delta - be_delta)
    d_prb = abs(buy_delta)
     
    ea = mg * a_prb
    logging.debug(f"ea: {ea}")
    eb = 0.5 * mg * b_prb
    logging.debug(f"eb: {eb}")  # wrong
    ec = 0.5 * ml * c_prb
    logging.debug(f"ec: {ec}")  # wrong
    ed = ml * d_prb
    logging.debug(f"ed: {ed}")
    e = ea + eb + ec + ed
    logging.debug(f"e: {e}")
    e_u = 100 * e / underlying
    logging.debug(f"e_u: {e_u}")

    mg_w = 100 * mg / width
    mg_u = 100 * mg / underlying

    row["e"] = e
    row["e_u"] = e_u
    row["mg"] = mg
    row["ml"] = ml
    row["ml_u"] = ml_u
    row["width"] = width
    row["mg_w"] = mg_w
    row["mg_u"] = mg_u
    row["pop"] = pop
    return True

def get_candidates(contracts, putCall=None, sell_range=None, buy_range=None, daysToExpiration=None):
    if len(contracts) == 0:
        logging.warning("no contracts")
        return None
    if putCall is None:
        putCalls = contracts['putCall']
        unique_putCalls = list(set(list(putCalls.values)))
        if len(unique_putCalls) != 1:
            raise ValueError("putCall should be either PUT or CALL")
        # contracts are all puts or all calls, so use that
        putCall = unique_putCalls[0]
    elif putCall.upper() not in ("PUT", "CALL"):
        raise ValueError("putCall should be either PUT or CALL")
    
    logging.debug(f"get_candidates - using putCall: {putCall}")
            
    if daysToExpiration is None:
        days = contracts['daysToExpiration']
        unique_days = list(set(list(days.values)))
        unique_days.sort()
        if len(unique_days) != 1:
            raise ValueError(f"set daysToExpiration to one of the values in: {unique_days}")
        daysToExpiration = unique_days[0]
    logging.info(f"get_candidates - using daysToExpiration: {daysToExpiration}")
    
    if putCall.upper() == "PUT":
        if sell_range is None:
            sell_range = PSR_PS_DELTA_RANGE
        if buy_range is None:
            buy_range = PSR_PB_DELTA_RANGE
        
    if putCall.upper() == "CALL":
        if sell_range is None:
            sell_range = CSR_CS_DELTA_RANGE
        if buy_range is None:
            buy_range = CSR_CB_DELTA_RANGE

    buy_prefix = "b_"
    sell_prefix = "s_"
    underlying = contracts.attrs["underlyingPrice"]
       
    columns = []
    keep_list = ["description", "last", "mark", "delta", "strikePrice", "totalVolume"]

    for name in keep_list:
        columns.append(sell_prefix+name)
        columns.append(buy_prefix+name)
    derived_list = ["e", "mg", "width", "mg_w", "mg_u", "pop", "e_u", "ml", "ml_u" ]
    for name in derived_list:
        columns.append(name)
          
    candidate_rows = []
    
    for i, b in contracts.iterrows(): 
        if b.daysToExpiration != daysToExpiration:
            logging.debug(f"skipping buy row {i}, daysToExpiration[{b.daysToExpiration}] != {daysToExpiration}")
            continue
        if b.putCall != putCall:
            logging.debug(f"skipping buy row {i}, putCall[{b.putCall}] != {putCall}")
            continue
        if abs(b.delta) < buy_range[0] or buy_range[1] < abs(b.delta):
            logging.debug(f"skipping row {i}, b delta {b.delta} out of range: {buy_range}")
            continue

        for j, s in contracts.iterrows():
            if i == j:
                continue
            if s.daysToExpiration != daysToExpiration:
                logging.debug(f"skipping sell row {j}, daysToExpiration[{s.daysToExpiration}] != {daysToExpiration}")
                continue
            if s.putCall != putCall:
                logging.debug(f"skipping sell row {j}, putCall[{s.putCall}] != {putCall}")
                continue
            if abs(s.delta) < sell_range[0] or sell_range[1] < abs(s.delta):
                logging.debug(f"skipping sell row {j},s  delta {abs(s.delta):.3f} out of range: {sell_range}")
                continue
            if putCall == "PUT" and b.strikePrice >= s.strikePrice:  
                logging.debug(f"skipping sell row {j}, buy price {b.strikePrice:.3f} >= sell price {s.strikePrice:.3f}")
                continue
            if putCall == "CALL" and b.strikePrice <= s.strikePrice:  
                logging.debug(f"skipping sell row {j}, buy price {b.strikePrice:.3f} <= sell price {s.strikePrice:.3f}")
                continue                                                       
            if abs(b.delta) > abs(s.delta):
                logging.debug(f"skipping sell row {j}, b delta {b.delta} greater than ps_delta {s.delta}")
                continue

            # TBD: pre-elimination
            row = {}
            for k in b.index:
                if k not in keep_list:
                    continue
                b_key = buy_prefix + k
                row[b_key] = b[k]
            for k in s.index:
                if k not in keep_list:
                    continue
                s_key = sell_prefix + k
                row[s_key] = s[k]  
            
            if get_derived(row, putCall=putCall, underlying=underlying, options=contracts):
                #if row["e"] > MIsN_VAL and row['e_u'] > MIN_VAL:
                candidate_rows.append(row)                                   
                                                                
    # construct candidates dataframe
    candidates =  pd.DataFrame(candidate_rows, columns=columns)
    for k in contracts.attrs:
        v = contracts.attrs[k]
        candidates.attrs[k] = v
    candidates.attrs['daysToExpiration'] = daysToExpiration
    candidates.attrs["putCall"] = putCall
    
    #candidates = candidates.sort_values(by="e", ascending=False)

    #candidates['pom'] = 1 - abs(candidates['s_delta'])
    logging.info(f"get_candidates, returning {len(candidates)} candidates from {len(contracts)} contracts")
    candidates = candidates.sort_values(by="e_u", ascending=False)
    return candidates
    

