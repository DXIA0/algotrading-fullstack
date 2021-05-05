#ctrl + shift + f10
#https://www.youtube.com/watch?v=bPPJTc3JoMI
#https://www.youtube.com/watch?v=_8lJ5lp8P0U&t=35s
#stream https://www.youtube.com/watch?v=fIzm57idu3Y
#stream https://www.youtube.com/watch?v=ThtNUs6VHj8
#schedule jobs https://datatofish.com/python-script-windows-scheduler/


#to do ###########################
# no working task scheduler
# websocket live price not working #disabled for now
#alpaca app https://www.youtube.com/watch?v=Ni8mqdUXH3g&t=80s 19:42


import sqlite3, requests, websocket, json, datetime, ibapi, time
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import *
import threading #hosting real time streaming data over socket
import pandas as pd
import alpaca_trade_api as alpacaapi
from fastapi import FastAPI
import datetime as date
from configparser import ConfigParser
config = ConfigParser()
import streamlit as st

############################      TRADING         ##################################


##################################################################################
#################                 IB Connection            #######################
##################################################################################

#class for IB brokers connection
class IBApi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self,self)

    #listen for real time bars
    def realtimeBar(self, reqId, time, open_, high, low, close, volume, wap, count):
        bot.on_bar_update(reqId, time, open_, high, low, close, volume, wap, count)

    def error(self, id, errorCode, errorMsg):
        print(errorCode)
        print(errorMsg)

class IBOrder():
    ib = None

    def error_handler(msg):
        """Handles the capturing of error messages"""
        print
        "Server Error: %s" % msg

    def reply_handler(msg):
        """Handles of server replies"""
        print
        "Server Response: %s, %s" % (msg.typeName, msg)

    def __init__(self):
        #Connect to IB on init
        self.ib = IBApi()
        self.ib.connect("127.0.0.1", 7497, 1)
        self.ib.register(error_handler, 'Error')
        self.ib.registerAll(reply_handler)
        ib_thread = threading.Thread(target=self.run_loop, daemon=True)
        ib_thread.start()
        time.sleep(1) #pause for 1 second

        '''Disabled until account funded with £500 and subscribe to markets, watch in episode pt2
        
        #get symbol you want to trade
        symbol = input("Enter symbol you want to trade: ")
        contract = Contract() #contract object for streaming data or submitting orders
        contract.symbol = symbol.upper() #sets symbol for contract from the one that was input and to uppercase
        contract.secType = "STK" #STK = stocks
        contract.exchange = "SMART" #automatic
        contract.currency = "USD"

        #Request real time market data
        self.ib.reqRealTimeBars(0, contract, 5, "TRADES", 1, [])
        # no more than 60 *new* requests for real time bars can be made in 10 minutes, and the total number
        # of active active subscriptions of all types cannot exceed the maximum allowed market data lines for the user.
        #https://interactivebrokers.github.io/tws-api/market_data.html#market_subscriptions
        '''

        # Bracket Order Setup
        def bracketOrder(self, parentOrderId, action, quantity, profitTarget, stopLoss):
            # Initial Entry
            # Create our IB Contract Object
            contract = Contract()
            contract.symbol = self.symbol.upper()
            contract.secType = "STK"
            contract.exchange = "SMART"
            contract.currency = "USD"

            # Create Parent Order / Initial Entry
            parent = Order()
            parent.orderId = parentOrderId
            parent.orderType = "MKT"
            parent.action = action
            parent.totalQuantity = quantity
            parent.transmit = False

            # Profit Target
            profitTargetOrder = Order()
            profitTargetOrder.orderId = parent.orderId + 1
            profitTargetOrder.orderType = "LMT"
            profitTargetOrder.action = "SELL"
            profitTargetOrder.totalQuantity = quantity
            profitTargetOrder.lmtPrice = round(profitTarget, 2)
            profitTargetOrder.parentId = parentOrderId
            profitTargetOrder.transmit = False

            # Stop Loss
            stopLossOrder = Order()
            stopLossOrder.orderId = parent.orderId + 2
            stopLossOrder.orderType = "STP"
            stopLossOrder.action = "SELL"
            stopLossOrder.totalQuantity = quantity
            stopLossOrder.parentId = parentOrderId
            stopLossOrder.auxPrice = round(stopLoss, 2)
            stopLossOrder.transmit = True

            bracketOrders = [parent, profitTargetOrder, stopLossOrder]
            return bracketOrders

        '''
                #place complex order
                bracket = self.bracketOrder(orderId, "BUY", quantity, profitTarget, stopLoss)

                for order in bracket:
                    order.ocaGroup = "OCA_" + str(orderId)
                    self.ib.placeOrder(order.orderId, contract, order)
                orderId += 3
                '''
    #listen to socket
    def run_loop(self):
        self.ib.run()

    #pass realtime bar data back to our bot object
    def on_bar_update(self, reqId, time, open_, high, low, close, volume, wap, count):
        print(reqId)


#Start bot
#Order = IBOrder()

def simple_buy(symbol, quantity):
    # Connect to IB on init
    ib = IBApi()
    ib.connect("127.0.0.1", 7497, 1)
    time.sleep(1)  # pause for 1 second

    # create simple order object
    order = Order()
    order.orderType = "MKT"  # at current market price or LMT for limit etc
    order.action = "BUY"  # or sell
    order.totalQuantity = quantity

    # create contract object
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"  # STK = stocks
    contract.exchange = "SMART"  # automatic
    contract.currency = "USD"
    contract.primaryExchange = "ISLAND"  # Route exhcange through nasdaq as backup

    config.read('config.ini')
    orderId = int(config.get('main', 'orderid'))

    ib.placeOrder(orderId, contract, order)

    orderId = str(orderId + 1)
    config.read('config.ini')
    config.set('main', 'orderId', f'{orderId}')

    ib.disconnect()

def simple_sell(symbol, quantity):
    # Connect to IB on init
    ib = IBApi()
    ib.connect("127.0.0.1", 7497, 1)
    time.sleep(1)  # pause for 1 second

    # create simple order object
    order = Order()
    order.orderType = "MKT"  # at current market price or LMT for limit etc
    order.action = "SELL"  # or sell
    order.totalQuantity = quantity

    # create contract object
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"  # STK = stocks
    contract.exchange = "SMART"  # automatic
    contract.currency = "USD"
    contract.primaryExchange = "ISLAND"  # Route exchange through nasdaq as backup

    config.read('config.ini')
    orderId = int(config.get('main', 'orderid')) + 1

    ib.placeOrder(orderId, contract, order)

    orderId = str(orderId + 1)
    config.read('config.ini')
    config.set('main', 'orderId2', f'{orderId}')

    ib.disconnect()

'''
#execution commands
# place order
symbol = "TSLA"
quantity = 1

simple_buy(symbol, quantity)
simple_sell(symbol, quantity)
'''

############################      DATA         ##################################


##################################################################################
#################                 Database                 #######################
##################################################################################

today = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')

#connection = sqlite3.connect('app.db')
#connection.row_factory = sqlite3.Row #call items within like objects
#cursor = connection.cursor()

#  you should avoid using cursor.execute in a loop - that's ineffective as after each
#  insert statement the transaction is commited. Use cursor.executemany instead
#cursor.executemany(db_operation, (asset.symbol, asset.name))

#connection.commit()

def db_drop():
    connection = sqlite3.connect('app.db')
    connection.row_factory = sqlite3.Row  # call items within like objects
    cursor = connection.cursor()

    cursor.execute("""
    DROP TABLE stock_price
    """
    )

    cursor.execute("""
    DROP TABLE stock
    """
    )
    connection.commit()

def db_create():
    connection = sqlite3.connect('app.db')
    connection.row_factory = sqlite3.Row  # call items within like objects
    cursor = connection.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock (
            id INTEGER PRIMARY KEY, 
            symbol TEXT NOT NULL UNIQUE, 
            name TEXT NOT NULL,
            exchange TEXT NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_price (
            id INTEGER PRIMARY KEY, 
            stock_id INTEGER,
            date NOT NULL,
            open NOT NULL, 
            high NOT NULL, 
            low NOT NULL, 
            close NOT NULL, 
            volume NOT NULL,
            FOREIGN KEY (stock_id) REFERENCES stock (id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS strategy (
        id INTEGER PRIMARY KEY,
        name NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_strategy (
        stock_id INTEGER NOT NULL,
        strategy_id INTEGER NOT NULL,
        FOREIGN KEY (stock_id) REFERENCES stock(id)
        FOREIGN KEY (strategy_id) REFERENCES strategy(id)
        )
    """)

    strategies = ["opening_range_breakout", "opening_range_breakdown"]

    for strategy in strategies:
        cursor.execute("""
            INSERT INTO strategy (name) VALUES (?)
        """, (strategy,))

    connection.commit()


def update_df(df, table_name, command): #command = "replace", "append"
    connection = sqlite3.connect('app.db')
    connection.row_factory = sqlite3.Row  # call items within like objects
    cursor = connection.cursor()

    today = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
    df.insert(0, 'DTLog', today)
    df.to_sql(table_name, con=connection, if_exists=command, index=False) #update db
    return df


##################################################################################
#################                Alpaca                ###########################
##################################################################################

config.read('config.ini')
ALPCACA_API_KEY = config.get('main', 'ALPCACA_API_KEY')
ALPCACA_SECRET_KEY = config.get('main', 'ALPCACA_SECRET_KEY')
ALPACA_BASE_URL = config.get('main', 'ALPACA_BASE_URL')


#https://paper-api.alpaca.markets

def alpaca_populate_stocks(): #updates all tradeable stocks run once per day
    #select all from database

    connection = sqlite3.connect('app.db')
    connection.row_factory = sqlite3.Row  # call items within like objects
    cursor = connection.cursor()

    cursor.execute("""
           SELECT symbol, name FROM stock
    """)

    rows = cursor.fetchall()
    db_symbols = [row['symbol'] for row in rows]

    alpaca_api = alpacaapi.REST(ALPCACA_API_KEY, ALPCACA_SECRET_KEY, base_url=ALPACA_BASE_URL)
    assets = alpaca_api.list_assets()

    db_operation = "INSERT INTO stock (symbol, name, exchange) VALUES (?,?,?)"

    for asset in assets:
        try:
            if asset.status == "active" and asset.tradable and asset.symbol not in db_symbols:
                cursor.execute(db_operation, (asset.symbol, asset.name, asset.exchange))
        except Exception as e:
            print(e)

    connection.commit()
    print("Alpaca Populate Stocks Complete")

def alpaca_populate_prices(): #day timeframe
    connection = sqlite3.connect('app.db')
    connection.row_factory = sqlite3.Row  # call items within like objects
    cursor = connection.cursor()

    cursor.execute("""
           SELECT id, symbol, name FROM stock
    """)

    rows = cursor.fetchall()

    symbols = []
    stock_dict = {}
    for row in rows:
        symbol = row["symbol"]
        symbols.append(symbol)
        stock_dict[symbol] = row["id"]

    alpaca_api = alpacaapi.REST(ALPCACA_API_KEY, ALPCACA_SECRET_KEY, base_url=ALPACA_BASE_URL)

    chunk_size = 200 #as you can only call 200 at a time
    for i in range(0, len(symbols), chunk_size):#for 0 to all the symbols in a step of 200
        symbol_chunk = symbols[i:i+chunk_size]

        barsets = alpaca_api.get_barset(symbol_chunk, "day")

        for symbol in barsets:
            #print(symbol)
            for bar in barsets[symbol]: #loop through each bar for the current symbol in dictionary
                stock_id = stock_dict[symbol] #get the matching stock id from dictionary
                cursor.execute(""" 
                    INSERT INTO stock_price (stock_id, date, open, high, low, close, volume)
                    VALUES (? ,? ,? , ?, ?, ?, ?)
                """, (stock_id, bar.t.date(), bar.o, bar.h, bar.l, bar.c, bar.v))
        connection.commit()
    print("Alpaca Populate Prices Complete")

       # SELECT symbol, date, open, high, low, close
       # FROM stock_price
       # JOIN stock on stock.id = stock_price.stock_id
       # WHERE symbol = 'AAPL'
       # ORDER BY date;


#################################################################################
#################             Premarket Gappers        ###########################
##################################################################################

def scrape_pregainers(gain_percent, max_last, min_volume):
    #https://www.tradingview.com/screener/

    #gain_percent = 10 # at least how many percent gain
    #max_last = 10 # maximum price
    #min_volume = 50000 # minimum volume

    #float_range = [2000000,15000000] # size of float
    #  if flt > FLOAT_BTW[0] and flt < FLOAT_BTW[1]:
    #    return True
    #  else:
    #    return False

    url = "https://thestockmarketwatch.com/markets/pre-market/today.aspx"
    header = {
      "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
      "X-Requested-With": "XMLHttpRequest"
    }

    r = requests.get(url, headers=header)

    df_temp = pd.read_html(r.text) #fetch table from "thestockmarketwatch.com"
    df_pregainers = df_temp[1]

    df_pregainers["%Chg"] = df_pregainers["%Chg"].str.strip("%") #reformat all percent
    df_pregainers["%Chg"] = pd.to_numeric(df_pregainers["%Chg"])
    df_pregainers["Last"] = df_pregainers["Last"].str.strip("$")
    df_pregainers["Last"] = pd.to_numeric(df_pregainers["Last"])
    df_pregainers["Volume"] = pd.to_numeric(df_pregainers["Volume"])

    df_pregainers.rename(columns={"Last":"Last Price ($)", "%Chg":"Change (%)", "Symb":"Symbol"}, inplace=True) #rename columns

    df_eligible_candidates = pd.DataFrame(columns = ["Change (%)", "Last Price ($)", "Symbol", "Company", "Volume"])

    for index, row in df_pregainers.iterrows():
        if row["Change (%)"] > gain_percent and                row["Last Price ($)"] < max_last and                row["Volume"] > min_volume:
              df_eligible_candidates = df_eligible_candidates.append(row, ignore_index=True)
        else:
            pass

    update_df(df_eligible_candidates, "premarket_gainers_stockmarketwatch", "replace")
    #return df_eligible_candidates #need to write into database with todays date and time


##################################################################################
#################               Finnhub                ###########################
##################################################################################

#    symbols = ["HSBC", "TSLA"]
def ws_price_call(symbols):
    def on_message(ws, message):
        #df_price = pd.json_normalize(message)
        #st.text(df_price)
        print(message)
        st.text(message)
        #return message
        #append to dataframe and then read dataframe

    def on_error(ws, error):
        print(error)

    def on_close(ws):
        print("### closed ###")

    def on_open(ws):
        for symbol in symbols:
            channel_data = {
                "type": "subscribe",
                "symbol": symbol
            }
            ws.send(json.dumps(channel_data))
            print(json.dumps(channel_data))


    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://ws.finnhub.io?token=c1obm9a37fkqrr9sfu50",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()


class finnhub_call:
    # https://pypi.org/project/websocket_client/
    def __init__(self, symbol):
        self.BASE_URL = "https://finnhub.io/api/v1/"
        self.token = "c1obm9a37fkqrr9sfu50"
        self.sandboxtoken = "sandbox_c1obm9a37fkqrr9sfu5g"
        self.webhook = "c1obm9a37fkqrr9sfu60"
        self.symbol = symbol

    def search_symbol(self):
        url = f'https://finnhub.io/api/v1/search?q=apple&token=c1obm9a37fkqrr9sfu50'
        r = requests.get(url)
        return r.json()


##################################################################################
#################               IEX Cloud              ###########################
##################################################################################

def update_df_date_amend(df, date_columns_to_amend, table_name):
    connection = sqlite3.connect('app.db')
    connection.row_factory = sqlite3.Row  # call items within like objects
    cursor = connection.cursor()

    today = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
    df[date_columns_to_amend] = df[date_columns_to_amend].applymap(lambda x: datetime.datetime.fromtimestamp(int(x)/1000.0).strftime('%Y-%m-%d %H:%M:%S')) #Reformat IEX dt format
    df.insert(0, 'DTLog', today)
    df.to_sql(table_name, con=connection, if_exists="replace", index=False) #update db
    return df


class iex_lookup:

    def __init__(self, symbol): #(self, token, symbol):
        self.BASE_URL = "https://cloud.iexapis.com/stable"
        self.token = "pk_18bf16c692c4487889b140e806463ed5"
        self.symbol = symbol

    def get_logo(self):
        url = f"{self.BASE_URL}/stock/{self.symbol}/logo?token={self.token}"
        r = requests.get(url)
        return r.json()

    def get_info(self):
        url = f"{self.BASE_URL}/stock/{self.symbol}/company?token={self.token}"
        r = requests.get(url)
        return r.json()
        #sample return
        #{'symbol': 'AAPL', 'companyName': 'Apple Inc', 'exchange': 'NASDAQ/NGS (GLOBAL SELECT MARKET)', 'industry': 'Electronic Computer Manufacturing ', 'website': 'https://www.apple.com/', 'description': "Apple Inc. is an American multinational technology company headquartered in Cupertino, California, that designs, develops, and sells consumer electronics, computer software, and online services. It is considered one of the Big Five companies in the U.S. information technology industry, along with Amazon, Google, Microsoft, and Facebook. Its hardware products include the iPhone smartphone, the iPad tablet computer, the Mac personal computer, the iPod portable media player, the Apple Watch smartwatch, the Apple TV digital media player, the AirPods wireless earbuds, the AirPods Max headphones, and the HomePod smart speaker line. Apple's software includes iOS, iPadOS, macOS, watchOS, and tvOS operating systems, the iTunes media player, the Safari web browser, the Shazam music identifier, and the iLife and iWork creativity and productivity suites, as well as professional applications like Final Cut Pro X, Logic Pro, and Xcode. Its online services include the iTunes Store, the iOS App Store, Mac App Store, Apple Arcade, Apple Music, Apple TV+, iMessage, and iCloud. Other services include Apple Store, Genius Bar, AppleCare, Apple Pay, Apple Pay Cash, and Apple Card. Apple was founded by Steve Jobs, Steve Wozniak, and Ronald Wayne in April 1976 to develop and sell Wozniak's Apple I personal computer, though Wayne sold his share back within 12 days. It was incorporated as Apple Computer, Inc., in January 1977, and sales of its computers, including the Apple I and Apple II, grew quickly.", 'CEO': 'Timothy Cook', 'securityName': 'Apple Inc', 'issueType': 'cs', 'sector': 'Manufacturing', 'primarySicCode': 3571, 'employees': 147000, 'tags': ['Electronic Technology', 'Telecommunications Equipment', 'Manufacturing', 'Electronic Computer Manufacturing '], 'address': '1 Apple Park Way', 'address2': None, 'state': 'California', 'city': 'Cupertino', 'zip': '95014-0642', 'country': 'US', 'phone': '14089961010'}


class iex_market_scan:
    def __init__(self):
        self.BASE_URL = "https://cloud.iexapis.com/stable"
        self.token = "pk_18bf16c692c4487889b140e806463ed5"

    def get_pregainers(self): #requires UPT "unlisted p.. trades authorisation"
        url = f"{self.BASE_URL}/stock/market/list/premarket_gainers?token={self.token}"
        r = requests.get(url)

        pregainers = r.json()
        df_pre = pd.json_normalize(pregainers)

        date_columns_to_amend = ["openTime", "closeTime", "highTime", "lowTime", "latestUpdate", "delayedPriceTime",
                                 "oddLotDelayedPriceTime", "extendedPriceTime", "iexOpenTime", "iexCloseTime",
                                 "lastTradeTime"]
        df_pre = update_df_date_amend(df_pre, date_columns_to_amend, "premarket_gainers")

        return df_pre
        #df_pre = pd.read_sql("SELECT * FROM premarket_gainers", connection)

    def get_gainers(self):
        url = f"{self.BASE_URL}/stock/market/list/gainers?token={self.token}"
        r = requests.get(url)

        livegainers = r.json()
        df_live = pd.json_normalize(livegainers)
        date_columns_to_amend = ["openTime", "closeTime", "highTime", "lowTime", "latestUpdate", "delayedPriceTime",
                                 "oddLotDelayedPriceTime", "extendedPriceTime", "iexOpenTime", "iexCloseTime",
                                 "lastTradeTime"]
        df_live = update_df_date_amend(df_live, date_columns_to_amend, "live_gainers")

        return df_live


################                 Strategies                 #####################

def opening_range_breakout():
    connection = sqlite3.connect('app.db')
    connection.row_factory = sqlite3.Row  # call items within like objects
    cursor = connection.cursor()

    cursor.execute("""
        SELECT id FROM strategy WHERE name = "opening_range_breakout"
    """)

    strategy = cursor.fetchone()["id"]

    cursor.execute("""
            SELECT symbol, name
            FROM stock
            JOIN stock_strategy on stock_strategy.stock_id = stock.id
            WHERE stock_strategy.strategy_id = ?
        """, (strategy_id,))

    stocks = cursor.fetchall()
    symbols = [stock["symbol"] for stock in stocks]

    alpaca_api = alpacaapi.REST(ALPCACA_API_KEY, ALPCACA_SECRET_KEY, base_url=ALPACA_BASE_URL)

    #for symbol in symbols:
        #need to get minute bar information from finnio or equivalnet

##################################################################################
#################                 Fast API                 #######################
##################################################################################

