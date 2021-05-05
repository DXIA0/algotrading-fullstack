import modules
import streamlit as st
import pandas as pd
import sqlite3
import datetime

##################################################################################
#################     Global variable declarations     ###########################
##################################################################################

global_var_today = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')

##################################################################################
#################     API & database initialization    ###########################
##################################################################################

#APIs
iex_market_scan = modules.iex_market_scan()

#iex_fetch = modules.iex_call(symbol)
#finn_fetch = modules.finnhub_call(symbol)

#symbol = "AAPL"
# company_info = iex_fetch.get_info()


#Database
#engine = sqlalchemy.create_engine("sqlite:///app.db", echo=False)

db_connect = sqlite3.connect('app.db')
db_cursor  = db_connect.cursor()



##################################################################################
#################               Functions              ###########################
##################################################################################





##################################################################################
#################                  Main                ###########################
##################################################################################

def main():
    st.sidebar.title("Navigation")
    option = st.sidebar.selectbox("Dashboard selection", ("Market Scanners", "Live Price", "News", "Technical Analysis"))

    col1, col2, col3, col4 = st.beta_columns(4)
    with col1:
        st.title("Mantis Capital  ")

    with col2:
        st.title(option)  # display dash name

    with col4:
        st.text("")
        if st.button("Refresh Premarket"):
            try:
                # Update databases
                gain_percent = 0
                max_last = 11
                min_volume = 50e3

                modules.scrape_pregainers(gain_percent, max_last, min_volume)
                "Live Gainers Scrape Complete"
            except:
                st.text("*   N/A Weekend or Markets Closed   *")

        if st.button("Refresh Databases"):
            #modules.alpaca_populate_stocks()
            #modules.alpaca_populate_prices()

            try:
                iex_market_scan = modules.iex_market_scan()
                df_live = iex_market_scan.get_gainers()
            except Exception as e:
                st.text(e)

            try:
                df_pre = modules.scrape_pregainers(10,10, 50000)
                #df_pre = modules.iex_market_scan.get_pregainers()
                #enter premarket scanner
            except:
                st.text("Premarket module missing")

            st.text(f"Last refreshed: {global_var_today}")

    st.text("")
    st.text("")

    if option == "Market Scanners":
        st.subheader("Live Premarket Gainers Feed")

        col1, col2, col3 = st.beta_columns(3)
        with col1:
            # gain_percent = st.number_input("Percentage Change")
            st.text("*15 min delay")
        with col2:
            # max_last = st.number_input("Maximum Price")
            st.text("Maximum Price $11")
        with col3:
            # min_volume = st.number_input("Minimum Volume, Default 50000") # minimum volume
            st.text("Minimum Volume 500k")

        df_pre = pd.read_sql("SELECT * FROM premarket_gainers_stockmarketwatch ORDER BY DTLog DESC", db_connect)
        st.dataframe(df_pre)

        st.subheader("Live Gainers Feed")
        df_live = pd.read_sql("SELECT * FROM live_gainers", db_connect)
        st.dataframe(df_live)

        st.text("")

        st.subheader("Buy Sell Command IKBR")
        col1, col2 = st.beta_columns(2)
        with col1:
            symbol = st.text_input("Company Ticker")
        with col2:
            quantity = st.number_input("Quantity")

        col1, col2 = st.beta_columns(2)
        with col1:
            if st.button("Buy"):
                modules.simple_buy(symbol, quantity)
        with col2:
            if st.button("Sell"):
                modules.simple_sell(symbol, quantity)

    if option == "Live Price":
        st.subheader("Live Price*")
        st.text("*US stock for now in format [TSLA, AAPL]")
        st.text("s: Symbol | p: Last price | t: UNIX milliseconds timestamp | v: Volume | c: Trade conditions")
        st.text("---------------------------------------------------------------------------------------------------------------")
        st.text("")

        symbols = ["TSLA", "AAPL"]
        #symbols = st.text_input("Company Tickers")
        modules.ws_price_call(symbols)




##################################################################################
#################            Namespace Init            ###########################
##################################################################################

if __name__ =="__main__":

  main() #calling the main method

