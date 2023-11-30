import streamlit as st
import pandas as pd
import numpy as np
import requests
import config 
import psycopg2, psycopg2.extras
import plotly.graph_objects as go

connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

option = st.sidebar.selectbox("Which Dashboard?", ('Etf List','Stock List', 'Stock Finder', 'chart', 'pattern'), 0)

st.header(option)

if option == 'Stock Finder':
    st.subheader('Search for a stock symbol from its corresponding id from the database')
    id = st.sidebar.text_input("ID from Database", value='22114', max_chars=None, key=None, type='default')
    data = pd.read_sql(f"""select * from stock where id = {id} ;""", 
                            connection, params=(id.upper(),))
    st.write(data) 

if option == 'Etf List':
    data = pd.read_sql("select * from etf_holding;", connection)
    st.write(data)

if option == 'chart':
    symbol = st.sidebar.text_input("Symbol", value='MSFT', max_chars=None, key=None, type='default')

    data = pd.read_sql("""
        select date(day) as day, open, high, low, close
        from daily_bars
        where stock_id = (select id from stock where UPPER(symbol) = %s) 
        order by day asc""", connection, params=(symbol.upper(),))

    st.subheader(symbol.upper())

    fig = go.Figure(data=[go.Candlestick(x=data['day'],
                    open=data['open'],
                    high=data['high'],
                    low=data['low'],
                    close=data['close'],
                    name=symbol)])

    fig.update_xaxes(type='category')
    fig.update_layout(height=700)

    st.plotly_chart(fig, use_container_width=True)

    st.write(data)

if option == 'Stock List':
    data = pd.read_sql("select * from stock_price", connection)
    st.write(data)

if option == 'pattern':
    pattern = st.sidebar.selectbox(
        "Which Pattern?",
        ("1 Hour Price Bars","20 Minute Price Bars","Daily Moving Average","engulfing", "threebar")
    )

    if pattern == "1 Hour Price Bars":
        symbol = st.sidebar.text_input("Symbol", value='MSFT', max_chars=None, key=None, type='default')
        st.subheader(f'1 Hour Price Bars for {symbol}')
        data = pd.read_sql("""select time_bucket(INTERVAL '1 hour', dt) AS bucket, first(open, 
                                dt), max(high), min(low), last(close, dt) 
                                from stock_price 
                                where stock_id = (select id from stock where UPPER(symbol) = %s) 
                                group by bucket order by bucket desc;""", connection, params=(symbol.upper(),))
        st.write(data)

    if pattern == "20 Minute Price Bars":
        symbol = st.sidebar.text_input("Symbol", value='MSFT', max_chars=None, key=None, type='default')
        st.subheader(f'20 Minute Price Bars for {symbol}')
        data = pd.read_sql("""select time_bucket(INTERVAL '20 minute', dt) AS bucket, first(open, 
                                dt), max(high), min(low), last(close, dt) 
                                from stock_price 
                                where stock_id = (select id from stock where UPPER(symbol) = %s) 
                                group by bucket order by bucket desc;""", connection, params=(symbol.upper(),))
        st.write(data)

    if pattern == "Daily Moving Average":
        symbol = st.sidebar.text_input("Symbol", value='MSFT', max_chars=None, key=None, type='default')
        st.subheader(f'Daily Moving Average for {symbol}')
        data = pd.read_sql("""SELECT avg(close) FROM ( SELECT * FROM daily_bars where stock_id = (select id from stock where UPPER(symbol) = %s) 
                                    ORDER BY day DESC LIMIT 20 ) a;""", connection, params=(symbol.upper(),))
        st.write(data)

    if pattern == 'engulfing':
        cursor.execute("""
            SELECT * 
            FROM ( 
                SELECT day, open, close, stock_id, symbol, 
                LAG(close, 1) OVER ( PARTITION BY stock_id ORDER BY day ) previous_close, 
                LAG(open, 1) OVER ( PARTITION BY stock_id ORDER BY day ) previous_open 
                FROM daily_bars
                JOIN stock ON stock.id = daily_bars.stock_id
            ) a 
            WHERE previous_close < previous_open AND close > previous_open AND open < previous_close
            AND day = '2021-02-18'
        """)

    if pattern == 'threebar':
        cursor.execute("""
            SELECT * 
            FROM ( 
                SELECT day, close, volume, stock_id, symbol, 
                LAG(close, 1) OVER ( PARTITION BY stock_id ORDER BY day ) previous_close, 
                LAG(volume, 1) OVER ( PARTITION BY stock_id ORDER BY day ) previous_volume, 
                LAG(close, 2) OVER ( PARTITION BY stock_id ORDER BY day ) previous_previous_close, 
                LAG(volume, 2) OVER ( PARTITION BY stock_id ORDER BY day ) previous_previous_volume, 
                LAG(close, 3) OVER ( PARTITION BY stock_id ORDER BY day ) previous_previous_previous_close, 
                LAG(volume, 3) OVER ( PARTITION BY stock_id ORDER BY day ) previous_previous_previous_volume 
            FROM daily_bars 
            JOIN stock ON stock.id = daily_bars.stock_id) a 
            WHERE close > previous_previous_previous_close 
                AND previous_close < previous_previous_close 
                AND previous_close < previous_previous_previous_close 
                AND volume > previous_volume 
                AND previous_volume < previous_previous_volume 
                AND previous_previous_volume < previous_previous_previous_volume 
                AND day = '2021-02-19'
        """)

    try:
        rows = cursor.fetchall()

        for row in rows:
            st.image(f"https://finviz.com/chart.ashx?t={row['symbol']}")
    except psycopg2.ProgrammingError as e:
        st.subheader('')