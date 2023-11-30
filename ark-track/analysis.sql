-- high and low value of a stock for that day
select max(high) from stock_price where stock_id = 15117;
select min(low) from stock_price where stock_id = 15117;

-- closing price
select last(close, dt) from stock_price where stock_id = 15117;


-- low volume stocks held by Ark ETFs
select stock_id, symbol, sum(volume) as total_volume 
from stock_price join stock on stock.id = stock_price.stock_id 
group by stock_id, symbol 
order by total_volume asc LIMIT 10;

-- make a histogram with 4 time intervals between the prices of $19 and $20
SELECT histogram(close, 19, 20, 4) FROM stock_price WHERE stock_id = 27104;

-- price data is stored as 5-minute bars in db; this function uses the time_bucket() function to aggregate into 1 hour and 20 min bars
select time_bucket(INTERVAL '1 hour', dt) AS bucket, first(open, 
dt), max(high), min(low), last(close, dt) 
from stock_price 
where stock_id = 27104 
group by bucket 
order by bucket desc;

select time_bucket(INTERVAL '20 minute', dt) AS bucket, first(open, 
dt), max(high), min(low), last(close, dt) 
from stock_price 
where stock_id = 27104 
group by bucket o
rder by bucket desc;

-- use time_bucket_gapfill() function to fill forward time gaps and missing prices in the data
SELECT time_bucket_gapfill('5 min', dt, now() - INTERVAL '5 day', now()) AS bar, 
avg(close) as close FROM stock_price WHERE stock_id = 7502 and dt > now () - INTERVAL '5 day' 
group by bar, stock_id 
order by bar;

-- pre-compute and save hourly price bars aggregated from 5 minute price bars by creating a materialized view
CREATE MATERIALIZED VIEW hourly_bars WITH (timescaledb.continuous) 
AS SELECT stock_id, time_bucket(INTERVAL '1 hour', dt) AS day, 
first(open, dt) as open, MAX(high) as high, MIN(low) as low, 
last(close, dt) as close, SUM(volume) as volume 
FROM stock_price 
GROUP BY stock_id, day;

CREATE MATERIALIZED VIEW daily_bars WITH (timescaledb.continuous) 
AS SELECT stock_id, time_bucket(INTERVAL '1 day', dt) AS day, 
first(open, dt) as open, MAX(high) as high, MIN(low) as low,
last(close, dt) as close, SUM(volume) as volume 
FROM stock_price 
GROUP BY stock_id, day;

SELECT * FROM hourly_bars WHERE stock_id = 27104 ORDER BY hour desc;

-- Daily moving average for price
SELECT avg(close) FROM ( SELECT * FROM daily_bars WHERE stock_id = 27104 ORDER BY day DESC LIMIT 20 ) a;

-- top gainers for the day
WITH prev_day_closing AS (
    SELECT stock_id, day, close, 
    LEAD(close) OVER (PARTITION BY stock_id ORDER BY day DESC) AS 
    rev_day_closing_price FROM daily_bars ), daily_factor AS ( 
        SELECT stock_id, day, close / prev_day_closing_price AS daily_factor 
        FROM prev_day_closing ) SELECT day, LAST(stock_id, daily_factor) AS 
        stock_id, MAX(daily_factor) AS max_daily_factor 
        FROM daily_factor 
        JOIN stock ON stock.id = daily_factor.stock_id 
        GROUP BY day 
        ORDER BY day DESC, max_daily_factor DESC;

-- detect bullish engulfing pattern on price data
SELECT * FROM ( SELECT day, open, close, stock_id, LAG(close, 1) 
OVER ( PARTITION BY stock_id ORDER BY day ) previous_close, LAG(open, 1) 
OVER ( PARTITION BY stock_id ORDER BY day ) previous_open FROM daily_bars ) a 
WHERE previous_close < previous_open AND close > previous_open AND open < previous_close AND day = '2023-11-25';

-- closing price is higher 3 times on higher volume
SELECT * FROM ( SELECT day, close, volume, stock_id, LAG(close, 1) 
OVER ( PARTITION BY stock_id ORDER BY day ) previous_close, LAG(volume, 1)
 OVER ( PARTITION BY stock_id ORDER BY day ) previous_volume, LAG(close, 2) 
 OVER ( PARTITION BY stock_id ORDER BY day ) previous_previous_close, LAG(volume, 2) 
 OVER ( PARTITION BY stock_id ORDER BY day ) previous_previous_volume FROM daily_bars ) a 
 WHERE close > previous_close AND previous_close > previous_previous_close AND volume > previous_volume 
 AND previous_volume > previous_previous_volume AND day = '2023-11-25';

-- three bar breakout pattern
SELECT * FROM ( SELECT day, close, volume, stock_id, LAG(close, 1)
OVER ( PARTITION BY stock_id ORDER BY day ) previous_close, LAG(volume, 1) 
OVER ( PARTITION BY stock_id ORDER BY day ) previous_volume, LAG(close, 2) 
OVER ( PARTITION BY stock_id ORDER BY day ) previous_previous_close, LAG(volume, 2) 
OVER ( PARTITION BY stock_id ORDER BY day ) previous_previous_volume, LAG(close, 3) 
OVER ( PARTITION BY stock_id ORDER BY day ) previous_previous_previous_close, LAG(volume, 3) 
OVER ( PARTITION BY stock_id ORDER BY day ) previous_previous_previous_volume 
FROM daily_bars ) a WHERE close > previous_previous_previous_close 
and previous_close < previous_previous_close and previous_close < previous_previous_previous_close 
AND volume > previous_volume and previous_volume < previous_previous_volume 
and previous_previous_volume < previous_previous_previous_volume AND day = '2023-11-25';


