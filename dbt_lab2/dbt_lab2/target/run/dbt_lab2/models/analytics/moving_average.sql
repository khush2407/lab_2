
  
    

        create or replace transient table STOCK_PRICE_DB.RAW_DATA_ANALYTICS.moving_average
         as
        (

WITH ordered_data AS (
    SELECT
        date,
        symbol,
        close,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date) AS row_num
    FROM STOCK_PRICE_DB.RAW_DATA.vantage_api
)

SELECT
    date,
    symbol,
    close,
    AVG(close) OVER (
        PARTITION BY symbol
        ORDER BY row_num
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ) AS moving_average_90d
FROM ordered_data
ORDER BY symbol, date
        );
      
  