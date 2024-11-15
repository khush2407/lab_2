from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from datetime import datetime
import requests
import logging

default_parameters = {
    'owner': 'deva',
    'retries': 1,
}

with DAG(
    dag_id='stock_price_forecast',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule_interval='@daily',
    default_args=default_parameters,
    tags=['ETL', 'ML'],
) as data_pipeline:

    initialize_snowflake_env = SnowflakeOperator(
        task_id='setup_snowflake_environment',
        snowflake_conn_id='snowflake_conn',
        sql="""
        CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
        WITH WAREHOUSE_SIZE='SMALL'
        AUTO_SUSPEND=300
        AUTO_RESUME=TRUE
        INITIALLY_SUSPENDED=TRUE;

        CREATE DATABASE IF NOT EXISTS STOCK_PRICE_DB;
        CREATE SCHEMA IF NOT EXISTS STOCK_PRICE_DB.RAW_DATA;
        CREATE SCHEMA IF NOT EXISTS STOCK_PRICE_DB.PREDICT_DATA;

        USE DATABASE STOCK_PRICE_DB;
        USE SCHEMA RAW_DATA;

        CREATE TABLE IF NOT EXISTS VANTAGE_API (
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume INT,
            date DATE,
            symbol VARCHAR(10),
            PRIMARY KEY (date, symbol)
        );

        USE SCHEMA PREDICT_DATA;

        CREATE TABLE IF NOT EXISTS STOCKPRICE_FORECAST (
            series VARCHAR(20),
            ts DATE,
            forecast DOUBLE,
            lower_bound DOUBLE,
            upper_bound DOUBLE
        );

        CREATE TABLE IF NOT EXISTS STOCKPRICE_FINAL (
            SYMBOL VARCHAR(20),
            DATE DATE,
            actual DOUBLE,
            forecast DOUBLE,
            lower_bound DOUBLE,
            upper_bound DOUBLE
        );
        """,
        warehouse='COMPUTE_WH'
    )

    @task
    def retrieve_stock_data(stock_symbols=['AMZN', 'NVDA']):
        stock_data = []
        api_token = Variable.get('apikey')
        for ticker in stock_symbols:
            try:
                query_url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&apikey={api_token}'
                response = requests.get(query_url)
                response.raise_for_status()
                data = response.json()

                logging.info(f"Data received for {ticker}: {data}")

                if "Time Series (Daily)" in data:
                    daily_data = data["Time Series (Daily)"]
                    for date, stock_info in daily_data.items():
                        entry = {
                            '1. open': stock_info['1. open'],
                            '2. high': stock_info['2. high'],
                            '3. low': stock_info['3. low'],
                            '4. close': stock_info['4. close'],
                            '5. volume': stock_info['5. volume'],
                            'date': date,
                            'symbol': ticker
                        }
                        stock_data.append(entry)
                    logging.info(f"Fetched data successfully for {ticker}")
                else:
                    error_message = data.get('Note', data.get('Error Message', 'Unknown error'))
                    logging.error(f"No time series data found for {ticker}: {error_message}")
            except Exception as ex:
                logging.error(f"Error fetching data for {ticker}: {str(ex)}")
                continue

        if not stock_data:
            raise ValueError("No stock data fetched from API. Check API key and rate limits.")
        return stock_data

    @task
    def generate_insert_statement(data_entries):
        try:
            insert_values_list = []
            for entry in data_entries:
                open_price = float(entry['1. open'])
                high_price = float(entry['2. high'])
                low_price = float(entry['3. low'])
                close_price = float(entry['4. close'])
                trade_volume = int(entry['5. volume'])
                trade_date = entry['date']
                ticker_symbol = entry['symbol']

                insert_values_list.append(f"({open_price}, {high_price}, {low_price}, {close_price}, {trade_volume}, '{trade_date}', '{ticker_symbol}')")

            if not insert_values_list:
                raise ValueError("No data to insert into Snowflake. Aborting insert.")

            insert_query = f"""
                INSERT INTO STOCK_PRICE_DB.RAW_DATA.VANTAGE_API (open, high, low, close, volume, date, symbol)
                VALUES {', '.join(insert_values_list)};
            """

            sql_transaction = f"""
            BEGIN TRANSACTION;

            USE DATABASE STOCK_PRICE_DB;
            USE SCHEMA RAW_DATA;

            {insert_query}

            COMMIT;
            """

            logging.info("Data insertion SQL prepared successfully.")
            return sql_transaction

        except Exception as ex:
            logging.error(f"Error in preparing data insertion: {str(ex)}")
            raise

    @task
    def execute_insert(sql_command):
        connection_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        db_connection = connection_hook.get_conn()
        try:
            with db_connection.cursor() as cursor:
                # Split the SQL into individual statements
                sql_statements = sql_command.split(';')
                for sql in sql_statements:
                    # Avoid executing empty statements
                    if sql.strip():
                        cursor.execute(sql)
                        logging.info("Executed SQL statement successfully.")
        except Exception as ex:
            logging.error(f"Error inserting data into Snowflake: {str(ex)}")
            raise
        finally:
            db_connection.close()


    verify_data_in_snowflake = SnowflakeOperator(
        task_id='validate_data_in_snowflake',
        snowflake_conn_id='snowflake_conn',
        sql="""
        USE DATABASE STOCK_PRICE_DB;
        USE SCHEMA RAW_DATA;
        SELECT COUNT(*), AVG(close)
        FROM VANTAGE_API;
        """,
        warehouse='COMPUTE_WH'
    )