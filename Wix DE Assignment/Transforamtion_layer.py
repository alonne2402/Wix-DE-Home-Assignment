import sqlite3
import time
from Polygon import*
from Frankfurter import*
from datetime import datetime, timedelta

# connect to SQLITE
conn = sqlite3.connect('database.sqlite')
# creating cursor for query execution
cursor = conn.cursor()
#number of requests Polygon allows per minute
max_requests_per_minute = 5

# read json configuration file
def read_config(json_file):
    with open(json_file, 'r') as config_file:
        config = json.load(config_file)
    return config

config = read_config("config.json")

polygon = PolygonAPI(config["polygon"]["api_key"], config["polygon"]["base_url"],
                    config["polygon"]["endpoints"]["us_stocks"], config["polygon"]["timeout"],
                    config["polygon"]["retries"])

frankfurter = FrankfurterAPI(None, config["frankfurter"]["base_url"], None, config["frankfurter"]["timeout"],
                        config["frankfurter"]["retries"])  # יצירת מופע של המחלקה


# read the SQL query files
def read_query(query):
    with open(query, 'r') as file:
        sql_script = file.read()
    return sql_script

# get the query result from the cursor
def get_query_results(query):
    cursor.execute(read_query(query))
    result = cursor.fetchall()
    column_names = [description[0] for description in cursor.description]
    df = pd.DataFrame(result, columns=column_names)
    return df

# insert the query result into staging table in the DB
def insert_query_result_to_stg(query, stg_table_name):
    df = get_query_results(query)
    df.to_sql(stg_table_name, conn, if_exists="replace", index=False)

# check for the first run of the process if the tables are empty
def is_empty_table(table_name):
    cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
    result = cursor.fetchone()[0]
    if result > 0:
        return False
    else:
        return True

# run history stocks data for the fist run of the process
def run_stocks_historical_data():
    back_fill_date = (datetime.now() - timedelta(days=10)).date() # I decided to take only last 10 days to validate the process
    now = datetime.now().date()
    stock_data = polygon.get_stock_data(back_fill_date)
    num_requests = 1
    while True:
        back_fill_date = back_fill_date + timedelta(days=1)
        if back_fill_date == now: # remove timedelta
            break
        # make sure only 5 requests sent to the API every minute
        if num_requests == max_requests_per_minute:
            num_requests = 1
            time.sleep(60)
        else:
            df = polygon.get_stock_data(back_fill_date)
            stock_data = pd.concat([stock_data,df], ignore_index=True)
            num_requests += 1

    stock_data['partition_time'] = datetime.now()
    stock_data.to_sql('ods_stocks', conn, if_exists='append', index=False)

# run stocks data for a single day (default=yesterday)
def run_stocks_yesterday_data(specific_date=None):
    try:
        if specific_date is None:
            date = (datetime.now() - timedelta(days=1)).date()
        else:
            date = specific_date
        print(datetime.now())
        print(date)
        stock_data = polygon.get_stock_data(date)
        # since SQLITE does not support partitioning tables while creating the table
        # I had to add manually a partition_time column into the table to know when was each inserted into the table
        stock_data['partition_time'] = datetime.now()
        stock_data.to_sql('ods_stocks', conn, if_exists='append', index=False)
    except:
        logging.error(f"Attempted to request {date} data before end of day, or there is no data for date: {date}")

# run exchange rates data (includes both history run and specific time range run)
def run_exchange_rates_data(boolean,start_date=None,end_date=None):
    if boolean:
        from_date = (datetime.now() - timedelta(days=10)).date() # I decided to take only last 10 days to validate the process
        to_date = (datetime.now() - timedelta(days=1)).date()
    elif start_date is None or end_date is None:
        from_date = (datetime.now() - timedelta(days=1)).date()
        to_date = from_date
    else:
        from_date = start_date
        to_date = end_date
    exchange_rates_data = frankfurter.get_exchange_rates_for_period(from_date, to_date)
    # since SQLITE does not support partitioning tables while creating the table
    # I had to add manually a partition_time column into the table to know when was each inserted into the table
    exchange_rates_data['partition_time'] = datetime.now()
    exchange_rates_data.to_sql('ods_exchange_rates', conn, if_exists='append', index=False)

# run executions before the first run of the process to create the tables needed
cursor.execute(read_query('DDL queries/ods_stocks.sql'))
cursor.execute(read_query('DDL queries/ods_exchange_rates.sql'))
cursor.execute(read_query('DDL queries/fact_stocks_and_exchange_rates.sql'))
cursor.execute(read_query('DDL queries/dim_exchange_rates.sql'))

# process starts by getting raw data for the ods_stocks and the ods_exchange_rates  tables
# if it's the first run the process runs history data for both tables otherwise will get yesterday's data

# 1. checking stocks history data
check_for_stocks_historical_data = is_empty_table('ods_stocks')

if check_for_stocks_historical_data:
    run_stocks_historical_data()
else:
    run_stocks_yesterday_data()
# 2. checking exchange rates history data
check_for_exchange_rates_historical_data = is_empty_table('ods_exchange_rates')
run_exchange_rates_data(check_for_stocks_historical_data)


# 3. checking incrementally when was the last timestamp a data was inserted into dim_exchange_rates (default for first run is '2024-01-10')
insert_query_result_to_stg(query='Dim_queries/exchange_rates_increment_for_dim.sql', stg_table_name='exchange_rates_increment_for_dim')
# 4. getting exchange rates data batch according to the increment
insert_query_result_to_stg(query='Dim_queries/stg_exchange_rates.sql', stg_table_name='stg_exchange_rates')
# since SQLITE does not support merge statement, I had to separate the merge to update and insert statements
# 5. update dim_exchange_rates tables according to the staging batch
cursor.execute(read_query('Dim_queries/update_dim_exchange_rates.sql'))
# 6. insert to dim_exchange_rates tables according to the staging batch
cursor.execute(read_query('Dim_queries/insert_to_dim_exchange_rates.sql'))
# 7. checking incrementally when was the last timestamp a data was inserted into fact_stocks_and_exchange_rates table
# (default for first run is '2024-01-10')
insert_query_result_to_stg(query='Fact_queries/stock_data_increment.sql', stg_table_name='stock_data_increment')
# 8. getting exchange rates data batch according to the increment
insert_query_result_to_stg(query='Fact_queries/stg_stocks.sql', stg_table_name='stg_stocks')
# 9. join data from stg_stocks batch with dim_exchange_rates data
insert_query_result_to_stg(query='Fact_queries/final_stg.sql', stg_table_name='final_stg')
# 10. update fact_stocks_and_exchange tables according to the staging final batch
cursor.execute(read_query('Fact_queries/update_fact_stocks_and_exchange_rates.sql'))
# 11. insert_into fact_stocks_and_exchange tables according to the staging final batch
cursor.execute(read_query('Fact_queries/insert_to_fact_stocks_and_exchange_rates.sql'))

# process flow:
# start >> [1,2]
# 2 >> 3
# 1 >> 3
# 3 >> 4 >> 5 >> 6 >> 7 >> 8 >> 9 >> 10 >> 11

conn.commit()
conn.close()
