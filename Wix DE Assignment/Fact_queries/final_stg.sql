select ifnull(stock_symbol,'NA') as stock_symbol,
       ifnull(stock_date,'2999-01-01') as date,
       open_price as open_price_usd,
       high_price as high_price_usd,
       low_price as low_price_usd,
       close_price as close_price_usd,
       volume,
       transactions,
       currency,
       exchange_rate,
       round(open_price*exchange_rate,4) as open_price_by_currency,
       round(high_price*exchange_rate,4) as high_price_by_currency,
       round(low_price*exchange_rate,4) as low_price_by_currency,
       round(close_price*exchange_rate,4) as close_price_by_currency,
       partition_time as ods_stocks_insert_date,
       ods_insert_date as ods_exchange_rates_insert_date
    from stg_stocks left join dim_exchange_rates
on stg_stocks.stock_date = dim_exchange_rates.exchange_date
