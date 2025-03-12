insert into fact_stocks_and_exchange_rates (stock_symbol, date, open_price_usd, high_price_usd, low_price_usd, close_price_usd, volume, transactions, currency, exchange_rate, open_price_by_currency, high_price_by_currency, low_price_by_currency, close_price_by_currency, ods_stocks_insert_date, ods_exchange_rates_insert_date)
select stock_symbol,
       date,
       open_price_usd,
       high_price_usd,
       low_price_usd,
       close_price_usd,
       volume,
       transactions,
       currency,
       exchange_rate,
       open_price_by_currency,
       high_price_by_currency,
       low_price_by_currency,
       close_price_by_currency,
       ods_stocks_insert_date,
       ods_exchange_rates_insert_date
from final_stg
where not exists (
    select 1
    from fact_stocks_and_exchange_rates
    where fact_stocks_and_exchange_rates.stock_symbol = final_stg.stock_symbol
    and fact_stocks_and_exchange_rates.date = final_stg.date
    and fact_stocks_and_exchange_rates.currency = final_stg.currency
);
