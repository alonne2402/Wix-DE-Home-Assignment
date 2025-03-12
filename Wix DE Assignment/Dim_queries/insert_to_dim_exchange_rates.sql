insert into dim_exchange_rates (exchange_date, currency, exchange_rate, ods_insert_date)
select exchange_date, currency, exchange_rate, ods_insert_date
from stg_exchange_rates
where not exists (
    select 1
    from dim_exchange_rates
    where dim_exchange_rates.exchange_date = stg_exchange_rates.exchange_date
    and dim_exchange_rates.currency = stg_exchange_rates.currency
);