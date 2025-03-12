update dim_exchange_rates
set exchange_rate = (select stg_exchange_rates.exchange_rate
                     from stg_exchange_rates
                     where stg_exchange_rates.exchange_date = dim_exchange_rates.exchange_date
                       and stg_exchange_rates.currency = dim_exchange_rates.currency),
    ods_insert_date = (
    select stg_exchange_rates.ods_insert_date
    from stg_exchange_rates
    where stg_exchange_rates.exchange_date = dim_exchange_rates.exchange_date
                       and stg_exchange_rates.currency = dim_exchange_rates.currency
                                )
where exists (select 1
              from stg_exchange_rates
              where stg_exchange_rates.exchange_date = dim_exchange_rates.exchange_date
                and stg_exchange_rates.currency = dim_exchange_rates.currency
                AND (stg_exchange_rates.exchange_rate != dim_exchange_rates.exchange_rate or
                     stg_exchange_rates.ods_insert_date != dim_exchange_rates.ods_insert_date));
