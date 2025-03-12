select  max(increment_column) as increment_column
    from (select max(ods_stocks_insert_date) as increment_column
          from fact_stocks_and_exchange_rates

          union all

          select '2024-01-01 00:00:00' as increment_column)
;

