select max(increment_column) as increment_column
                                   from (select max(ods_insert_date) as increment_column
                                         from dim_exchange_rates

                                         union all

                                         select '2024-01-01 00:00:00' as increment_column)