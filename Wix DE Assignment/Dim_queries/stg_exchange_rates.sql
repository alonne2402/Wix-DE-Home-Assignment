select exchange_date,
       currency,
       exchange_rate,
       partition_time as ods_insert_date
    from (select exchange_date,
                 currency,
                 exchange_rate,
                 partition_time,
                 row_number() over (partition by exchange_date, currency order by partition_time desc) as rank
          from ods_exchange_rates
          where partition_time >= (select increment_column from exchange_rates_increment_for_dim))
where rank = 1


