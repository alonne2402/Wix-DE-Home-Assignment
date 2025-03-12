select stock_symbol,
       stock_date,
       open_price,
       high_price,
       low_price,
       close_price,
       volume,
       transactions,
       partition_time
from (select stock_symbol,
             stock_date,
             open_price,
             high_price,
             low_price,
             close_price,
             volume,
             transactions,
             partition_time,
             row_number() over (partition by stock_symbol,stock_date order by partition_time desc) as rank
      from ods_stocks
      where partition_time >= (select increment_column from stock_data_increment))
where rank = 1