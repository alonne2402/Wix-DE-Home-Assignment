CREATE TABLE IF NOT EXISTS ods_exchange_rates (
   exchange_date DATE NOT NULL,
   currency STRING NOT NULL,
   exchange_rate FLOAT CHECK(exchange_rate > 0),
   partition_time TIMESTAMP
)
