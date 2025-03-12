CREATE TABLE IF NOT EXISTS dim_exchange_rates (
   exchange_date DATE NOT NULL,
   currency STRING NOT NULL,
   exchange_rate FLOAT CHECK(exchange_rate > 0),
   ods_insert_date TIMESTAMP,
   PRIMARY KEY (exchange_date, currency)
)