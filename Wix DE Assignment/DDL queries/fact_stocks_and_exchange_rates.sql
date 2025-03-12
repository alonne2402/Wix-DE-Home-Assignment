CREATE TABLE IF NOT EXISTS fact_stocks_and_exchange_rates (
    stock_symbol STRING NOT NULL,
    date DATE NOT NULL,
    open_price_usd FLOAT CHECK (open_price_usd >0),
    high_price_usd FLOAT CHECK (high_price_usd >0),
    low_price_usd FLOAT CHECK (low_price_usd >0),
    close_price_usd FLOAT CHECK (close_price_usd >0),
    volume INT CHECK (volume >= 0),
    transactions INT CHECK (transactions >= 0),
    currency STRING,
    exchange_rate FLOAT CHECK (exchange_rate > 0),
    open_price_by_currency FLOAT CHECK (open_price_by_currency > 0),
    high_price_by_currency FLOAT CHECK (high_price_by_currency > 0),
    low_price_by_currency FLOAT CHECK (low_price_by_currency > 0),
    close_price_by_currency FLOAT CHECK (close_price_by_currency > 0),
    ods_stocks_insert_date TIMESTAMP,
    ods_exchange_rates_insert_date TIMESTAMP,
    PRIMARY KEY(stock_symbol, date, currency)
)
