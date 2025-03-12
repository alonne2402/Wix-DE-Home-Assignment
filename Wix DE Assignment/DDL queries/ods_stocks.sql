CREATE TABLE IF NOT EXISTS ods_stocks (
    stock_symbol STRING NOT NULL,
    stock_date DATE NOT NULL,
    open_price FLOAT CHECK (open_price >0),
    high_price FLOAT CHECK (high_price >0),
    low_price FLOAT CHECK (low_price >0),
    close_price FLOAT CHECK (close_price >0),
    volume INT CHECK (volume >= 0),
    transactions INT CHECK (transactions >= 0),
    partition_time TIMESTAMP
)



