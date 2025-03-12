update fact_stocks_and_exchange_rates
set open_price_usd = (select final_stg.open_price_usd
                     from final_stg
                     where final_stg.stock_symbol = fact_stocks_and_exchange_rates.stock_symbol
                       and final_stg.date = fact_stocks_and_exchange_rates.date
                       and final_stg.currency = fact_stocks_and_exchange_rates.currency),

    high_price_usd = (select final_stg.high_price_usd
                     from final_stg
                     where final_stg.stock_symbol = fact_stocks_and_exchange_rates.stock_symbol
                       and final_stg.date = fact_stocks_and_exchange_rates.date
                     and final_stg.currency = fact_stocks_and_exchange_rates.currency),

    low_price_usd = (select final_stg.low_price_usd
                     from final_stg
                     where final_stg.stock_symbol = fact_stocks_and_exchange_rates.stock_symbol
                       and final_stg.date = fact_stocks_and_exchange_rates.date
                     and final_stg.currency = fact_stocks_and_exchange_rates.currency),

    close_price_usd = (select final_stg.close_price_by_currency
                     from final_stg
                     where final_stg.stock_symbol = fact_stocks_and_exchange_rates.stock_symbol
                       and final_stg.date = fact_stocks_and_exchange_rates.date
                     and final_stg.currency = fact_stocks_and_exchange_rates.currency),

    volume = (select final_stg.volume
                     from final_stg
                     where final_stg.stock_symbol = fact_stocks_and_exchange_rates.stock_symbol
                       and final_stg.date = fact_stocks_and_exchange_rates.date
                     and final_stg.currency = fact_stocks_and_exchange_rates.currency),

    transactions = (select final_stg.transactions
                     from final_stg
                     where final_stg.stock_symbol = fact_stocks_and_exchange_rates.stock_symbol
                       and final_stg.date = fact_stocks_and_exchange_rates.date
                     and final_stg.currency = fact_stocks_and_exchange_rates.currency),

    exchange_rate = (select final_stg.exchange_rate
                     from final_stg
                     where final_stg.stock_symbol = fact_stocks_and_exchange_rates.stock_symbol
                       and final_stg.date = fact_stocks_and_exchange_rates.date
                     and final_stg.currency = fact_stocks_and_exchange_rates.currency),

    open_price_by_currency = (select final_stg.open_price_by_currency
                     from final_stg
                     where final_stg.stock_symbol = fact_stocks_and_exchange_rates.stock_symbol
                       and final_stg.date = fact_stocks_and_exchange_rates.date
                     and final_stg.currency = fact_stocks_and_exchange_rates.currency),

    high_price_by_currency = (select final_stg.high_price_by_currency
                     from final_stg
                     where final_stg.stock_symbol = fact_stocks_and_exchange_rates.stock_symbol
                       and final_stg.date = fact_stocks_and_exchange_rates.date
                     and final_stg.currency = fact_stocks_and_exchange_rates.currency),

    close_price_by_currency = (select final_stg.close_price_by_currency
                     from final_stg
                     where final_stg.stock_symbol = fact_stocks_and_exchange_rates.stock_symbol
                       and final_stg.date = fact_stocks_and_exchange_rates.date
                     and final_stg.currency = fact_stocks_and_exchange_rates.currency),

    open_price_by_currency = (select final_stg.open_price_by_currency
                     from final_stg
                     where final_stg.stock_symbol = fact_stocks_and_exchange_rates.stock_symbol
                       and final_stg.date = fact_stocks_and_exchange_rates.date
                     and final_stg.currency = fact_stocks_and_exchange_rates.currency),

    ods_stocks_insert_date = (select final_stg.ods_stocks_insert_date
                     from final_stg
                     where final_stg.stock_symbol = fact_stocks_and_exchange_rates.stock_symbol
                       and final_stg.date = fact_stocks_and_exchange_rates.date
                     and final_stg.currency = fact_stocks_and_exchange_rates.currency),

    ods_exchange_rates_insert_date = (select final_stg.ods_exchange_rates_insert_date
                     from final_stg
                     where final_stg.stock_symbol = fact_stocks_and_exchange_rates.stock_symbol
                       and final_stg.date = fact_stocks_and_exchange_rates.date
                     and final_stg.currency = fact_stocks_and_exchange_rates.currency)
where exists (select 1
              from final_stg
              where final_stg.stock_symbol = fact_stocks_and_exchange_rates.stock_symbol
                and final_stg.date = fact_stocks_and_exchange_rates.date
                and final_stg.currency = fact_stocks_and_exchange_rates.currency
                and (
                    final_stg.open_price_usd != fact_stocks_and_exchange_rates.open_price_usd or
                    final_stg.high_price_usd != fact_stocks_and_exchange_rates.high_price_usd or
                    final_stg.low_price_usd != fact_stocks_and_exchange_rates.low_price_usd or
                    final_stg.close_price_usd != fact_stocks_and_exchange_rates.close_price_usd or
                    final_stg.volume != fact_stocks_and_exchange_rates.volume or
                    final_stg.transactions != fact_stocks_and_exchange_rates.transactions or
                    final_stg.exchange_rate != fact_stocks_and_exchange_rates.exchange_rate or
                    final_stg.open_price_by_currency != fact_stocks_and_exchange_rates.open_price_by_currency or
                    final_stg.high_price_by_currency != fact_stocks_and_exchange_rates.high_price_by_currency or
                    final_stg.low_price_by_currency != fact_stocks_and_exchange_rates.low_price_by_currency or
                    final_stg.close_price_by_currency != fact_stocks_and_exchange_rates.close_price_by_currency
                  ));
