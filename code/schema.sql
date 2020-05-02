DROP SCHEMA IF EXISTS stocks CASCADE;
CREATE SCHEMA stocks;
DROP TABLE IF EXISTS company_information;
DROP TABLE IF EXISTS historical_stock_prices;

CREATE TABLE company_information(
	ticker VARCHAR(7),
	exchange VARCHAR(7),
	name VARCHAR(127),
	sector VARCHAR(127),
	industry VARCHAR(127),
	PRIMARY KEY(ticker)
);

CREATE TABLE historical_stock_prices(
    ticker VARCHAR(7) REFERENCES company_information(ticker),
    open NUMERIC(4, 2),
    close NUMERIC(4, 2),
    adj_close NUMERIC(4, 2),
    low NUMERIC(4, 2),
    high NUMERIC(4, 2),
    volume BIGINT,
    trade_date DATE,
    PRIMARY KEY(ticker, trade_date)
);
