use nifty_stock_data;

CREATE TABLE stock_hist (
    Date DATE,
    Stock VARCHAR(20),
    Open DOUBLE,
    High DOUBLE,
    Low DOUBLE,
    Close DOUBLE,
    `Adj Close` DOUBLE,
    Volume BIGINT,

    Stock_return DOUBLE,
    Index_return DOUBLE,
    Alpha DOUBLE,
    Volatility DOUBLE,

    MA50 DOUBLE,
    MA200 DOUBLE,

    Trend_signal BOOLEAN,
    Sharpe DOUBLE,
    Market_crash BOOLEAN,

    Sector VARCHAR(50),
    Industry VARCHAR(100),

    Return_30d DOUBLE,
    Return_90d DOUBLE,
    Return_1y DOUBLE,

    Volatility_trend DOUBLE,
    Momentum DOUBLE
);

DESCRIBE stock_hist;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Final_stock_analysis.csv'
INTO TABLE stock_hist
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@Date, Stock, @Open, @High, @Low, @Close, @AdjClose, @Volume,
 @Stock_return, @Index_return, @Alpha, @Volatility,
 @MA50, @MA200, @Trend_signal, @Sharpe, @Market_crash,
 Sector, Industry, @Return_30d, @Return_90d, @Return_1y,
 @Volatility_trend, @Momentum)
SET 
    Date = STR_TO_DATE(@Date, '%d-%m-%Y'),

    Open = IF(TRIM(@Open) REGEXP '^-?[0-9]+(\\.[0-9]+)?$', @Open, NULL),
    High = IF(TRIM(@High) REGEXP '^-?[0-9]+(\\.[0-9]+)?$', @High, NULL),
    Low = IF(TRIM(@Low) REGEXP '^-?[0-9]+(\\.[0-9]+)?$', @Low, NULL),
    Close = IF(TRIM(@Close) REGEXP '^-?[0-9]+(\\.[0-9]+)?$', @Close, NULL),
    `Adj Close` = IF(TRIM(@AdjClose) REGEXP '^-?[0-9]+(\\.[0-9]+)?$', @AdjClose, NULL),
    Volume = IF(TRIM(@Volume) REGEXP '^[0-9]+$', @Volume, NULL),

    Stock_return = IF(TRIM(@Stock_return) REGEXP '^-?[0-9]+(\\.[0-9]+)?$', @Stock_return, NULL),
    Index_return = IF(TRIM(@Index_return) REGEXP '^-?[0-9]+(\\.[0-9]+)?$', @Index_return, NULL),
    Alpha = IF(TRIM(@Alpha) REGEXP '^-?[0-9]+(\\.[0-9]+)?$', @Alpha, NULL),
    Volatility = IF(TRIM(@Volatility) REGEXP '^-?[0-9]+(\\.[0-9]+)?$', @Volatility, NULL),

    MA50 = IF(TRIM(@MA50) REGEXP '^-?[0-9]+(\\.[0-9]+)?$', @MA50, NULL),
    MA200 = IF(TRIM(@MA200) REGEXP '^-?[0-9]+(\\.[0-9]+)?$', @MA200, NULL),

    Sharpe = IF(TRIM(@Sharpe) REGEXP '^-?[0-9]+(\\.[0-9]+)?$', @Sharpe, NULL),

    Return_30d = IF(TRIM(@Return_30d) REGEXP '^-?[0-9]+(\\.[0-9]+)?$', @Return_30d, NULL),
    Return_90d = IF(TRIM(@Return_90d) REGEXP '^-?[0-9]+(\\.[0-9]+)?$', @Return_90d, NULL),
    Return_1y = IF(TRIM(@Return_1y) REGEXP '^-?[0-9]+(\\.[0-9]+)?$', @Return_1y, NULL),

    Volatility_trend = IF(TRIM(@Volatility_trend) REGEXP '^-?[0-9]+(\\.[0-9]+)?$', @Volatility_trend, NULL),
    Momentum = IF(TRIM(@Momentum) REGEXP '^-?[0-9]+(\\.[0-9]+)?$', @Momentum, NULL),

    Trend_signal = IF(@Trend_signal = 'TRUE', 1, 0),
    Market_crash = IF(@Market_crash = 'TRUE', 1, 0);
    
    
select * from stock_hist;


-----------------------------------------------------------------------------------------------------------------------

CREATE TABLE index_data (
    Date DATE,
    Index_Close DOUBLE,
    Index_return DOUBLE
);


LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Nifty50.csv'
INTO TABLE index_data
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@Date, Index_Close, Index_return)
SET Date = STR_TO_DATE(@Date, '%d-%m-%Y');


select * from index_data;

-----------------------------------------------------------------------------------------------------------

CREATE TABLE fundamental_data (
    Stock VARCHAR(20),

    Market_Cap DOUBLE,
    PE_Ratio DOUBLE,
    PB_Ratio DOUBLE,
    ROE DOUBLE,
    ROA DOUBLE,

    Debt_to_Equity DOUBLE,
    EPS DOUBLE,
    Revenue DOUBLE,

    Profit_Margin DOUBLE,
    Dividend_Yield DOUBLE,

    Sector VARCHAR(50),
    Industry VARCHAR(100)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Nifty50_fundamental_data.csv'
INTO TABLE fundamental_data
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(Stock, @Market_Cap, @PE_Ratio, @PB_Ratio, @ROE, @ROA,
 @Debt_to_Equity, @EPS, @Revenue, @Profit_Margin,
 @Dividend_Yield, Sector, Industry)
SET
    Market_Cap = IF(TRIM(@Market_Cap) REGEXP '^[0-9.eE+-]+$', @Market_Cap, NULL),
    PE_Ratio = IF(TRIM(@PE_Ratio) REGEXP '^-?[0-9]+(\\.[0-9]+)?$', @PE_Ratio, NULL),
    PB_Ratio = IF(TRIM(@PB_Ratio) REGEXP '^-?[0-9]+(\\.[0-9]+)?$', @PB_Ratio, NULL),
    ROE = IF(TRIM(@ROE) REGEXP '^-?[0-9]+(\\.[0-9]+)?$', @ROE, NULL),
    ROA = IF(TRIM(@ROA) REGEXP '^-?[0-9]+(\\.[0-9]+)?$', @ROA, NULL),

    Debt_to_Equity = IF(TRIM(@Debt_to_Equity) REGEXP '^-?[0-9]+(\\.[0-9]+)?$', @Debt_to_Equity, NULL),

    EPS = IF(TRIM(@EPS) REGEXP '^-?[0-9]+(\\.[0-9]+)?$', @EPS, NULL),
    Revenue = IF(TRIM(@Revenue) REGEXP '^[0-9.eE+-]+$', @Revenue, NULL),

    Profit_Margin = IF(TRIM(@Profit_Margin) REGEXP '^-?[0-9]+(\\.[0-9]+)?$', @Profit_Margin, NULL),
    Dividend_Yield = IF(TRIM(@Dividend_Yield) REGEXP '^-?[0-9]+(\\.[0-9]+)?$', @Dividend_Yield, NULL);
    
-----------------------------------------------------------------------------------------------------------------------------

alter table stock_hist add PRIMARY KEY (Stock, Date);

alter table index_data add PRIMARY KEY (Date);

alter table fundamental_data add PRIMARY KEY (Stock);

alter table stock_hist add FOREIGN KEY (Stock) REFERENCES fundamental_data(Stock);

SELECT 
    s.Stock,
    s.Date,
    s.Close,
    s.Volume,

    f.PE_Ratio,
    f.ROE,
    f.Sector,
    f.Industry,

    i.Index_Close,
    i.Index_return

FROM stock_hist s

LEFT JOIN fundamental_data f 
    ON s.Stock = f.Stock

LEFT JOIN index_data i 
    ON s.Date = i.Date;
    
----------------------------------------------------------------------------------------------------------------------

