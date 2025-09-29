IF OBJECT_ID('dbo.trades','U') IS NULL
BEGIN
  CREATE TABLE dbo.trades(
    trade_id        varchar(50)  NOT NULL,
    counterparty_id varchar(50)  NOT NULL,
    book_id         varchar(50)  NOT NULL,
    [version]       int          NOT NULL,
    maturity_date   date         NULL,
    created_date    date         NULL,
    expired         bit          NOT NULL,
    CONSTRAINT PK_trades PRIMARY KEY (trade_id,[version],book_id)
  );
END