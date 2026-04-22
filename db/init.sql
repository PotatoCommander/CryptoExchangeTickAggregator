CREATE TABLE IF NOT EXISTS exchange_trades (
    source           TEXT            NOT NULL,
    symbol           TEXT            NOT NULL,
    trade_id         TEXT            NOT NULL,
    price            NUMERIC(20,10)  NOT NULL,
    quantity         NUMERIC(20,10)  NOT NULL,
    side             TEXT            NOT NULL,
    ts_exchange      TIMESTAMPTZ     NOT NULL,
    received_at_utc  TIMESTAMPTZ     NOT NULL,
    PRIMARY KEY (source, symbol, trade_id)
);

CREATE INDEX IF NOT EXISTS ix_exchange_trades_symbol_ts ON exchange_trades (symbol, ts_exchange DESC);
CREATE INDEX IF NOT EXISTS ix_exchange_trades_source_ts ON exchange_trades (source, ts_exchange DESC);
