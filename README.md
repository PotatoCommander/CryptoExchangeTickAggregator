# TickAggregator

Small console app that listens to trade streams from Bybit and Binance, normalizes trades, removes duplicates, and writes them to Postgres.

For this test task I treat a "ticker" as a trade event: price, volume, symbol, and trade timestamp.

## What it does

- listens to Bybit `publicTrade` and Binance `@trade`
- maps both sources into one `ExchangeTradeModel`
- deduplicates by `(source, symbol, trade_id)`
- writes to `exchange_trades` in batches via `NpgsqlBatch`
- reconnects if the socket drops or if no data is received for too long

## Run

```powershell
docker compose up -d
dotnet run --project .\src\TickAggregator\
```

Run tests:

```powershell
dotnet test
```

Stop the app with `Ctrl+C`.

Remove local Postgres together with its volume:

```powershell
.\cleanup.ps1
```

## Config

Main config file: [`src/TickAggregator/appsettings.json`](src/TickAggregator/appsettings.json)

Important settings:

- `Postgres:ConnectionString`
- `Symbols:Bybit`
- `Symbols:Binance`
- `Batch:MaxSize`
- `Batch:FlushIntervalMs`
- `Dedup:WindowSize`
- `Connectors:NoDataTimeoutSeconds`

You can override any of them via env vars with the `TICKS_` prefix.

Example:

```powershell
$env:TICKS_Symbols__Bybit__0="BTCUSDT"
$env:TICKS_Symbols__Binance__0="btcusdt"
```

## Structure

- [`src/TickAggregator/TickAggregator.App.csproj`](src/TickAggregator/TickAggregator.App.csproj) - entry point, orchestration, writer, counters
- [`src/TickAggregator.Infrastructure/TickAggregator.Infrastructure.csproj`](src/TickAggregator.Infrastructure/TickAggregator.Infrastructure.csproj) - shared model, interfaces, Postgres
- [`src/Connectors/Bybit/TickAggregator.Connectors.Bybit.csproj`](src/Connectors/Bybit/TickAggregator.Connectors.Bybit.csproj) - Bybit connector
- [`src/Connectors/Binance/TickAggregator.Connectors.Binance.csproj`](src/Connectors/Binance/TickAggregator.Connectors.Binance.csproj) - Binance connector
- [`src/WebSocketClient/WebSocketClient.csproj`](src/WebSocketClient/WebSocketClient.csproj) - small websocket library with reconnect, heartbeat, and timeout

Data flow:

```text
Connector -> TickWriter.OnTick -> Channel -> DeduplicationFilter -> batch -> DbTradeService -> PostgreSQL
```

## Database

The table is created automatically on startup if it does not exist yet.

Main table:

- `exchange_trades`

Primary key:

- `PRIMARY KEY (source, symbol, trade_id)`

This is the second protection layer against duplicates after in-memory deduplication.

Quick check:

```powershell
docker exec -it tickaggregator-postgres psql -U ticks -d ticks -c "
SELECT source, symbol, trade_id, price, quantity, side, ts_exchange
FROM exchange_trades
ORDER BY ts_exchange DESC
LIMIT 20;
"
```

## Reconnect behavior

- if the connection drops, the client waits for a fixed delay and reconnects
- if no messages are received for too long, the connection is treated as stale and recreated
- after reconnect, subscription is sent again
- duplicates are filtered first in memory and then again in Postgres via `ON CONFLICT DO NOTHING`

## Tests

There are unit tests for:

- Binance message parsing
- Bybit message parsing
- deduplication
- writer behavior
- reconnect / timeout behavior in the websocket client

There is no integration test with a real Postgres right now. That part is verified manually with `docker compose up` + `dotnet run`.
