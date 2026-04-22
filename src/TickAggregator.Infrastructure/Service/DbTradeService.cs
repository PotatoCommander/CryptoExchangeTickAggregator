using Npgsql;
using TickAggregator.Infrastructure.Model;

namespace TickAggregator.Infrastructure.Service
{
    public class DbTradeService : IAsyncDisposable
    {
        private readonly NpgsqlDataSource _dataSource;

        public DbTradeService(string connectionString)
        {
            _dataSource = NpgsqlDataSource.Create(connectionString);
        }

        public async Task EnsureSchemaAsync(CancellationToken ct)
        {
            const string sql = """
                               CREATE TABLE IF NOT EXISTS exchange_trades (
                                   id               BIGSERIAL       PRIMARY KEY,
                                   source           TEXT            NOT NULL,
                                   symbol           TEXT            NOT NULL,
                                   trade_id         TEXT            NOT NULL,
                                   price            NUMERIC(20,10)  NOT NULL,
                                   quantity         NUMERIC(20,10)  NOT NULL,
                                   side             TEXT            NOT NULL,
                                   ts_exchange      TIMESTAMPTZ     NOT NULL,
                                   received_at_utc  TIMESTAMPTZ     NOT NULL,
                                   CONSTRAINT ux_exchange_trades_source_symbol_trade_id UNIQUE (source, symbol, trade_id)
                               );
                               CREATE INDEX IF NOT EXISTS ix_exchange_trades_symbol_ts ON exchange_trades (symbol, ts_exchange DESC);
                               CREATE INDEX IF NOT EXISTS ix_exchange_trades_source_ts ON exchange_trades (source, ts_exchange DESC);
                               """;

            await using var cmd = _dataSource.CreateCommand(sql);
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        public async Task WriteAsync(IReadOnlyList<ExchangeTradeModel> batch, CancellationToken cancellationToken)
        {
            if (batch.Count == 0)
            {
                return;
            }

            await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var npgsqlBatch = new NpgsqlBatch(conn);

            foreach (var trade in batch)
            {
                var command = new NpgsqlBatchCommand("""
                                                     INSERT INTO exchange_trades (source, symbol, trade_id, price, quantity, side, ts_exchange, received_at_utc)
                                                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                                                     ON CONFLICT (source, symbol, trade_id) DO NOTHING
                                                     """);

                command.Parameters.Add(new NpgsqlParameter { Value = trade.Source });
                command.Parameters.Add(new NpgsqlParameter { Value = trade.Symbol });
                command.Parameters.Add(new NpgsqlParameter { Value = trade.TradeId });
                command.Parameters.Add(new NpgsqlParameter { Value = trade.Price });
                command.Parameters.Add(new NpgsqlParameter { Value = trade.Volume });
                command.Parameters.Add(new NpgsqlParameter { Value = trade.Side.ToString() });
                command.Parameters.Add(new NpgsqlParameter { Value = trade.TimestampUtc });
                command.Parameters.Add(new NpgsqlParameter { Value = trade.ReceivedAtUtc });
                npgsqlBatch.BatchCommands.Add(command);
            }

            await npgsqlBatch.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        public ValueTask DisposeAsync() => _dataSource.DisposeAsync();
    }
}
