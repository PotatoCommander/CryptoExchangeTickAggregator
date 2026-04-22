using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using TickAggregator.Infrastructure.Connectors;
using TickAggregator.Infrastructure.Connectors.Binance;
using TickAggregator.Infrastructure.Connectors.Bybit;
using TickAggregator.Infrastructure.Service;
using TickAggregator.Model;
using TickAggregator.Service;

namespace TickAggregator
{
    public static class Program
    {
        public static async Task<int> Main()
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json", optional: false)
                .AddEnvironmentVariables(prefix: "TICKS_")
                .Build();

            using var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder
                    .AddConfiguration(config.GetSection("Logging"))
                    .AddSimpleConsole(o =>
                    {
                        o.TimestampFormat = "HH:mm:ss.fff ";
                        o.SingleLine = true;
                    })
                    .SetMinimumLevel(LogLevel.Information);
            });

            var logger = loggerFactory.CreateLogger("TickAggregator");
            using var cts = new CancellationTokenSource();

            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                logger.LogInformation("Cancel received, shutting down...");
                cts.Cancel();
            };

            var connectionString = config["Postgres:ConnectionString"]
                ?? throw new InvalidOperationException("Postgres:ConnectionString is not configured");
            await using var dbTradeService = new DbTradeService(connectionString);

            try
            {
                await dbTradeService.EnsureSchemaAsync(cts.Token);
            }
            catch (Exception ex)
            {
                logger.LogCritical(ex, "Failed to connect to Postgres. Is docker compose up? {Connection}", connectionString);
                return 1;
            }

            var counter = new TickCounter(loggerFactory.CreateLogger<TickCounter>());
            var writerOptions = new TickWriterOptions
            {
                MaxBatchSize = config.GetValue<int?>("Batch:MaxSize") ?? 500,
                FlushInterval = TimeSpan.FromMilliseconds(config.GetValue<int?>("Batch:FlushIntervalMs") ?? 200),
                DedupWindowSize = config.GetValue<int?>("Dedup:WindowSize") ?? 50_000
            };

            var writer = new TickWriter(
                sink: dbTradeService,
                options: writerOptions,
                counter: counter,
                logger: loggerFactory.CreateLogger<TickWriter>());

            var bybitUri = new Uri(config["Connectors:Bybit:Uri"] ?? "wss://stream.bybit.com/v5/public/spot");
            var binanceBaseUri = new Uri(config["Connectors:Binance:BaseUri"] ?? "wss://stream.binance.com:9443/stream");
            var noDataTimeout = TimeSpan.FromSeconds(config.GetValue<int?>("Connectors:NoDataTimeoutSeconds") ?? 30);

            var connectors = new List<(IExchangeConnector Connector, string[] Symbols)>();

            var bybitSymbols = config.GetSection("Symbols:Bybit").Get<string[]>() ?? [];
            if (bybitSymbols.Length > 0)
            {
                connectors.Add((new BybitConnector(bybitUri, noDataTimeout, loggerFactory.CreateLogger<BybitConnector>()), bybitSymbols));
            }

            var binanceSymbols = config.GetSection("Symbols:Binance").Get<string[]>() ?? [];
            if (binanceSymbols.Length > 0)
            {
                connectors.Add((new BinanceConnector(binanceBaseUri, noDataTimeout, loggerFactory.CreateLogger<BinanceConnector>()), binanceSymbols));
            }

            if (connectors.Count == 0)
            {
                logger.LogCritical("No connectors configured. Populate Symbols:Bybit and/or Symbols:Binance.");
                return 1;
            }

            var reportingInterval = TimeSpan.FromSeconds(config.GetValue<int?>("MonitoringIntervalSeconds") ?? 5);

            logger.LogInformation(
                "Starting with {Count} connector(s), batch={Batch}, flush={Flush}ms, dedup-window={Dedup}",
                connectors.Count,
                writerOptions.MaxBatchSize,
                writerOptions.FlushInterval.TotalMilliseconds,
                writerOptions.DedupWindowSize);

            var writerTask = writer.RunAsync(cts.Token);
            var reporterTask = counter.RunRateReporterAsync(reportingInterval, cts.Token);
            var connectorTasks = connectors
                .Select(c => c.Connector.SubscribeToTradesAsync(c.Symbols, writer.OnTick, cts.Token))
                .ToArray();

            try
            {
                await Task.WhenAll(connectorTasks);
            }
            catch (OperationCanceledException) when (cts.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Connector task faulted");
            }
            finally
            {
                writer.Complete();
                try { await writerTask; } catch { }
                try { await reporterTask; } catch { }
            }

            logger.LogInformation(
                "Shutdown complete. received={Received} written={Written} dupes={Dupes}",
                counter.Received,
                counter.Written,
                counter.Duplicates);

            return 0;
        }
    }
}
