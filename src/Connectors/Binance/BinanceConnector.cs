using System.Globalization;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using TickAggregator.Infrastructure.Model;
using WebSocketClient;

namespace TickAggregator.Infrastructure.Connectors.Binance
{
    public sealed class BinanceConnector : IExchangeConnector
    {
        private readonly Uri _baseUri;
        private readonly TimeSpan? _connectionTimeout;
        private readonly ILogger<BinanceConnector> _logger;

        public BinanceConnector(Uri baseUri, TimeSpan? connectionTimeout, ILogger<BinanceConnector> logger)
        {
            _baseUri = baseUri;
            _connectionTimeout = connectionTimeout;
            _logger = logger;
        }

        public string Name => "binance";

        public Task SubscribeToTradesAsync(IReadOnlyCollection<string> symbols, Action<ExchangeTradeModel> onMessage, CancellationToken cancellationToken)
        {
            var streams = symbols.Select(s => s.ToLowerInvariant()).Distinct().Select(s => $"{s}@trade").ToArray();
            var uri = new Uri($"{_baseUri}?streams={string.Join('/', streams)}");

            var options = new WebSocketOptions
            {
                Name = Name,
                Uri = uri,
                ConnectionTimeout = _connectionTimeout,
                OnConnectedAsync = (_, _) =>
                {
                    _logger.LogInformation("[{Name}] subscribed via URL: {Streams}", Name, string.Join(", ", streams));
                    return Task.CompletedTask;
                },
                OnMessage = payload =>
                {
                    try
                    {
                        var trade = ParseTrade(payload.Span);
                        if (trade is not null)
                        {
                            onMessage(trade);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "[{Name}] failed to parse message", Name);
                    }
                }
            };

            return new ExchangeWebSocketClient(options, _logger).ConnectAsync(cancellationToken);
        }

        public static ExchangeTradeModel? ParseTrade(ReadOnlySpan<byte> json)
        {
            var trade = JsonSerializer.Deserialize<BinanceTrade>(json);
            var data = trade?.Data;
            if (data?.EventType != "trade")
            {
                return null;
            }

            return new ExchangeTradeModel
            {
                Source = "binance",
                Symbol = data.Symbol,
                TradeId = data.TradeId.ToString(CultureInfo.InvariantCulture),
                Price = decimal.Parse(data.Price, CultureInfo.InvariantCulture),
                Volume = decimal.Parse(data.Quantity, CultureInfo.InvariantCulture),
                Side = data.IsBuyerMarketMaker ? TradeSide.Sell : TradeSide.Buy,
                TimestampUtc = DateTimeOffset
                    .FromUnixTimeMilliseconds(data.TradeTime)
                    .UtcDateTime,
                ReceivedAtUtc = DateTime.UtcNow
            };
        }
    }
}
