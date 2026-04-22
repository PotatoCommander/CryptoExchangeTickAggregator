using System.Globalization;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using TickAggregator.Infrastructure.Model;
using WebSocketClient;

namespace TickAggregator.Infrastructure.Connectors.Bybit
{
    public sealed class BybitConnector : IExchangeConnector
    {
        private static readonly byte[] PingPayload = "{\"op\":\"ping\"}"u8.ToArray();

        private readonly Uri _wsUri;
        private readonly TimeSpan? _connectionTimeout;
        private readonly ILogger<BybitConnector> _logger;

        public BybitConnector(Uri wsUri, TimeSpan? connectionTimeout, ILogger<BybitConnector> logger)
        {
            _wsUri = wsUri;
            _connectionTimeout = connectionTimeout;
            _logger = logger;
        }

        public string Name => "bybit";

        public Task SubscribeToTradesAsync(IReadOnlyCollection<string> symbols, Action<ExchangeTradeModel> onMessage, CancellationToken cancellationToken)
        {
            var topics = symbols.Select(s => s.ToUpperInvariant()).Distinct().Select(s => $"publicTrade.{s}").ToArray();

            var options = new WebSocketOptions
            {
                Name = Name,
                Uri = _wsUri,
                PingInterval = TimeSpan.FromSeconds(20),
                PingPayloadFactory = () => PingPayload,
                ConnectionTimeout = _connectionTimeout,
                OnConnectedAsync = async (ws, ct) =>
                {
                    var subscribe = JsonSerializer.Serialize(new { op = "subscribe", args = topics });
                    await ws.SendAsync(Encoding.UTF8.GetBytes(subscribe), WebSocketMessageType.Text, true, ct).ConfigureAwait(false);
                    _logger.LogInformation("[{Name}] subscribed: {Topics}", Name, string.Join(", ", topics));
                },
                OnMessage = payload =>
                {
                    try
                    {
                        foreach (var trade in ParseTrades(payload.Span))
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

        public static IReadOnlyList<ExchangeTradeModel> ParseTrades(ReadOnlySpan<byte> json)
        {
            using var document = JsonDocument.Parse(json.ToArray());
            var root = document.RootElement;
            if (!root.TryGetProperty("topic", out var topicElement))
            {
                return [];
            }

            var topic = topicElement.GetString();
            if (string.IsNullOrWhiteSpace(topic) || !topic.StartsWith("publicTrade.", StringComparison.Ordinal))
            {
                return [];
            }

            if (!root.TryGetProperty("data", out var dataElement) || dataElement.ValueKind != JsonValueKind.Array)
            {
                return [];
            }

            var events = JsonSerializer.Deserialize<List<BybitTradeEvent>>(dataElement.GetRawText());
            if (events is null || events.Count == 0)
            {
                return [];
            }

            var result = new List<ExchangeTradeModel>(events.Count);
            foreach (var item in events)
            {
                result.Add(new ExchangeTradeModel
                {
                    Source = "bybit",
                    Symbol = item.Symbol,
                    TradeId = item.TradeId,
                    Price = decimal.Parse(item.Price, CultureInfo.InvariantCulture),
                    Volume = decimal.Parse(item.Quantity, CultureInfo.InvariantCulture),
                    Side = item.Side == "Sell" ? TradeSide.Sell : TradeSide.Buy,
                    TimestampUtc = DateTimeOffset
                        .FromUnixTimeMilliseconds(item.TradeTime)
                        .UtcDateTime,
                    ReceivedAtUtc = DateTime.UtcNow
                });
            }

            return result;
        }
    }
}
