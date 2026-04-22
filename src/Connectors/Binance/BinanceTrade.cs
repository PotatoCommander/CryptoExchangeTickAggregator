using System.Text.Json.Serialization;

namespace TickAggregator.Infrastructure.Connectors.Binance
{
    public sealed class BinanceTrade
    {
        [JsonPropertyName("data")]
        public BinanceTradeEvent? Data { get; set; }
    }
}
