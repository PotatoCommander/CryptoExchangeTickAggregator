using System.Text.Json.Serialization;

namespace TickAggregator.Infrastructure.Connectors.Binance
{
    public sealed class BinanceTradeEvent
    {
        [JsonPropertyName("e")]
        public string EventType { get; set; } = "";

        [JsonPropertyName("s")]
        public string Symbol { get; set; } = "";

        [JsonPropertyName("t")]
        public long TradeId { get; set; }

        [JsonPropertyName("p")]
        public string Price { get; set; } = "";

        [JsonPropertyName("q")]
        public string Quantity { get; set; } = "";

        [JsonPropertyName("T")]
        public long TradeTime { get; set; }

        [JsonPropertyName("m")]
        public bool IsBuyerMarketMaker { get; set; }
    }
}
