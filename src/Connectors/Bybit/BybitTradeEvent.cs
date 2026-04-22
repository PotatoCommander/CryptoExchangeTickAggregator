using System.Text.Json.Serialization;

namespace TickAggregator.Infrastructure.Connectors.Bybit
{
    public sealed class BybitTradeEvent
    {
        [JsonPropertyName("T")]
        public long TradeTime { get; set; }

        [JsonPropertyName("s")]
        public string Symbol { get; set; } = "";

        [JsonPropertyName("S")]
        public string Side { get; set; } = "";

        [JsonPropertyName("v")]
        public string Quantity { get; set; } = "";

        [JsonPropertyName("p")]
        public string Price { get; set; } = "";

        [JsonPropertyName("i")]
        public string TradeId { get; set; } = "";
    }
}
