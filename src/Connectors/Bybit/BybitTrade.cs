using System.Text.Json.Serialization;

namespace TickAggregator.Infrastructure.Connectors.Bybit
{
    public sealed class BybitTrade
    {
        [JsonPropertyName("topic")]
        public string Topic { get; set; } = "";

        [JsonPropertyName("type")]
        public string Type { get; set; } = "";

        [JsonPropertyName("data")]
        public List<BybitTradeEvent>? Data { get; set; }
    }
}
