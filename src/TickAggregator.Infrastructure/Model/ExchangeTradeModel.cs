namespace TickAggregator.Infrastructure.Model
{
    public class ExchangeTradeModel
    {
        public string Source { get; set; } = "";

        public string Symbol { get; set; } = "";

        public string TradeId { get; set; } = "";

        public decimal Price { get; set; }

        //TODO: Consider that in different exchanges trade volume can be represented in different ways (e.g. quote vs base currency).
        public decimal Volume { get; set; }

        public TradeSide Side { get; set; }

        public DateTime TimestampUtc { get; set; }

        public DateTime ReceivedAtUtc { get; set; }
    }
}
