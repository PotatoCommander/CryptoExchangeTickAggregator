using TickAggregator.Infrastructure.Model;

namespace TickAggregator.Infrastructure.Connectors
{
    public interface IExchangeConnector
    {
        public string Name { get; }

        public Task SubscribeToTradesAsync(IReadOnlyCollection<string> symbols, Action<ExchangeTradeModel> onMessage, CancellationToken cancellationToken);
    }
}
