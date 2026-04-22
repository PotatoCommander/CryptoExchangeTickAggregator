using TickAggregator.Infrastructure.Model;

namespace TickAggregator.Infrastructure.Service
{
    public interface ITradeSink
    {
        Task WriteAsync(IReadOnlyList<ExchangeTradeModel> batch, CancellationToken cancellationToken);
    }
}
