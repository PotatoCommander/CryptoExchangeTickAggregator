using TickAggregator.Infrastructure.Model;
using TickAggregator.Model;

namespace TickAggregator.Service
{
    public class DeduplicationFilter
    {
        private readonly int _capacity;
        private readonly HashSet<string> _seen;
        private readonly Queue<string> _order;

        public DeduplicationFilter(int windowSize)
        {
            if (windowSize <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(windowSize), "Window size must be positive.");
            }

            _capacity = windowSize;
            _seen = new HashSet<string>(windowSize);
            _order = new Queue<string>(windowSize);
        }

        public long DuplicatesCount { get; private set; }

        public bool TryAccept(in ExchangeTradeModel ticker)
        {
            var key = string.Concat(ticker.Source, "|", ticker.Symbol, "|", ticker.TradeId);

            if (!_seen.Add(key))
            {
                DuplicatesCount++;
                return false;
            }

            _order.Enqueue(key);
            if (_order.Count > _capacity)
            {
                var evicted = _order.Dequeue();
                _seen.Remove(evicted);
            }

            return true;
        }
    }
}
