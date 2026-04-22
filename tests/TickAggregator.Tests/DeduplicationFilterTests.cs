using FluentAssertions;
using TickAggregator.Infrastructure.Model;
using TickAggregator.Model;
using TickAggregator.Service;
using Xunit;

namespace TickAggregator.Tests
{
    public class DeduplicationFilterTests
    {
        private static ExchangeTradeModel Trade(string tradeId, long tsOffsetMs, string src = "bybit", string sym = "BTCUSDT") =>
            new()
            {
                Source = src,
                Symbol = sym,
                TradeId = tradeId,
                Price = 1,
                Volume = 1,
                Side = TradeSide.Buy,
                TimestampUtc = DateTime.UnixEpoch.AddMilliseconds(tsOffsetMs),
                ReceivedAtUtc = DateTime.UtcNow
            };

        [Fact]
        public void Unique_trades_pass_through()
        {
            var filter = new DeduplicationFilter(100);

            filter.TryAccept(Trade("1", 1)).Should().BeTrue();
            filter.TryAccept(Trade("2", 2)).Should().BeTrue();
            filter.TryAccept(Trade("3", 3)).Should().BeTrue();
            filter.DuplicatesCount.Should().Be(0);
        }

        [Fact]
        public void Duplicate_trade_is_rejected()
        {
            var filter = new DeduplicationFilter(100);

            filter.TryAccept(Trade("42", 1)).Should().BeTrue();
            filter.TryAccept(Trade("42", 2)).Should().BeFalse();
            filter.TryAccept(Trade("42", 3)).Should().BeFalse();

            filter.DuplicatesCount.Should().Be(2);
        }

        [Fact]
        public void Same_trade_id_different_source_is_not_duplicate()
        {
            var filter = new DeduplicationFilter(100);

            filter.TryAccept(Trade("42", 1, src: "bybit")).Should().BeTrue();
            filter.TryAccept(Trade("42", 1, src: "binance")).Should().BeTrue();
        }

        [Fact]
        public void Same_trade_id_different_symbol_is_not_duplicate()
        {
            var filter = new DeduplicationFilter(100);

            filter.TryAccept(Trade("42", 1, sym: "BTCUSDT")).Should().BeTrue();
            filter.TryAccept(Trade("42", 1, sym: "ETHUSDT")).Should().BeTrue();
        }

        [Fact]
        public void Same_timestamp_different_trade_ids_are_not_duplicates()
        {
            var filter = new DeduplicationFilter(100);

            filter.TryAccept(Trade("100", 42)).Should().BeTrue();
            filter.TryAccept(Trade("101", 42)).Should().BeTrue();
        }

        [Fact]
        public void Sliding_window_evicts_oldest_trade_id_so_re_entry_is_allowed()
        {
            var filter = new DeduplicationFilter(3);

            filter.TryAccept(Trade("1", 1)).Should().BeTrue();
            filter.TryAccept(Trade("2", 2)).Should().BeTrue();
            filter.TryAccept(Trade("3", 3)).Should().BeTrue();

            filter.TryAccept(Trade("1", 4)).Should().BeFalse();
            filter.TryAccept(Trade("3", 5)).Should().BeFalse();
            filter.TryAccept(Trade("4", 6)).Should().BeTrue();
            filter.TryAccept(Trade("1", 7)).Should().BeTrue();
            filter.TryAccept(Trade("3", 8)).Should().BeFalse();
        }
    }
}
