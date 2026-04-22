using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using TickAggregator.Infrastructure.Model;
using TickAggregator.Infrastructure.Service;
using TickAggregator.Model;
using TickAggregator.Service;
using Xunit;

namespace TickAggregator.Tests
{
    public class TickWriterTests
    {
        private static ExchangeTradeModel T(string tradeId, int timestampId) =>
            new()
            {
                Source = "bybit",
                Symbol = "BTCUSDT",
                TradeId = tradeId,
                Price = 1,
                Volume = 1,
                Side = TradeSide.Buy,
                TimestampUtc = DateTime.UnixEpoch.AddMilliseconds(timestampId),
                ReceivedAtUtc = DateTime.UtcNow
            };

        private sealed class Recorder
            : ITradeSink
        {
            public List<ExchangeTradeModel> All { get; } = [];
            public int BatchCount { get; private set; }

            public Task WriteAsync(IReadOnlyList<ExchangeTradeModel> batch, CancellationToken _)
            {
                lock (All)
                {
                    All.AddRange(batch);
                    BatchCount++;
                }

                return Task.CompletedTask;
            }
        }

        [Fact]
        public async Task Drops_duplicates_and_forwards_unique_trades()
        {
            var rec = new Recorder();
            var counter = new TickCounter(NullLogger<TickCounter>.Instance);
            var writer = new TickWriter(
                sink: rec,
                options: new TickWriterOptions { MaxBatchSize = 100, FlushInterval = TimeSpan.FromMilliseconds(50), DedupWindowSize = 1000 },
                counter: counter,
                logger: NullLogger<TickWriter>.Instance);

            using var cts = new CancellationTokenSource();
            var runTask = writer.RunAsync(cts.Token);

            writer.OnTick(T("1", 1));
            writer.OnTick(T("2", 2));
            writer.OnTick(T("2", 3));
            writer.OnTick(T("3", 4));
            writer.OnTick(T("1", 5));
            writer.OnTick(T("4", 6));

            writer.Complete();
            cts.CancelAfter(TimeSpan.FromSeconds(2));

            try { await runTask; } catch (OperationCanceledException) { }

            rec.All.Select(t => t.TradeId).Should().Equal("1", "2", "3", "4");
            counter.Duplicates.Should().Be(2);
            counter.Written.Should().Be(4);
        }

        [Fact]
        public async Task Same_timestamp_with_different_trade_ids_is_not_dropped()
        {
            var rec = new Recorder();
            var counter = new TickCounter(NullLogger<TickCounter>.Instance);
            var writer = new TickWriter(
                sink: rec,
                options: new TickWriterOptions { MaxBatchSize = 100, FlushInterval = TimeSpan.FromMilliseconds(50), DedupWindowSize = 1000 },
                counter: counter,
                logger: NullLogger<TickWriter>.Instance);

            using var cts = new CancellationTokenSource();
            var runTask = writer.RunAsync(cts.Token);

            writer.OnTick(T("100", 42));
            writer.OnTick(T("101", 42));

            writer.Complete();
            cts.CancelAfter(TimeSpan.FromSeconds(2));

            try { await runTask; } catch (OperationCanceledException) { }

            rec.All.Select(t => t.TradeId).Should().Equal("100", "101");
            counter.Duplicates.Should().Be(0);
            counter.Written.Should().Be(2);
        }

        [Fact]
        public async Task Flushes_partial_batch_on_timeout()
        {
            var rec = new Recorder();
            var counter = new TickCounter(NullLogger<TickCounter>.Instance);
            var writer = new TickWriter(
                sink: rec,
                options: new TickWriterOptions { MaxBatchSize = 1000, FlushInterval = TimeSpan.FromMilliseconds(100), DedupWindowSize = 1000 },
                counter: counter,
                logger: NullLogger<TickWriter>.Instance);

            using var cts = new CancellationTokenSource();
            var runTask = writer.RunAsync(cts.Token);

            writer.OnTick(T("1", 1));
            writer.OnTick(T("2", 2));

            await Task.Delay(300);

            rec.All.Should().HaveCount(2);
            rec.BatchCount.Should().BeGreaterThanOrEqualTo(1);

            cts.Cancel();
            try { await runTask; } catch (OperationCanceledException) { }
        }

        [Fact]
        public async Task Flushes_when_max_batch_size_reached()
        {
            var rec = new Recorder();
            var counter = new TickCounter(NullLogger<TickCounter>.Instance);
            var writer = new TickWriter(
                sink: rec,
                options: new TickWriterOptions { MaxBatchSize = 10, FlushInterval = TimeSpan.FromSeconds(60), DedupWindowSize = 1000 },
                counter: counter,
                logger: NullLogger<TickWriter>.Instance);

            using var cts = new CancellationTokenSource();
            var runTask = writer.RunAsync(cts.Token);

            for (var i = 0; i < 25; i++)
            {
                writer.OnTick(T(i.ToString(), i));
            }

            var deadline = DateTime.UtcNow.AddSeconds(3);
            while (rec.All.Count < 20 && DateTime.UtcNow < deadline)
            {
                await Task.Delay(20);
            }

            rec.All.Count.Should().BeGreaterThanOrEqualTo(20);
            rec.BatchCount.Should().BeGreaterThanOrEqualTo(2);

            cts.Cancel();
            try { await runTask; } catch (OperationCanceledException) { }
        }
    }
}
