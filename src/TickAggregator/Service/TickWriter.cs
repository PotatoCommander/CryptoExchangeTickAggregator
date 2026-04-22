using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using TickAggregator.Infrastructure.Model;
using TickAggregator.Infrastructure.Service;
using TickAggregator.Model;

namespace TickAggregator.Service
{
    public class TickWriter
    {
        private readonly Channel<ExchangeTradeModel> _channel;
        private readonly Func<IReadOnlyList<ExchangeTradeModel>, CancellationToken, Task> _flush;
        private readonly TickWriterOptions _options;
        private readonly TickCounter _counter;
        private readonly ILogger<TickWriter> _logger;

        public TickWriter(
            DbTradeService sink,
            TickWriterOptions options,
            TickCounter counter,
            ILogger<TickWriter> logger)
            : this(sink.WriteAsync, options, counter, logger)
        {
        }

        public TickWriter(
            Func<IReadOnlyList<ExchangeTradeModel>, CancellationToken, Task> flush,
            TickWriterOptions options,
            TickCounter counter,
            ILogger<TickWriter> logger)
        {
            _flush = flush;
            _options = options;
            _counter = counter;
            _logger = logger;

            _channel = Channel.CreateUnbounded<ExchangeTradeModel>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = false
            });
        }

        public void OnTick(ExchangeTradeModel ticker)
        {
            _ = _channel.Writer.TryWrite(ticker);
        }

        public async Task RunAsync(CancellationToken cancellationToken)
        {
            var filter = new DeduplicationFilter(_options.DedupWindowSize);
            var batch = new List<ExchangeTradeModel>(_options.MaxBatchSize);
            var nextFlush = DateTime.UtcNow + _options.FlushInterval;

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var timeLeft = nextFlush - DateTime.UtcNow;
                    if (timeLeft <= TimeSpan.Zero)
                    {
                        await FlushAsync(batch, cancellationToken).ConfigureAwait(false);
                        nextFlush = DateTime.UtcNow + _options.FlushInterval;
                        continue;
                    }

                    using var waitCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    waitCts.CancelAfter(timeLeft);

                    bool ready;
                    try
                    {
                        ready = await _channel.Reader.WaitToReadAsync(waitCts.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                    {
                        await FlushAsync(batch, cancellationToken).ConfigureAwait(false);
                        nextFlush = DateTime.UtcNow + _options.FlushInterval;
                        continue;
                    }

                    if (!ready)
                    {
                        break;
                    }

                    while (_channel.Reader.TryRead(out var ticker))
                    {
                        _counter.IncrementReceived();

                        if (!filter.TryAccept(in ticker))
                        {
                            _counter.IncrementDuplicate();
                            continue;
                        }

                        batch.Add(ticker);

                        if (batch.Count >= _options.MaxBatchSize)
                        {
                            await FlushAsync(batch, cancellationToken).ConfigureAwait(false);
                            nextFlush = DateTime.UtcNow + _options.FlushInterval;
                        }
                    }
                }
            }
            finally
            {
                try { await FlushAsync(batch, CancellationToken.None).ConfigureAwait(false); }
                catch (Exception ex) { _logger.LogError(ex, "final flush failed"); }
            }
        }

        public void Complete()
        {
            _channel.Writer.TryComplete();
        }

        private async Task FlushAsync(List<ExchangeTradeModel> batch, CancellationToken ct)
        {
            if (batch.Count == 0)
            {
                return;
            }

            try
            {
                await _flush(batch, ct).ConfigureAwait(false);
                _counter.AddWritten(batch.Count);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "flush failed for batch of {Count}", batch.Count);
            }
            finally
            {
                batch.Clear();
            }
        }
    }
}
