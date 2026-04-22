using Microsoft.Extensions.Logging;

namespace TickAggregator.Service
{
    public class TickCounter
    {
        private long _received;
        private long _written;
        private long _duplicates;
        private long _lastLoggedReceived;
        private DateTime _lastLoggedAt = DateTime.UtcNow;

        private readonly ILogger<TickCounter> _logger;

        public TickCounter(ILogger<TickCounter> logger)
        {
            _logger = logger;
        }

        public long Received => Interlocked.Read(ref _received);

        public long Written => Interlocked.Read(ref _written);

        public long Duplicates => Interlocked.Read(ref _duplicates);

        public void IncrementReceived() => Interlocked.Increment(ref _received);

        public void IncrementDuplicate() => Interlocked.Increment(ref _duplicates);

        public void AddWritten(int n) => Interlocked.Add(ref _written, n);

        public async Task RunRateReporterAsync(TimeSpan interval, CancellationToken ct)
        {
            using var timer = new PeriodicTimer(interval);
            try
            {
                while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
                {
                    var now = DateTime.UtcNow;
                    var received = Received;
                    var deltaTicks = received - _lastLoggedReceived;
                    var deltaSec = (now - _lastLoggedAt).TotalSeconds;
                    var rate = deltaSec > 0 ? deltaTicks / deltaSec : 0;

                    _logger.LogInformation(
                        "rate={Rate:F1} tick/s, received={Received}, written={Written}, dupes={Dupes}",
                        rate, received, Written, Duplicates);

                    _lastLoggedReceived = received;
                    _lastLoggedAt = now;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                _logger.LogInformation("Rate reporter stopped.");
            }
        }
    }
}
