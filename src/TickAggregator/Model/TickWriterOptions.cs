namespace TickAggregator.Model
{
    public class TickWriterOptions
    {
        public int MaxBatchSize { get; set; } = 500;

        public TimeSpan FlushInterval { get; set; } = TimeSpan.FromMilliseconds(200);

        public int DedupWindowSize { get; set; } = 50_000;
    }
}