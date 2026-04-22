using System.Text;
using FluentAssertions;
using TickAggregator.Infrastructure.Connectors.Binance;
using TickAggregator.Infrastructure.Model;
using Xunit;

namespace TickAggregator.Tests
{
    public class BinanceTradesTests
    {
        [Fact]
        public void Parses_combined_stream_trade_event()
        {
            var json = """
                       {
                         "stream": "btcusdt@trade",
                         "data": {
                           "e": "trade",
                           "E": 1700000000000,
                           "s": "BTCUSDT",
                           "t": 12345,
                           "p": "50000.12345678",
                           "q": "0.125",
                           "T": 1700000000001,
                           "m": true
                         }
                       }
                       """;

            var trade = BinanceConnector.ParseTrade(Encoding.UTF8.GetBytes(json));

            trade.Should().NotBeNull();
            trade!.Source.Should().Be("binance");
            trade.Symbol.Should().Be("BTCUSDT");
            trade.TradeId.Should().Be("12345");
            trade.Price.Should().Be(50000.12345678m);
            trade.Volume.Should().Be(0.125m);
            trade.Side.Should().Be(TradeSide.Sell);
            trade.TimestampUtc.Should().Be(new DateTime(2023, 11, 14, 22, 13, 20, 1, DateTimeKind.Utc));
        }

        [Fact]
        public void Returns_null_for_non_trade_events()
        {
            var json = """{"stream":"btcusdt@kline_1m","data":{"e":"kline","E":1}}""";
            BinanceConnector.ParseTrade(Encoding.UTF8.GetBytes(json)).Should().BeNull();
        }

        [Fact]
        public void Returns_null_when_data_is_absent()
        {
            var json = """{"stream":"btcusdt@trade"}""";
            BinanceConnector.ParseTrade(Encoding.UTF8.GetBytes(json)).Should().BeNull();
        }
    }
}
