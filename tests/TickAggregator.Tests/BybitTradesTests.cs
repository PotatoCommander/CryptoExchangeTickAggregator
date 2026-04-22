using System.Text;
using FluentAssertions;
using TickAggregator.Infrastructure.Connectors.Bybit;
using TickAggregator.Infrastructure.Model;
using Xunit;

namespace TickAggregator.Tests
{
    public class BybitTradesTests
    {
        [Fact]
        public void Parses_trade_snapshot()
        {
            var json = """
                       {
                         "topic": "publicTrade.BTCUSDT",
                         "type": "snapshot",
                         "ts": 1700000000000,
                         "data": [
                           {
                             "T": 1700000000001,
                             "s": "BTCUSDT",
                             "S": "Buy",
                             "v": "0.001",
                             "p": "50000.5",
                             "i": "trade-1"
                           }
                         ]
                       }
                       """;

            var trades = BybitConnector.ParseTrades(Encoding.UTF8.GetBytes(json));

            trades.Should().HaveCount(1);
            trades[0].Source.Should().Be("bybit");
            trades[0].Symbol.Should().Be("BTCUSDT");
            trades[0].TradeId.Should().Be("trade-1");
            trades[0].Price.Should().Be(50000.5m);
            trades[0].Volume.Should().Be(0.001m);
            trades[0].Side.Should().Be(TradeSide.Buy);
            trades[0].TimestampUtc.Should().Be(new DateTime(2023, 11, 14, 22, 13, 20, 1, DateTimeKind.Utc));
        }

        [Fact]
        public void Parses_multiple_trades_from_single_message()
        {
            var json = """
                       {
                         "topic": "publicTrade.BTCUSDT",
                         "type": "snapshot",
                         "ts": 1700000000000,
                         "data": [
                           {
                             "T": 1700000000001,
                             "s": "BTCUSDT",
                             "S": "Buy",
                             "v": "0.001",
                             "p": "50000.5",
                             "i": "trade-1"
                           },
                           {
                             "T": 1700000000001,
                             "s": "BTCUSDT",
                             "S": "Sell",
                             "v": "0.002",
                             "p": "50000.4",
                             "i": "trade-2"
                           }
                         ]
                       }
                       """;

            var trades = BybitConnector.ParseTrades(Encoding.UTF8.GetBytes(json));

            trades.Should().HaveCount(2);
            trades.Select(x => x.TradeId).Should().Contain(["trade-1", "trade-2"]);
            trades.Select(x => x.Side).Should().Contain([TradeSide.Buy, TradeSide.Sell]);
        }

        [Fact]
        public void Returns_empty_for_non_trade_topic()
        {
            var json = """{"topic":"tickers.BTCUSDT","type":"snapshot","data":{}}""";
            BybitConnector.ParseTrades(Encoding.UTF8.GetBytes(json)).Should().BeEmpty();
        }

        [Fact]
        public void Returns_empty_for_subscribe_ack()
        {
            var json = """{"success":true,"op":"subscribe","conn_id":"abc"}""";
            BybitConnector.ParseTrades(Encoding.UTF8.GetBytes(json)).Should().BeEmpty();
        }
    }
}
