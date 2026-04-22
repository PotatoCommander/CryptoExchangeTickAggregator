using System.Net;
using System.Net.WebSockets;
using System.Text;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using WebSocketClient;
using Xunit;

namespace TickAggregator.Tests
{
    public class ExchangeWebSocketClientTests
    {
        private static int FindFreePort()
        {
            var listener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return port;
        }

        private sealed class LocalWsServer : IAsyncDisposable
        {
            private readonly HttpListener _http = new();
            private readonly CancellationTokenSource _cts = new();
            private readonly Task _loop;

            public int Port { get; }
            public int ConnectionsAccepted;

            public Func<WebSocket, CancellationToken, Task>? OnConnected { get; set; }

            public LocalWsServer()
            {
                Port = FindFreePort();
                _http.Prefixes.Add($"http://localhost:{Port}/");
                _http.Start();
                _loop = Task.Run(AcceptLoop);
            }

            private async Task AcceptLoop()
            {
                while (!_cts.IsCancellationRequested)
                {
                    HttpListenerContext ctx;
                    try { ctx = await _http.GetContextAsync().WaitAsync(_cts.Token); }
                    catch { return; }

                    if (!ctx.Request.IsWebSocketRequest)
                    {
                        ctx.Response.StatusCode = 400;
                        ctx.Response.Close();
                        continue;
                    }

                    _ = Task.Run(async () =>
                    {
                        var wsCtx = await ctx.AcceptWebSocketAsync(null);
                        Interlocked.Increment(ref ConnectionsAccepted);
                        try
                        {
                            if (OnConnected != null)
                            {
                                await OnConnected(wsCtx.WebSocket, _cts.Token);
                            }
                        }
                        catch { }
                    });
                }
            }

            public async ValueTask DisposeAsync()
            {
                _cts.Cancel();
                try { _http.Stop(); } catch { }
                try { await _loop; } catch { }
            }
        }

        [Fact]
        public async Task Delivers_messages_from_server()
        {
            await using var server = new LocalWsServer();
            server.OnConnected = async (ws, ct) =>
            {
                var bytes = Encoding.UTF8.GetBytes("{\"hello\":\"world\"}");
                await ws.SendAsync(bytes, WebSocketMessageType.Text, true, ct);
                await Task.Delay(200, ct);
                await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None);
            };

            var received = new TaskCompletionSource<string>();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

            var options = new WebSocketOptions
            {
                Name = "test",
                Uri = new Uri($"ws://localhost:{server.Port}/"),
                OnMessage = mem =>
                {
                    received.TrySetResult(Encoding.UTF8.GetString(mem.Span));
                    cts.Cancel();
                }
            };

            var client = new ExchangeWebSocketClient(options, NullLogger.Instance);
            try { await client.ConnectAsync(cts.Token); } catch (OperationCanceledException) { }

            (await received.Task).Should().Be("{\"hello\":\"world\"}");
        }

        [Fact]
        public async Task Reconnects_after_server_drops_connection()
        {
            await using var server = new LocalWsServer();
            var connectionSeen = 0;
            var subscriptions = 0;

            server.OnConnected = async (ws, ct) =>
            {
                var n = Interlocked.Increment(ref connectionSeen);
                var receiveBuffer = new byte[128];
                var receive = await ws.ReceiveAsync(receiveBuffer, ct);
                if (receive.MessageType == WebSocketMessageType.Text)
                {
                    var payload = Encoding.UTF8.GetString(receiveBuffer, 0, receive.Count);
                    if (payload == "subscribe")
                    {
                        Interlocked.Increment(ref subscriptions);
                    }
                }

                if (n == 1)
                {
                    await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "drop", CancellationToken.None);
                    return;
                }

                var bytes = Encoding.UTF8.GetBytes("{\"n\":2}");
                await ws.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
                await Task.Delay(500, ct);
            };

            var secondReceived = new TaskCompletionSource<string>();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

            var options = new WebSocketOptions
            {
                Name = "test",
                Uri = new Uri($"ws://localhost:{server.Port}/"),
                ReconnectDelay = TimeSpan.FromMilliseconds(50),
                OnConnectedAsync = (ws, ct) => ws.SendAsync(Encoding.UTF8.GetBytes("subscribe"), WebSocketMessageType.Text, true, ct),
                OnMessage = mem =>
                {
                    secondReceived.TrySetResult(Encoding.UTF8.GetString(mem.Span));
                    cts.Cancel();
                }
            };

            var client = new ExchangeWebSocketClient(options, NullLogger.Instance);
            try { await client.ConnectAsync(cts.Token); } catch (OperationCanceledException) { }

            var payload = await secondReceived.Task;
            payload.Should().Contain("\"n\":2");
            server.ConnectionsAccepted.Should().BeGreaterThanOrEqualTo(2);
            subscriptions.Should().BeGreaterThanOrEqualTo(2);
        }

        [Fact]
        public async Task Reconnects_after_no_data_timeout()
        {
            await using var server = new LocalWsServer();
            var connectionSeen = 0;

            server.OnConnected = async (ws, ct) =>
            {
                var n = Interlocked.Increment(ref connectionSeen);
                if (n == 1)
                {
                    await Task.Delay(TimeSpan.FromSeconds(5), ct);
                    return;
                }

                var bytes = Encoding.UTF8.GetBytes("{\"n\":2}");
                await ws.SendAsync(bytes, WebSocketMessageType.Text, true, ct);
                await Task.Delay(200, ct);
            };

            var received = new TaskCompletionSource<string>();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

            var options = new WebSocketOptions
            {
                Name = "test",
                Uri = new Uri($"ws://localhost:{server.Port}/"),
                ReconnectDelay = TimeSpan.FromMilliseconds(50),
                ConnectionTimeout = TimeSpan.FromMilliseconds(200),
                OnMessage = mem =>
                {
                    received.TrySetResult(Encoding.UTF8.GetString(mem.Span));
                    cts.Cancel();
                }
            };

            var client = new ExchangeWebSocketClient(options, NullLogger.Instance);
            try { await client.ConnectAsync(cts.Token); } catch (OperationCanceledException) { }

            var payload = await received.Task;
            payload.Should().Contain("\"n\":2");
            server.ConnectionsAccepted.Should().BeGreaterThanOrEqualTo(2);
        }
    }
}
