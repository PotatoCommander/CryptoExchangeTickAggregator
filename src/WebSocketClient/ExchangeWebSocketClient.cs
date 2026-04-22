using System.Net.WebSockets;
using Microsoft.Extensions.Logging;

namespace WebSocketClient
{
    public class ExchangeWebSocketClient
    {
        private readonly WebSocketOptions _options;
        private readonly ILogger _logger;

        public ExchangeWebSocketClient(WebSocketOptions options, ILogger logger)
        {
            _options = options;
            _logger = logger;
        }

        public async Task ConnectAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                using var socket = new ClientWebSocket();
                socket.Options.KeepAliveInterval = _options.KeepAlive;

                try
                {
                    _logger.LogInformation("[{Name}] connecting to {Uri}", _options.Name, _options.Uri);
                    await socket.ConnectAsync(_options.Uri, cancellationToken).ConfigureAwait(false);
                    _logger.LogInformation("[{Name}] connected", _options.Name);

                    if (_options.OnConnectedAsync != null)
                    {
                        await _options.OnConnectedAsync(socket, cancellationToken).ConfigureAwait(false);
                    }

                    using var sessionCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    var state = new SessionState();
                    var receiveTask = ReceiveLoopAsync(socket, state, sessionCts.Token);
                    var pingTask = HeartbeatLoopAsync(socket, sessionCts.Token);
                    var timeoutTask = TimeoutLoopAsync(state, sessionCts.Token);

                    //If anyone failed
                    var completedTask = await Task.WhenAny(receiveTask, pingTask, timeoutTask).ConfigureAwait(false);
                    await completedTask.ConfigureAwait(false);
                    await sessionCts.CancelAsync();

                    try
                    {
                        await Task.WhenAll(receiveTask, pingTask, timeoutTask).ConfigureAwait(false);
                    }
                    catch
                    {
                    }

                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "[{Name}] disconnected: {Message}", _options.Name, ex.Message);
                }

                if (cancellationToken.IsCancellationRequested)
                {
                    break;
                }

                _options.OnDisconnected?.Invoke();

                // TODO: Replace the fixed reconnect delay with exponential backoff 
                _logger.LogInformation("[{Name}] reconnecting in {Delay}s", _options.Name, _options.ReconnectDelay.TotalSeconds);
                try
                {
                    await Task.Delay(_options.ReconnectDelay, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) { break; }
            }

            _logger.LogInformation("[{Name}] stopped", _options.Name);
        }

        private async Task ReceiveLoopAsync(ClientWebSocket socket, SessionState state, CancellationToken ct)
        {
            var buffer = new byte[_options.ReceiveBufferBytes];
            var segment = new ArraySegment<byte>(buffer);

            while (!ct.IsCancellationRequested && socket.State == WebSocketState.Open)
            {
                MemoryStream? assembled = null;

                while (true)
                {
                    var result = await socket.ReceiveAsync(segment, ct).ConfigureAwait(false);

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        _logger.LogInformation("[{Name}] received Close frame (status={Status}, desc={Desc})",
                            _options.Name, result.CloseStatus, result.CloseStatusDescription);
                        return;
                    }

                    if (result.EndOfMessage && assembled is null)
                    {
                        _options.OnMessage(new ReadOnlyMemory<byte>(buffer, 0, result.Count));
                        state.MarkMessageReceived();
                        break;
                    }

                    assembled ??= new MemoryStream(_options.ReceiveBufferBytes);
                    assembled.Write(buffer, 0, result.Count);

                    if (result.EndOfMessage)
                    {
                        if (assembled.TryGetBuffer(out var full))
                        {
                            _options.OnMessage(new ReadOnlyMemory<byte>(full.Array!, full.Offset, (int)assembled.Length));
                        }
                        else
                        {
                            _options.OnMessage(assembled.ToArray());
                        }

                        state.MarkMessageReceived();
                        assembled.Dispose();
                        break;
                    }
                }
            }
        }

        private async Task HeartbeatLoopAsync(ClientWebSocket socket, CancellationToken ct)
        {
            if (_options.PingInterval is null || _options.PingPayloadFactory is null)
            {
                await Task.Delay(Timeout.Infinite, ct).ConfigureAwait(false);
                return;
            }

            using var timer = new PeriodicTimer(_options.PingInterval.Value);
            while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            {
                if (socket.State != WebSocketState.Open)
                {
                    return;
                }

                var payload = _options.PingPayloadFactory();
                await socket.SendAsync(payload, WebSocketMessageType.Text, true, ct).ConfigureAwait(false);
            }
        }

        private async Task TimeoutLoopAsync(SessionState state, CancellationToken ct)
        {
            if (_options.ConnectionTimeout is null)
            {
                await Task.Delay(Timeout.Infinite, ct).ConfigureAwait(false);
                return;
            }

            using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(Math.Max(100, _options.ConnectionTimeout.Value.TotalMilliseconds / 4)));
            while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            {
                if (DateTime.UtcNow - state.LastMessageAtUtc > _options.ConnectionTimeout.Value)
                {
                    throw new TimeoutException($"No data received for {_options.ConnectionTimeout.Value}.");
                }
            }
        }

        private class SessionState
        {
            private long _receivedAnyMessage;
            private long _lastMessageAtTicks = DateTime.UtcNow.Ticks;

            public bool ReceivedAnyMessage => Interlocked.Read(ref _receivedAnyMessage) == 1;
            public DateTime LastMessageAtUtc => new(Interlocked.Read(ref _lastMessageAtTicks), DateTimeKind.Utc);

            public void MarkMessageReceived()
            {
                Interlocked.Exchange(ref _receivedAnyMessage, 1);
                Interlocked.Exchange(ref _lastMessageAtTicks, DateTime.UtcNow.Ticks);
            }
        }
    }
}
