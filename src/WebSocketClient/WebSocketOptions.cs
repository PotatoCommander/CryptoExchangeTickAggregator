using System.Net.WebSockets;

namespace WebSocketClient
{
    public class WebSocketOptions
    {
        public required Uri Uri { get; set; }

        public Func<ClientWebSocket, CancellationToken, Task>? OnConnectedAsync { get; set; }

        public required Action<ReadOnlyMemory<byte>> OnMessage { get; set; }

        public Action? OnDisconnected { get; set; }

        public TimeSpan? PingInterval { get; set; }

        public Func<byte[]>? PingPayloadFactory { get; set; }

        public TimeSpan KeepAlive { get; set; } = TimeSpan.FromSeconds(10);

        public TimeSpan ReconnectDelay { get; set; } = TimeSpan.FromSeconds(3);

        public TimeSpan? ConnectionTimeout { get; set; }

        public int ReceiveBufferBytes { get; set; } = 64 * 1024;

        public string Name { get; set; } = "ws";
    }
}
