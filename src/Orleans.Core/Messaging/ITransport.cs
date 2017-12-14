using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Messaging
{
    public interface ITransport
    {
        bool Connected { get; }
        bool IsListening { get; }
        object UserToken { get; set; }
        EndPoint LocalEndPoint { get; }
        EndPoint RemoteEndPoint { get; }

        void Connect(EndPoint endpoint, TimeSpan connectionTimeout);
        void ListenAsync(ITransportListener listener);
        void RestartListenAsync(ITransportListener listener);
        void Close();
        void ReceiveAsync(ITransportReceiver receiver);
        int Receive(byte[] buffer, int offset, int length);
        int Receive(IList<ArraySegment<byte>> buffer);
        int Send(List<ArraySegment<byte>> buffer);
        int Send(byte[] buffer);
    }
}
