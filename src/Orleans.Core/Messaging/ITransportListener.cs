using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Messaging
{
    public interface ITransportListener
    {
        bool OnAccept(ITransport transport, TransportError transportError);
        bool OnOpen(ITransport transport, TransportError transportError);
        void OnError(Exception exception);
        void OnDataReceive(ITransport transport, TransportError transportError, byte[] buffer, int bytesTransferred);
        void OnClose(ITransport transport, TransportError transportError);
    }
}
