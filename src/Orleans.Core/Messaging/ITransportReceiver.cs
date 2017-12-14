using System;
using System.Collections.Generic;
using System.Text;

namespace Orleans.Messaging
{
    public interface ITransportReceiver
    {
        void OnReceive(ITransport transport, TransportError transportError, byte[] buffer, int bytesReceived);
    }
}
