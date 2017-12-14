using System;
using System.Collections.Generic;
using System.Net;
using System.Text;

namespace Orleans.Messaging
{
    public interface ITransportFactory
    {
        ITransport CreateReceivingTransport(EndPoint endpoint);
        ITransport CreateSendingTransport();
    }
}
