using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Messaging;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Orleans.Runtime.Messaging
{
    internal class SocketTransportFactory : ITransportFactory
    {
        private readonly NetworkingOptions networkingOptions;
        private readonly ExecutorService executorService;
        private readonly ILoggerFactory loggerFactory;

        public SocketTransportFactory(IOptions<NetworkingOptions> networkingOptions, ExecutorService executorService, ILoggerFactory loggerFactory)
        {
            this.networkingOptions = networkingOptions;
            this.executorService = executorService;
            this.loggerFactory = loggerFactory;
        }

        public ITransport CreateReceivingTransport(EndPoint endpoint)
        {
            var socket = SocketManager.GetAcceptingSocketForEndpoint((IPEndPoint) endpoint, networkingOptions);
            return new SocketTransport(socket, null, socketManager, executorService, loggerFactory);
        }

        public ITransport CreateSendingTransport()
        {
            var socket = socketManager.SendingTransportCreator();
            return new SocketTransport(socket, null, socketManager, executorService, loggerFactory);
        }
    }
}
