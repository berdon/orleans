using System;
using System.Collections.Generic;
using System.Net;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Messaging;
using Orleans.Hosting;


namespace Orleans.Runtime
{
    internal class TransportManager : ITransportReceiver
    {
        private readonly LRU<IPEndPoint, ITransport> cache;
        private readonly TimeSpan connectionTimeout;
        private readonly ITransportFactory transportFactory;
        private readonly ILogger logger;
        private const int MAX_TRANSPORTS = 200;

        internal TransportManager(IOptions<NetworkingOptions> options, ITransportFactory transportFactory, ILoggerFactory loggerFactory)
        {
            var networkingOptions = options.Value;
            connectionTimeout = networkingOptions.OpenConnectionTimeout;
            cache = new LRU<IPEndPoint, ITransport>(MAX_TRANSPORTS, networkingOptions.MaxSocketAge, SendingTransportCreator);
            this.transportFactory = transportFactory;
            this.logger = loggerFactory.CreateLogger<TransportManager>();
            cache.RaiseFlushEvent += FlushHandler;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
        internal bool CheckSendingSocket(IPEndPoint target)
        {
            return cache.ContainsKey(target);
        }

        internal ITransport GetSendingTransport(IPEndPoint target)
        {
            return cache.Get(target);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private ITransport SendingTransportCreator(IPEndPoint target)
        {
            ITransport transport = null;
            try
            {
                transport = transportFactory.CreateSendingTransport();
                transport.UserToken = Tuple.Create(transport, target, this);
                transport.Connect(target, connectionTimeout);
                WriteConnectionPreamble(transport, Constants.SiloDirectConnectionId); // Identifies this client as a direct silo-to-silo socket
                transport.ReceiveAsync(this);
            }
            catch (Exception)
            {
                try
                {
                    transport?.Close();
                }
                catch (Exception)
                {
                    // ignore
                }
                throw;
            }
            return transport;
        }

        internal static void WriteConnectionPreamble(ITransport transport, GrainId grainId)
        {
            int size = 0;
            byte[] grainIdByteArray = null;
            if (grainId != null)
            {
                grainIdByteArray = grainId.ToByteArray();
                size += grainIdByteArray.Length;
            }
            ByteArrayBuilder sizeArray = new ByteArrayBuilder();
            sizeArray.Append(size);
            transport.Send(sizeArray.ToBytes());       // The size of the data that is coming next.
            if (grainId != null)
            {
                // No need to send in a loop.
                // From MSDN: If you are using a connection-oriented protocol, Send will block until all of the bytes in the buffer are sent, 
                // unless a time-out was set by using Socket.SendTimeout. 
                // If the time-out value was exceeded, the Send call will throw a SocketException. 
                transport.Send(grainIdByteArray);     // The grainId of the client
            }
        }


        // We start an asynch receive, with this callback, off of every send socket.
        // Since we should never see data coming in on these sockets, having the receive complete means that
        // the socket is in an unknown state and we should close it and try again.
        public void OnReceive(ITransport inbound, TransportError transportError, byte[] buffer, int bytesReceived)
        {
            (var transport, var target, var transportManager) = inbound.UserToken as Tuple<ITransport, IPEndPoint, TransportManager>;

            try
            {
                transportManager?.InvalidateEntry(target);
            }
            catch (Exception ex)
            {
                transportManager?.logger.Error(ErrorCode.Messaging_Socket_ReceiveError, $"ReceiveCallback: {target}", ex);
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", MessageId = "s")]
        internal void ReturnSendingTransport(ITransport transport)
        {
            // Do nothing -- the socket will get cleaned up when it gets flushed from the cache
        }

        private static void FlushHandler(Object sender, LRU<IPEndPoint, ITransport>.FlushEventArgs args)
        {
            if (args.Value == null) return;

            args.Value.Close();
            NetworkingStatisticsGroup.OnClosedSendingSocket();
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        internal void InvalidateEntry(IPEndPoint target)
        {
            ITransport transport;
            if (!cache.RemoveKey(target, out transport)) return;

            transport.Close();
            NetworkingStatisticsGroup.OnClosedSendingSocket();
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        // Note that this method assumes that there are no other threads accessing this object while this method runs.
        // Since this is true for the MessageCenter's use of this object, we don't lock around all calls to avoid the overhead.
        internal void Stop()
        {
            // Clear() on an LRU<> calls the flush handler on every item, so no need to manually close the sockets.
            cache.Clear();
        }
    }
}
