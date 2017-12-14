using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Orleans.Messaging
{
    /// <summary>
    /// The Receiver class is used by the GatewayConnection to receive messages. It runs its own thread, but it performs all i/o operations synchronously.
    /// </summary>
    internal class GatewayClientReceiver : SingleTaskAsynchAgent
    {
        private readonly GatewayConnection gatewayConnection;
        private readonly IncomingMessageBuffer buffer;
        private ITransport transport;

        internal GatewayClientReceiver(GatewayConnection gateway, SerializationManager serializationManager, ExecutorService executorService, ILoggerFactory loggerFactory)
            : base(gateway.Address.ToString(), executorService, loggerFactory)
        {
            gatewayConnection = gateway;
            OnFault = FaultBehavior.RestartOnFault;
            buffer = new IncomingMessageBuffer(loggerFactory, serializationManager, true); 
        }

        protected override void Run()
        {
            try
            {
                while (!Cts.IsCancellationRequested)
                {
                    int bytesRead = FillBuffer(buffer.BuildReceiveBuffer());
                    if (bytesRead == 0)
                    {
                        continue;
                    }

                    buffer.UpdateReceivedData(bytesRead);

                    Message msg;
                    while (buffer.TryDecodeMessage(out msg))
                    {
                        gatewayConnection.MsgCenter.QueueIncomingMessage(msg);
                        if (Log.IsVerbose3) Log.Verbose3("Received a message from gateway {0}: {1}", gatewayConnection.Address, msg);
                    }
                }
            }
            catch (Exception ex)
            {
                buffer.Reset();
                Log.Warn(ErrorCode.ProxyClientUnhandledExceptionWhileReceiving, String.Format("Unexpected/unhandled exception while receiving: {0}. Restarting gateway receiver for {1}.",
                    ex, gatewayConnection.Address), ex);
                throw;
            }
        }

        private int FillBuffer(List<ArraySegment<byte>> bufferSegments)
        {
            try
            {
                if (gatewayConnection.Transport == null || !gatewayConnection.Transport.Connected)
                {
                    gatewayConnection.Connect();
                }
                if(!Equals(transport, gatewayConnection.Transport))
                {
                    buffer.Reset();
                    transport = gatewayConnection.Transport;
                }
                if (transport != null && transport.Connected)
                {
                    var bytesRead = transport.Receive(bufferSegments);
                    if (bytesRead == 0)
                    {
                        throw new EndOfStreamException("Socket closed");
                    }
                    return bytesRead;
                }
            }
            catch (Exception ex)
            {
                buffer.Reset();
                // Only try to reconnect if we're not shutting down
                if (Cts.IsCancellationRequested) return 0;

                Log.Warn(ErrorCode.Runtime_Error_100158, String.Format("Exception receiving from gateway {0}: {1}", gatewayConnection.Address, ex.Message));
                gatewayConnection.MarkAsDisconnected(transport);
                transport = null;
                return 0;
            }
            return 0;
        }
    }
}
