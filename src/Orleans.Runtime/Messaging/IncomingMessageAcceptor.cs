using System;
using System.Collections.Generic;
using System.Net;
using Microsoft.Extensions.Logging;
using Orleans.Messaging;
using Orleans.Serialization;

namespace Orleans.Runtime.Messaging
{
    internal class IncomingMessageAcceptor : IMessageHandler, ITransportListener
    {
        public ITransport AcceptingTransport;
        private readonly IPEndPoint listenAddress;
        private Action<Message> sniffIncomingMessageHandler;
        protected Logger Log;
        private object Lockable = new object();
        protected MessageCenter MessageCenter;
        protected HashSet<ITransport> OpenReceiveTransports;
        protected readonly MessageFactory MessageFactory;

        private readonly SerializationManager serializationManager;
        private readonly ILoggerFactory loggerFactory;
        public Action<Message> SniffIncomingMessage
        {
            set
            {
                if (sniffIncomingMessageHandler != null)
                    throw new InvalidOperationException("IncomingMessageAcceptor SniffIncomingMessage already set");

                sniffIncomingMessageHandler = value;
            }
        }

        private const int LISTEN_BACKLOG_SIZE = 1024;

        // Used for holding enough info to handle receive completion
        internal IncomingMessageAcceptor(MessageCenter msgCtr, IPEndPoint here, MessageFactory messageFactory, SerializationManager serializationManager,
            ExecutorService executorService, ILoggerFactory loggerFactory, ITransportFactory transportFactory)
        {
            this.loggerFactory = loggerFactory;
            Log = new LoggerWrapper<IncomingMessageAcceptor>(loggerFactory);
            MessageCenter = msgCtr;
            listenAddress = here;
            this.MessageFactory = messageFactory;
            this.serializationManager = serializationManager;
            if (here == null)
                listenAddress = MessageCenter.MyAddress.Endpoint;

            AcceptingTransport = transportFactory.CreateReceivingTransport(listenAddress);
            Log.Info(ErrorCode.Messaging_IMA_OpenedListeningSocket, "Opened a listening socket at address " + AcceptingTransport.LocalEndPoint);
            OpenReceiveTransports = new HashSet<ITransport>();
        }

        public void Start()
        {
            try
            {
                AcceptingTransport.ListenAsync(this);
            }
            catch (Exception ex)
            {
                Log.Error(ErrorCode.MessagingAcceptAsyncSocketException, "Exception beginning accept on listening socket", ex);
                throw;
            }
            if (Log.IsVerbose3) Log.Verbose3("Started accepting connections.");
        }

        public void Stop()
        {
            if (Log.IsVerbose) Log.Verbose("Disconnecting the listening socket");
            AcceptingTransport.Close();
        }

        protected virtual bool RecordOpenedTransport(ITransport transport)
        {
            GrainId client;
            if (!ReceiveTransportPreample(transport, false, out client)) return false;

            NetworkingStatisticsGroup.OnOpenedReceiveSocket();
            return true;
        }

        protected bool ReceiveTransportPreample(ITransport transport, bool expectProxiedConnection, out GrainId client)
        {
            client = null;

            if (!ReadConnectionPreamble(transport, out client))
            {
                return false;
            }

            if (Log.IsVerbose2) Log.Verbose2(ErrorCode.MessageAcceptor_Connection, "Received connection from {0} at source address {1}", client, transport.RemoteEndPoint.ToString());

            if (expectProxiedConnection)
            {
                // Proxied Gateway Connection - must have sender id
                if (client.Equals(Constants.SiloDirectConnectionId))
                {
                    Log.Error(ErrorCode.MessageAcceptor_NotAProxiedConnection, $"Gateway received unexpected non-proxied connection from {client} at source address {transport.RemoteEndPoint}");
                    return false;
                }
            }
            else
            {
                // Direct connection - should not have sender id
                if (!client.Equals(Constants.SiloDirectConnectionId))
                {
                    Log.Error(ErrorCode.MessageAcceptor_UnexpectedProxiedConnection, $"Silo received unexpected proxied connection from {client} at source address {transport.RemoteEndPoint}");
                    return false;
                }
            }

            lock (Lockable)
            {
                OpenReceiveTransports.Add(transport);
            }

            return true;
        }

        private bool ReadConnectionPreamble(ITransport transport, out GrainId grainId)
        {
            grainId = null;
            byte[] buffer = null;
            try
            {
                buffer = ReadFromTransport(transport, sizeof(int)); // Read the size 
                if (buffer == null) return false;
                Int32 size = BitConverter.ToInt32(buffer, 0);

                if (size > 0)
                {
                    buffer = ReadFromTransport(transport, size); // Receive the client ID
                    if (buffer == null) return false;
                    grainId = GrainIdExtensions.FromByteArray(buffer);
                }
                return true;
            }
            catch (Exception exc)
            {
                Log.Error(ErrorCode.GatewayFailedToParse,
                    $"Failed to convert the data that read from the socket. buffer = {Utils.EnumerableToString(buffer)}, from endpoint {transport.RemoteEndPoint}.", exc);
                return false;
            }
        }

        private byte[] ReadFromTransport(ITransport transport, int expected)
        {
            var buffer = new byte[expected];
            int offset = 0;
            while (offset < buffer.Length)
            {
                try
                {
                    int bytesRead = transport.Receive(buffer, offset, buffer.Length - offset);
                    if (bytesRead == 0)
                    {
                        Log.Warn(ErrorCode.GatewayAcceptor_SocketClosed,
                            "Remote socket closed while receiving connection preamble data from endpoint {0}.", transport.RemoteEndPoint);
                        return null;
                    }
                    offset += bytesRead;
                }
                catch (Exception ex)
                {
                    Log.Warn(ErrorCode.GatewayAcceptor_ExceptionReceiving,
                        "Exception receiving connection preamble data from endpoint " + transport.RemoteEndPoint, ex);
                    return null;
                }
            }
            return buffer;
        }

        protected virtual void RecordClosedTransport(ITransport transport)
        {
            if (TryRemoveClosedTransport(transport))
                NetworkingStatisticsGroup.OnClosedReceivingSocket();
        }

        protected bool TryRemoveClosedTransport(ITransport transport)
        {
            lock (Lockable)
            {
                return OpenReceiveTransports.Remove(transport);
            }
        }

        protected virtual void ClearTransports()
        {
            lock (Lockable)
            {
                OpenReceiveTransports.Clear();
            }
        }

        public virtual void HandleMessage(Message msg)
        {
            // See it's a Ping message, and if so, short-circuit it
            object pingObj;
            var requestContext = msg.RequestContextData;
            if (requestContext != null &&
                requestContext.TryGetValue(RequestContext.PING_APPLICATION_HEADER, out pingObj) &&
                pingObj is bool &&
                (bool)pingObj)
            {
                MessagingStatisticsGroup.OnPingReceive(msg.SendingSilo);

                if (Log.IsVerbose2) Log.Verbose2("Responding to Ping from {0}", msg.SendingSilo);

                if (!msg.TargetSilo.Equals(MessageCenter.MyAddress)) // got ping that is not destined to me. For example, got a ping to my older incarnation.
                {
                    MessagingStatisticsGroup.OnRejectedMessage(msg);
                    Message rejection = this.MessageFactory.CreateRejectionResponse(msg, Message.RejectionTypes.Unrecoverable,
                        $"The target silo is no longer active: target was {msg.TargetSilo.ToLongString()}, but this silo is {MessageCenter.MyAddress.ToLongString()}. " +
                        $"The rejected ping message is {msg}.");
                    MessageCenter.OutboundQueue.SendMessage(rejection);
                }
                else
                {
                    var response = this.MessageFactory.CreateResponseMessage(msg);
                    response.BodyObject = Response.Done;
                    MessageCenter.SendMessage(response);
                }
                return;
            }

            // sniff message headers for directory cache management
            sniffIncomingMessageHandler?.Invoke(msg);

            // Don't process messages that have already timed out
            if (msg.IsExpired)
            {
                msg.DropExpiredMessage(MessagingStatisticsGroup.Phase.Receive);
                return;
            }

            // If we've stopped application message processing, then filter those out now
            // Note that if we identify or add other grains that are required for proper stopping, we will need to treat them as we do the membership table grain here.
            if (MessageCenter.IsBlockingApplicationMessages && (msg.Category == Message.Categories.Application) && !Constants.SystemMembershipTableId.Equals(msg.SendingGrain))
            {
                // We reject new requests, and drop all other messages
                if (msg.Direction != Message.Directions.Request) return;

                MessagingStatisticsGroup.OnRejectedMessage(msg);
                var reject = this.MessageFactory.CreateRejectionResponse(msg, Message.RejectionTypes.Unrecoverable, "Silo stopping");
                MessageCenter.SendMessage(reject);
                return;
            }

            // Make sure the message is for us. Note that some control messages may have no target
            // information, so a null target silo is OK.
            if ((msg.TargetSilo == null) || msg.TargetSilo.Matches(MessageCenter.MyAddress))
            {
                // See if it's a message for a client we're proxying.
                if (MessageCenter.IsProxying && MessageCenter.TryDeliverToProxy(msg)) return;

                // Nope, it's for us
                MessageCenter.InboundQueue.PostMessage(msg);
                return;
            }

            if (!msg.TargetSilo.Endpoint.Equals(MessageCenter.MyAddress.Endpoint))
            {
                // If the message is for some other silo altogether, then we need to forward it.
                if (Log.IsVerbose2) Log.Verbose2("Forwarding message {0} from {1} to silo {2}", msg.Id, msg.SendingSilo, msg.TargetSilo);
                MessageCenter.OutboundQueue.SendMessage(msg);
                return;
            }

            // If the message was for this endpoint but an older epoch, then reject the message
            // (if it was a request), or drop it on the floor if it was a response or one-way.
            if (msg.Direction == Message.Directions.Request)
            {
                MessagingStatisticsGroup.OnRejectedMessage(msg);
                Message rejection = this.MessageFactory.CreateRejectionResponse(msg, Message.RejectionTypes.Transient,
                    string.Format("The target silo is no longer active: target was {0}, but this silo is {1}. The rejected message is {2}.",
                        msg.TargetSilo.ToLongString(), MessageCenter.MyAddress.ToLongString(), msg));
                MessageCenter.OutboundQueue.SendMessage(rejection);
                if (Log.IsVerbose) Log.Verbose("Rejecting an obsolete request; target was {0}, but this silo is {1}. The rejected message is {2}.",
                    msg.TargetSilo.ToLongString(), MessageCenter.MyAddress.ToLongString(), msg);
            }
        }

        private void RestartAcceptingListener()
        {
            try
            {
                if (Log.IsVerbose) Log.Verbose("Restarting of the accepting socket");
                AcceptingTransport.RestartListenAsync(this);
            }
            catch (Exception ex)
            {
                Log.Error(ErrorCode.Runtime_Error_100016, "Unable to create a new accepting socket", ex);
                throw;
            }
        }

        private void SafeCloseTransport(ITransport transport)
        {
            RecordClosedTransport(transport);
            transport.Close();
        }

        public bool OnAccept(ITransport transport, TransportError transportError)
        {
            transport.UserToken = this;

            if (transportError != TransportError.Success)
            {
                RestartAcceptingListener();
                return false;
            }

            return true;
        }

        public bool OnOpen(ITransport transport, TransportError transportError)
        {
            var ima = transport.UserToken as IncomingMessageAcceptor;
            if (ima == null)
            {
                Log.Warn(ErrorCode.Messaging_IMA_AcceptCallbackUnexpectedState,
                    "AcceptCallback invoked with an unexpected async state of type {0}",
                    transport.UserToken?.GetType().ToString() ?? "null");
                transport.Close();
                return false;
            }

            if (ima != this)
            {
                ima.OnOpen(transport, transportError);
                return false;
            }

            if (transportError != TransportError.Success)
            {
                transport.Close();
                RestartAcceptingListener();
                return false;
            }

            if (!RecordOpenedTransport(transport))
            {
                SafeCloseTransport(transport);
            }

            var token = new TransportToken(transport, this.MessageFactory, this.serializationManager, this.loggerFactory);
            token.IMA = ima;
            transport.UserToken = token;

            return true;
        }

        public void OnError(Exception exception)
        {
            RestartAcceptingListener();
        }

        public void OnDataReceive(ITransport transport, TransportError transportError, byte[] buffer, int bytesTransferred)
        {
            var token = transport.UserToken as TransportToken;
            if (token.IMA != this)
            {
                token.IMA.OnDataReceive(transport, transportError, buffer, bytesTransferred);
                return;
            }

            // If no data was received, close the connection. This is a normal
            // situation that shows when the remote host has finished sending data.
            if (bytesTransferred <= 0)
            {
                if (Log.IsVerbose) Log.Verbose("Closing recieving socket: " + transport.RemoteEndPoint);
                token.IMA.SafeCloseTransport(transport);
                return;
            }

            if (transportError != TransportError.Success)
            {
                Log.Warn(ErrorCode.Messaging_IMA_NewBeginReceiveException,
                   $"Socket error on new socket during ReceiveAsync with RemoteEndPoint: {transportError}");
                token.IMA.SafeCloseTransport(transport);
                return;
            }

            try
            {
                token.ProcessReceived(buffer, bytesTransferred);
            }
            catch (Exception ex)
            {
                token.IMA.Log.Error(ErrorCode.Messaging_IMA_BadBufferReceived,
                    $"ProcessReceivedBuffer exception with RemoteEndPoint {token.RemoteEndPoint}: ", ex);

                // There was a problem with the buffer, presumably data corruption, so give up
                token.IMA.SafeCloseTransport(transport);
                // And we're done
                return;
            }
        }

        public void OnClose(ITransport transport, TransportError transportError)
        {
            var token = transport.UserToken as TransportToken;
            if (token.IMA != this)
            {
                token.IMA.OnClose(transport, transportError);
            }

            Log.Error(-1, "Error occured during socket closing");
        }

        private class TransportToken
        {
            private readonly MessageFactory messageFactory;
            private readonly IncomingMessageBuffer _buffer;
            private ITransport transport;

            public ITransport Transport
            {
                get { return transport; }
                internal set
                {
                    transport = value;
                    RemoteEndPoint = transport.RemoteEndPoint;
                }
            }
            public EndPoint RemoteEndPoint { get; private set; }
            public IncomingMessageAcceptor IMA { get; internal set; }

            public TransportToken(ITransport transport, MessageFactory messageFactory, SerializationManager serializationManager, ILoggerFactory loggerFactory)
            {
                Transport = transport;
                this.messageFactory = messageFactory;
                _buffer = new IncomingMessageBuffer(loggerFactory, serializationManager);
            }

            public void ProcessReceived(byte[] transportBuffer, int bytesReceived)
            {
#if TRACK_DETAILED_STATS
                ThreadTrackingStatistic tracker = null;
                if (StatisticsCollector.CollectThreadTimeTrackingStats)
                {
                    int id = System.Threading.Thread.CurrentThread.ManagedThreadId;
                    if (!trackers.TryGetValue(id, out tracker))
                    {
                        tracker = new ThreadTrackingStatistic("ThreadPoolThread." + System.Threading.Thread.CurrentThread.ManagedThreadId);
                        bool added = trackers.TryAdd(id, tracker);
                        if (added)
                        {
                            tracker.OnStartExecution();
                        }
                    }
                    tracker.OnStartProcessing();
                }
#endif
                try
                {
                    _buffer.UpdateReceivedData(transportBuffer, bytesReceived);

                    while (true)
                    {
                        Message msg = null;
                        try
                        {
                            if (!this._buffer.TryDecodeMessage(out msg)) break;
                            this.IMA.HandleMessage(msg);
                        }
                        catch (Exception exception)
                        {
                            // If deserialization completely failed or the message was one-way, rethrow the exception
                            // so that it can be handled at another level.
                            if (msg?.Headers == null || msg.Direction != Message.Directions.Request)
                            {
                                throw;
                            }

                            // The message body was not successfully decoded, but the headers were.
                            // Send a fast fail to the caller.
                            MessagingStatisticsGroup.OnRejectedMessage(msg);
                            var response = this.messageFactory.CreateResponseMessage(msg);
                            response.Result = Message.ResponseTypes.Error;
                            response.BodyObject = Response.ExceptionResponse(exception);

                            // Send the error response and continue processing the next message.
                            this.IMA.MessageCenter.SendMessage(response);
                        }
                    }
                }
                catch (Exception exc)
                {
                    try
                    {
                        // Log details of receive state machine
                        IMA.Log.Error(ErrorCode.MessagingProcessReceiveBufferException,
                            $"Exception trying to process {bytesReceived} bytes from endpoint {RemoteEndPoint}",
                            exc);
                    }
                    catch (Exception) { }
                    _buffer.Reset(); // Reset back to a hopefully good base state

                    throw;
                }
#if TRACK_DETAILED_STATS
                finally
                {
                    if (StatisticsCollector.CollectThreadTimeTrackingStats)
                    {
                        tracker.IncrementNumberOfProcessed();
                        tracker.OnStopProcessing();
                    }
                }
#endif
            }

            public void Reset()
            {
                _buffer.Reset();
            }
        }
    }
}