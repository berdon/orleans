using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Orleans.Messaging;
using Orleans.Serialization;

namespace Orleans.Runtime.Messaging
{
    internal class IncomingMessageSocketAcceptor : SingleTaskAsynchAgent
    {
        protected ITransportListener Listener;

        private readonly ConcurrentObjectPool<SaeaPoolWrapper> receiveEventArgsPool;
        private const int SocketBufferSize = 1024 * 128; // 128 kb
        private readonly EndPoint listenAddress;
        private Action<Message> sniffIncomingMessageHandler;
        private readonly LingerOption receiveLingerOption = new LingerOption(true, 0);
        internal Socket AcceptingSocket;

        private static readonly CounterStatistic allocatedSocketEventArgsCounter 
            = CounterStatistic.FindOrCreate(StatisticNames.MESSAGE_ACCEPTOR_ALLOCATED_SOCKET_EVENT_ARGS, false);
        private readonly CounterStatistic checkedOutSocketEventArgsCounter;
        private readonly CounterStatistic checkedInSocketEventArgsCounter;
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
        internal IncomingMessageSocketAcceptor(Socket acceptingSocket, ExecutorService executorService, ILoggerFactory loggerFactory)
            :base(executorService, loggerFactory)
        {
            this.loggerFactory = loggerFactory;
            Log = new LoggerWrapper<IncomingMessageAcceptor>(loggerFactory);
            this.listenAddress = acceptingSocket.LocalEndPoint;
            this.receiveEventArgsPool = new ConcurrentObjectPool<SaeaPoolWrapper>(() => this.CreateSocketReceiveAsyncEventArgsPoolWrapper());

            AcceptingSocket = acceptingSocket;
            Log.Info(ErrorCode.Messaging_IMA_OpenedListeningSocket, "Opened a listening socket at address " + AcceptingSocket.LocalEndPoint);
            OnFault = FaultBehavior.CrashOnFault;

            checkedOutSocketEventArgsCounter = CounterStatistic.FindOrCreate(StatisticNames.MESSAGE_ACCEPTOR_CHECKED_OUT_SOCKET_EVENT_ARGS, false);
            checkedInSocketEventArgsCounter = CounterStatistic.FindOrCreate(StatisticNames.MESSAGE_ACCEPTOR_CHECKED_IN_SOCKET_EVENT_ARGS, false);

            IntValueStatistic.FindOrCreate(StatisticNames.MESSAGE_ACCEPTOR_IN_USE_SOCKET_EVENT_ARGS,
                () => checkedOutSocketEventArgsCounter.GetCurrentValue() - checkedInSocketEventArgsCounter.GetCurrentValue());
        }

        protected override void Run()
        {
            try
            {
                AcceptingSocket.Listen(LISTEN_BACKLOG_SIZE);
                StartAccept(null);
            }
            catch (Exception ex)
            {
                Log.Error(ErrorCode.MessagingAcceptAsyncSocketException, "Exception beginning accept on listening socket", ex);
                throw;
            }
            if (Log.IsVerbose3) Log.Verbose3("Started accepting connections.");
        }

        public override void Stop()
        {
            base.Stop();

            if (Log.IsVerbose) Log.Verbose("Disconnecting the listening socket");
            SocketManager.CloseSocket(AcceptingSocket);
        }

        /// <summary>
        /// Begins an operation to accept a connection request from the client.
        /// </summary>
        /// <param name="acceptEventArg">The context object to use when issuing 
        /// the accept operation on the server's listening socket.</param>
        private void StartAccept(SocketAsyncEventArgs acceptEventArg)
        {
            if (acceptEventArg == null)
            {
                acceptEventArg = new SocketAsyncEventArgs();
                acceptEventArg.UserToken = Tuple.Create(this, (object) null);
                acceptEventArg.Completed += OnAcceptCompleted;
            }
            else
            {
                // We have handed off the connection info from the
                // accepting socket to the receiving socket. So, now we will clear 
                // the socket info from that object, so it will be 
                // ready for a new socket
                acceptEventArg.AcceptSocket = null;
            }

            // Socket.AcceptAsync begins asynchronous operation to accept the connection.
            // Note the listening socket will pass info to the SocketAsyncEventArgs
            // object that has the Socket that does the accept operation.
            // If you do not create a Socket object and put it in the SAEA object
            // before calling AcceptAsync and use the AcceptSocket property to get it,
            // then a new Socket object will be created by .NET. 
            try
            {
                // AcceptAsync returns true if the I / O operation is pending.The SocketAsyncEventArgs.Completed event 
                // on the e parameter will be raised upon completion of the operation.Returns false if the I/O operation 
                // completed synchronously. The SocketAsyncEventArgs.Completed event on the e parameter will not be raised 
                // and the e object passed as a parameter may be examined immediately after the method call returns to retrieve 
                // the result of the operation.
                while (!AcceptingSocket.AcceptAsync(acceptEventArg))
                {
                    ProcessAccept(acceptEventArg, true);
                }
            }
            catch (SocketException ex)
            {
                Log.Warn(ErrorCode.MessagingAcceptAsyncSocketException, "Socket error on accepting socket during AcceptAsync {0}", ex.SocketErrorCode);
                Listener.OnError(ex);
            }
            catch (ObjectDisposedException ex)
            {
                // Socket was closed, but we're not shutting down; we need to open a new socket and start over...
                // Close the old socket and open a new one
                Log.Warn(ErrorCode.MessagingAcceptingSocketClosed, "Accepting socket was closed when not shutting down");
                Listener.OnError(ex);
            }
            catch (Exception ex)
            {
                // There was a network error. We need to get a new accepting socket and re-issue an accept before we continue.
                // Close the old socket and open a new one
                Log.Warn(ErrorCode.MessagingAcceptAsyncSocketException, "Exception on accepting socket during AcceptAsync", ex);
                Listener.OnError(ex);
            }
        }

        private void OnAcceptCompleted(object sender, SocketAsyncEventArgs e)
        {
            ((IncomingMessageSocketAcceptor)e.UserToken).ProcessAccept(e, false);
        }

        /// <summary>
        /// Process the accept for the socket listener.
        /// </summary>
        /// <param name="e">SocketAsyncEventArg associated with the completed accept operation.</param>
        /// <param name="completedSynchronously">Shows whether AcceptAsync completed synchronously, 
        /// if true - the next accept operation woun't be started. Used for avoiding potential stack overflows.</param>
        private void ProcessAccept(SocketAsyncEventArgs e, bool completedSynchronously)
        {
            var token = e.UserToken as Tuple<IncomingMessageSocketAcceptor, object>;
            var imsa = token.Item1;
            try
            {
                if (token == null)
                {
                    Log.Warn(ErrorCode.Messaging_IMA_AcceptCallbackUnexpectedState,
                        "AcceptCallback invoked with an unexpected async state of type {0}",
                        e.UserToken?.GetType().ToString() ?? "null");
                    Listener.OnAccept(null, TransportError.Error);
                    return;
                }

                var transport = new SocketTransport(e.AcceptSocket, token.Item2);

                if (e.SocketError != SocketError.Success)
                {
                    Listener.OnAccept(transport, TransportError.Error);
                    return;
                }

                Listener.OnAccept(transport, TransportError.Success);

                // First check to see if we're shutting down, in which case there's no point in doing anything other
                // than closing the accepting socket and returning.
                if (imsa.Cts == null || imsa.Cts.IsCancellationRequested)
                {
                    SocketManager.CloseSocket(imsa.AcceptingSocket);
                    imsa.Log.Info(ErrorCode.Messaging_IMA_ClosingSocket, "Closing accepting socket during shutdown");
                    return;
                }

                Socket sock = e.AcceptSocket;
                if (sock.Connected)
                {
                    if (imsa.Log.IsVerbose) imsa.Log.Verbose("Received a connection from {0}", sock.RemoteEndPoint);

                    // Finally, process the incoming request:
                    // Prep the socket so it will reset on close
                    sock.LingerState = receiveLingerOption;

                    if (Listener.OnOpen(transport, TransportError.Success))
                    {
                        // Get the socket for the accepted client connection and put it into the 
                        // ReadEventArg object user token.
                        var readEventArgs = GetSocketReceiveAsyncEventArgs(transport);

                        StartReceiveAsync(sock, readEventArgs, imsa);
                    }
                    else
                    {
                        imsa.SafeCloseSocket(sock);
                    }
                }

                // The next accept will be started in the caller method
                if (completedSynchronously)
                {
                    return;
                }

                // Start a new Accept 
                StartAccept(e);
            }
            catch (Exception ex)
            {
                var logger = imsa?.Log ?? this.Log;
                logger.Error(ErrorCode.Messaging_IMA_ExceptionAccepting, "Unexpected exception in IncomingMessageAccepter.AcceptCallback", ex);
                Listener.OnAccept(null, TransportError.Error);
            }
        }

        private void StartReceiveAsync(Socket sock, SocketAsyncEventArgs readEventArgs, IncomingMessageSocketAcceptor ima)
        {
            try
            {
                // Set up the async receive
                if (!sock.ReceiveAsync(readEventArgs))
                {
                    ProcessReceive(readEventArgs);
                }
            }
            catch (Exception exception)
            {
                var socketException = exception as SocketException;
                var context = readEventArgs.UserToken as ReceiveCallbackContext;
                ima.Log.Warn(ErrorCode.Messaging_IMA_NewBeginReceiveException,
                    $"Exception on new socket during ReceiveAsync with RemoteEndPoint " +
                    $"{socketException?.SocketErrorCode}: {context?.RemoteEndPoint}", exception);
                ima.SafeCloseSocket(sock);
                FreeSocketAsyncEventArgs(readEventArgs);
            }
        }

        private SocketAsyncEventArgs GetSocketReceiveAsyncEventArgs(SocketTransport transport)
        {
            var saea = receiveEventArgsPool.Allocate();
            var token = ((ReceiveCallbackContext) saea.SocketAsyncEventArgs.UserToken);
            token.IMA = this;
            token.Transport = transport;
            checkedOutSocketEventArgsCounter.Increment();
            return saea.SocketAsyncEventArgs;
        }

        private SaeaPoolWrapper CreateSocketReceiveAsyncEventArgsPoolWrapper()
        {
            SocketAsyncEventArgs readEventArgs = new SocketAsyncEventArgs();
            readEventArgs.Completed += OnReceiveCompleted;

            var buffer = new byte[SocketBufferSize];

            // SocketAsyncEventArgs and ReceiveCallbackContext's buffer shares the same buffer list with pinned arrays.
            readEventArgs.SetBuffer(buffer, 0, buffer.Length);
            var poolWrapper = new SaeaPoolWrapper(readEventArgs);

            // Creates with incomplete state: IMA should be set before using
            readEventArgs.UserToken = new ReceiveCallbackContext(poolWrapper);
            allocatedSocketEventArgsCounter.Increment();
            return poolWrapper;
        }

        private void FreeSocketAsyncEventArgs(SocketAsyncEventArgs args)
        {
            var receiveToken = (ReceiveCallbackContext) args.UserToken;
            args.AcceptSocket = null;
            checkedInSocketEventArgsCounter.Increment();
            receiveEventArgsPool.Free(receiveToken.SaeaPoolWrapper);
        }

        private static void OnReceiveCompleted(object sender, SocketAsyncEventArgs e)
        {
            if (e.LastOperation != SocketAsyncOperation.Receive)
            {
                throw new ArgumentException("The last operation completed on the socket was not a receive");
            }

            var rcc = e.UserToken as ReceiveCallbackContext;
            if (rcc.IMA.Log.IsVerbose3) rcc.IMA.Log.Verbose("Socket receive completed from remote " + e.RemoteEndPoint);
            rcc.IMA.ProcessReceive(e);
        }

        /// <summary>
        /// This method is invoked when an asynchronous receive operation completes. 
        /// If the remote host closed the connection, then the socket is closed. 
        /// </summary>
        /// <param name="e">SocketAsyncEventArg associated with the completed receive operation.</param>
        private void ProcessReceive(SocketAsyncEventArgs e)
        {
            var rcc = e.UserToken as ReceiveCallbackContext;

            // If no data was received, close the connection. This is a normal
            // situation that shows when the remote host has finished sending data.
            if (e.BytesTransferred <= 0)
            {
                if (Log.IsVerbose) Log.Verbose("Closing recieving socket: " + e.RemoteEndPoint);
                Listener.OnClose(rcc.Transport, TransportError.Success);
                rcc.IMA.SafeCloseSocket(rcc.Transport.Socket);
                FreeSocketAsyncEventArgs(e);
                return;
            }

            if (e.SocketError != SocketError.Success)
            {
                Log.Warn(ErrorCode.Messaging_IMA_NewBeginReceiveException,
                   $"Socket error on new socket during ReceiveAsync with RemoteEndPoint: {e.SocketError}");

                Listener.OnClose(rcc.Transport, TransportError.Error);

                rcc.IMA.SafeCloseSocket(rcc.Transport.Socket);
                FreeSocketAsyncEventArgs(e);
                return;
            }

            Socket sock = rcc.Transport.Socket;
            try
            {
                rcc.IMA.Listener.OnDataReceive(rcc.Transport, TransportError.Success, e.Buffer, e.BytesTransferred);
            }
            catch (Exception ex)
            {
                rcc.IMA.Log.Error(ErrorCode.Messaging_IMA_BadBufferReceived,
                    $"ProcessReceivedBuffer exception with RemoteEndPoint {rcc.RemoteEndPoint}: ", ex);

                Listener.OnClose(rcc.Transport, TransportError.Error);

                // There was a problem with the buffer, presumably data corruption, so give up
                rcc.IMA.SafeCloseSocket(rcc.Transport.Socket);
                FreeSocketAsyncEventArgs(e);
                // And we're done
                return;
            }

            StartReceiveAsync(sock, e, rcc.IMA);
        }

        public void RestartAcceptingSocket(Socket acceptingSocket)
        {
            try
            {
                if (Log.IsVerbose) Log.Verbose("Restarting of the accepting socket");
                SocketManager.CloseSocket(AcceptingSocket);
                AcceptingSocket = acceptingSocket;
                AcceptingSocket.Listen(LISTEN_BACKLOG_SIZE);
                StartAccept(null);
            }
            catch (Exception ex)
            {
                Log.Error(ErrorCode.Runtime_Error_100016, "Unable to create a new accepting socket", ex);
                throw;
            }
        }

        private void SafeCloseSocket(Socket sock)
        {
            SocketManager.CloseSocket(sock);
        }

        private class SocketToken
        {
            private SocketTransport transport;

            public SocketTransport Transport
            {
                get { return transport; }
                internal set
                {
                    transport = value;
                    RemoteEndPoint = transport.RemoteEndPoint;
                }
            }
            public EndPoint RemoteEndPoint { get; private set; }
            public IncomingMessageSocketAcceptor IMA { get; internal set; }
            public SaeaPoolWrapper SaeaPoolWrapper { get; }
        }

        private class ReceiveCallbackContext
        {
            private SocketTransport transport;

            public SocketTransport Transport {
                get { return transport; }
                internal set
                {
                    transport = value;
                    RemoteEndPoint = transport.RemoteEndPoint;
                }
            }
            public EndPoint RemoteEndPoint { get; private set; }
            public IncomingMessageSocketAcceptor IMA { get; internal set; }
            public SaeaPoolWrapper SaeaPoolWrapper { get; }

            public ReceiveCallbackContext(SaeaPoolWrapper poolWrapper)
            {
                SaeaPoolWrapper = poolWrapper;
            }
        }

        private class SaeaPoolWrapper : PooledResource<SaeaPoolWrapper>
        {
            public SocketAsyncEventArgs SocketAsyncEventArgs { get; }

            public SaeaPoolWrapper(SocketAsyncEventArgs socketAsyncEventArgs)
            {
                SocketAsyncEventArgs = socketAsyncEventArgs;
            }
        }
    }
}