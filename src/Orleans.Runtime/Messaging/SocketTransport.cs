using Microsoft.Extensions.Logging;
using Orleans.Messaging;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.Messaging
{
    public class SocketTransport : ITransport
    {
        internal Socket Socket;

        private readonly ExecutorService executorService;
        private readonly ILoggerFactory loggerFactory;
        private readonly ILogger log;

        private IncomingMessageSocketAcceptor acceptor;

        public bool Connected => Socket.Connected;
        public bool IsListening { get; private set; } = false;
        public object UserToken { get; set; }
        public EndPoint LocalEndPoint => Socket.LocalEndPoint;
        public EndPoint RemoteEndPoint => Socket.RemoteEndPoint;

        public void Close()
        {
            SocketManager.CloseSocket(Socket);
        }

        public void ListenAsync(ITransportListener listener)
        {
            if (IsListening || acceptor != null)
            {
                throw new Exception("Already listening");
            }

            acceptor = new IncomingMessageSocketAcceptor(Socket, executorService, loggerFactory);
            acceptor.Start();
        }

        public void RestartListenAsync(ITransportListener listener)
        {
            // The IMSA handles closing the underlying socket
            Socket = SocketManager.GetAcceptingSocketForEndpoint((IPEndPoint)LocalEndPoint);
            acceptor.RestartAcceptingSocket(Socket);
        }

        public int Receive(byte[] buffer, int offset, int length)
        {
            return Socket.Receive(buffer, offset, length, SocketFlags.None);
        }

        public int Receive(IList<ArraySegment<byte>> buffer)
        {
            return Socket.Receive(buffer);
        }

        public int Send(List<ArraySegment<byte>> buffer)
        {
            return Socket.Send(buffer);
        }

        public int Send(byte[] buffer)
        {
            return Socket.Send(buffer);
        }

        public void Connect(EndPoint endpoint, TimeSpan connectionTimeout)
        {
            SocketManager.Connect(Socket, endpoint, connectionTimeout);

            // Prep the socket so it will reset on close and won't Nagle
            Socket.LingerState = new LingerOption(true, 0);
            Socket.NoDelay = true;
        }

        public void ReceiveAsync(ITransportReceiver receiver)
        {
            // Start an asynch receive off of the socket to detect closure
            var receiveAsyncEventArgs = new SocketAsyncEventArgs
            {
                BufferList = new List<ArraySegment<byte>> { new ArraySegment<byte>(new byte[4]) },
                UserToken = Tuple.Create(this, receiver)
            };

            receiveAsyncEventArgs.Completed += ReceiveAsyncHandler;

            bool complete = Socket.ReceiveAsync(receiveAsyncEventArgs);
            if (complete)
            {
                // Filter through the normal async handler call to make sure the saea gets disposed
                ReceiveAsyncHandler(this, receiveAsyncEventArgs);
            }
        }

        private void ReceiveAsyncHandler(object sender, SocketAsyncEventArgs args)
        {
            (var transport, var receiver) = args.UserToken as Tuple<SocketTransport, ITransportReceiver>;
            if (transport == null)
            {
                log?.Error(-1, "Received unexpected user token from receive async socket");
                return;
            }

            if (receiver == null)
            {
                log?.Error(-1, "Received unexpected user token from receive async socket");
                transport.Close();
                return;
            }

            try
            {
                receiver.OnReceive(transport, TransportError.Success, args.Buffer, args.BytesTransferred);
            }
            catch (Exception e)
            {
                log?.Error(-1, "Error occured while receiving data from receive async", e);
            }
            finally
            {
                args.Dispose();
            }
        }

        internal SocketTransport(Socket socket, object userToken)
            : this(socket, userToken, null, null, null)
        {
        }

        internal SocketTransport(Socket socket, object userToken, ExecutorService executorService, ILoggerFactory loggerFactory)
        {
            this.Socket = socket;
            UserToken = userToken;
            this.socketManager = socketManager;
            this.executorService = executorService;
            this.loggerFactory = loggerFactory;
            log = loggerFactory.CreateLogger<SocketTransport>();
        }


    }
}
