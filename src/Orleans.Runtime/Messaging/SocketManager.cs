using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Messaging;
using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace Orleans.Runtime.Messaging
{
    internal static class SocketManager
    {
        /// <summary>
        /// Creates a socket bound to an address for use accepting connections.
        /// This is for use by client gateways and other acceptors.
        /// </summary>
        /// <param name="address">The address to bind to.</param>
        /// <returns>The new socket, appropriately bound.</returns>
        internal static Socket GetAcceptingSocketForEndpoint(EndPoint address, TimeSpan syncReceiveTimeout)
        {
            var s = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                // Prep the socket so it will reset on close
                s.LingerState = new LingerOption(true, 0);
                s.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
                s.EnableFastpath();
                // The following timeout is only effective when calling the synchronous
                // Socket.Receive method. We should only use this method when we are reading
                // the connection preamble
                s.ReceiveTimeout = (int)syncReceiveTimeout.TotalMilliseconds;

                // And bind it to the address
                s.Bind(address);
            }
            catch (Exception)
            {
                CloseSocket(s);
                throw;
            }
            return s;
        }

        /// <summary>
        /// Connect the socket to the target endpoint
        /// </summary>
        /// <param name="s">The socket</param>
        /// <param name="endPoint">The target endpoint</param>
        /// <param name="connectionTimeout">The timeout value to use when opening the connection</param>
        /// <exception cref="TimeoutException">When the connection could not be established in time</exception>
        internal static void Connect(Socket s, EndPoint endPoint, TimeSpan connectionTimeout)
        {
            var signal = new AutoResetEvent(false);
            var e = new SocketAsyncEventArgs();
            e.RemoteEndPoint = endPoint;
            e.Completed += (sender, eventArgs) => signal.Set();
            s.ConnectAsync(e);

            if (!signal.WaitOne(connectionTimeout))
                throw new TimeoutException($"Connection to {endPoint} could not be established in {connectionTimeout}");

            if (e.SocketError != SocketError.Success || !s.Connected)
                throw new OrleansException($"Could not connect to {endPoint}: {e.SocketError}");
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        internal static Socket SendingTransportCreator()
        {
            var s = new Socket(SocketType.Stream, ProtocolType.Tcp);
            s.EnableFastpath();
            return s;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        internal static void CloseSocket(Socket s)
        {
            if (s == null)
            {
                return;
            }

            try
            {
                s.Shutdown(SocketShutdown.Both);
            }
            catch (ObjectDisposedException)
            {
                // Socket is already closed -- we're done here
                return;
            }
            catch (Exception)
            {
                // Ignore
            }

            try
            {
                s.Disconnect(false);
            }
            catch (Exception)
            {
                // Ignore
            }

            try
            {
                s.Dispose();
            }
            catch (Exception)
            {
                // Ignore
            }
        }
    }
}
