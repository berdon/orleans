using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using Orleans.Serialization;


namespace Orleans.Messaging
{
    internal enum TransportDirection
    {
        SiloToSilo,
        ClientToGateway,
        GatewayToClient
    }

    internal abstract class OutgoingMessageSender : AsynchQueueAgent<Message>
    {
        private readonly SerializationManager serializationManager;

        internal OutgoingMessageSender(string nameSuffix, SerializationManager serializationManager, ExecutorService executorService, ILoggerFactory loggerFactory)
            : base(nameSuffix, executorService, loggerFactory)
        {
            this.serializationManager = serializationManager;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        protected override void Process(Message msg)
        {
            if (Log.IsVerbose2) Log.Verbose2("Got a {0} message to send: {1}", msg.Direction, msg);
            bool continueSend = PrepareMessageForSend(msg);
            if (!continueSend) return;

            ITransport transport;
            string error;
            SiloAddress targetSilo;
            continueSend = GetSendingTransport(msg, out transport, out targetSilo, out error);
            if (!continueSend)
            {
                OnGetSendingTransportFailure(msg, error);
                return;
            }

            List<ArraySegment<byte>> data;
            int headerLength = 0;
            try
            {
                int bodyLength;
                data = msg.Serialize(this.serializationManager, out headerLength, out bodyLength);
                if (headerLength + bodyLength > this.serializationManager.LargeObjectSizeThreshold)
                {
                    this.Log.Info(ErrorCode.Messaging_LargeMsg_Outgoing, "Preparing to send large message Size={0} HeaderLength={1} BodyLength={2} #ArraySegments={3}. Msg={4}",
                        headerLength + bodyLength + Message.LENGTH_HEADER_SIZE, headerLength, bodyLength, data.Count, this.ToString());
                    if (this.Log.IsVerbose3) this.Log.Verbose3("Sending large message {0}", msg.ToLongString());
                }
            }
            catch (Exception exc)
            {
                this.OnMessageSerializationFailure(msg, exc);
                return;
            }

            int length = data.Sum(x => x.Count);
            int bytesSent = 0;
            bool exceptionSending = false;
            bool countMismatchSending = false;
            string sendErrorStr = null;
            try
            {
                bytesSent = transport.Send(data);
                if (bytesSent != length)
                {
                    // The complete message wasn't sent, even though no error was reported; treat this as an error
                    countMismatchSending = true;
                    sendErrorStr = String.Format("Byte count mismatch on sending to {0}: sent {1}, expected {2}", targetSilo, bytesSent, length);
                    Log.Warn(ErrorCode.Messaging_CountMismatchSending, sendErrorStr);
                }
            }
            catch (Exception exc)
            {
                exceptionSending = true;
                if (!(exc is ObjectDisposedException))
                {
                    sendErrorStr = String.Format("Exception sending message to {0}. Message: {1}. {2}", targetSilo, msg, exc);
                    Log.Warn(ErrorCode.Messaging_ExceptionSending, sendErrorStr, exc);
                }
            }
            MessagingStatisticsGroup.OnMessageSend(targetSilo, msg.Direction, bytesSent, headerLength, GetTransportDirection());
            bool sendError = exceptionSending || countMismatchSending;
            if (sendError)
                OnSendFailure(transport, targetSilo);

            ProcessMessageAfterSend(msg, sendError, sendErrorStr);
        }

        protected abstract TransportDirection GetTransportDirection();
        protected abstract bool PrepareMessageForSend(Message msg);
        protected abstract bool GetSendingTransport(Message msg, out ITransport transport, out SiloAddress targetSilo, out string error);
        protected abstract void OnGetSendingTransportFailure(Message msg, string error);
        protected abstract void OnMessageSerializationFailure(Message msg, Exception exc);
        protected abstract void OnSendFailure(ITransport transport, SiloAddress targetSilo);
        protected abstract void ProcessMessageAfterSend(Message msg, bool sendError, string sendErrorStr);
        protected abstract void FailMessage(Message msg, string reason);
    }
}
