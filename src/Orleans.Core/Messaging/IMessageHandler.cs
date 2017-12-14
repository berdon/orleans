using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Text;

namespace Orleans.Messaging
{
    internal interface IMessageHandler
    {
        void HandleMessage(Message msg);
    }
}
