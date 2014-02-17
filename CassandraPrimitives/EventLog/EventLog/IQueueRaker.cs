using System;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.EventLog
{
    internal interface IQueueRaker : IDisposable
    {
        DeferredResult Enqueue(EventStorageElement[] events, int priority);
    }
}