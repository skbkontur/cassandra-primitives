using System;
using System.Threading.Tasks;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Implementation
{
    internal interface IQueueRaker : IDisposable
    {
        Task<EnqueueResult> EnqueueAsync(EventStorageElement[] events, int priority);
    }
}