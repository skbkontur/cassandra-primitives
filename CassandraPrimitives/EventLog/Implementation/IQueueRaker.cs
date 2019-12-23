using System;
using System.Threading.Tasks;

using SkbKontur.Cassandra.Primitives.EventLog.Primitives;

namespace SkbKontur.Cassandra.Primitives.EventLog.Implementation
{
    internal interface IQueueRaker : IDisposable
    {
        Task<ProcessResult> ProcessAsync(EventStorageElement[] events, int priority);
    }
}