using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Cassandra;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.CasRemoteLock
{
    internal class LeaseProlonger : IDisposable
    {
        private readonly ConcurrentDictionary<Tuple<string, string>, bool> locksToProlong;
        private readonly ISession session;
        private readonly int prolongIntervalMs;
        private bool stopped;
        private readonly PreparedStatement tryProlongStatement;

        public LeaseProlonger(ISession session, TimeSpan prolongIntervalMs, PreparedStatement tryProlongStatement)
        {
            this.tryProlongStatement = tryProlongStatement;
            locksToProlong = new ConcurrentDictionary<Tuple<string, string>, bool>();
            this.session = session;
            this.prolongIntervalMs = (int)prolongIntervalMs.TotalMilliseconds;
            new Thread(InfinetelyProlongLocks).Start();
        }

        public void AddLock(string lockId, string processId)
        {
            locksToProlong.AddOrUpdate(Tuple.Create(lockId, processId), _ => true, (_a, _b) => true);
            //if (!locksToProlong.TryAdd(Tuple.Create(lockId, processId), true))
            //    throw new Exception(string.Format("Failed to add lock {0} of process {1} to prolonger", lockId, processId));
        }

        public void InfinetelyProlongLocks()
        {
            while (!stopped)
            {
                try
                {
                    var toRemove = new List<Tuple<string, string>>();
                    foreach (var lockAndProcess in locksToProlong)
                    {
                        if (!TryProlongSingleLock(lockAndProcess.Key.Item1, lockAndProcess.Key.Item2))
                        {
                            toRemove.Add(lockAndProcess.Key);
                        }
                    }
                    foreach (var removable in toRemove)
                    {
                        bool _;
                        locksToProlong.TryRemove(removable, out _);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Exception while prolonging:\n{0}", e);
                }
                Task.Delay(prolongIntervalMs).Wait();
            }
        }

        private bool TryProlongSingleLock(string lockId, string processId)
        {
            var rowSet = session.Execute(tryProlongStatement.Bind(new {Owner = processId, LockId = lockId}));
            var applied = rowSet.Single().GetValue<bool>("[applied]");
            return applied;
        }

        private void ProlongSingleLock(string lockId, string processId)
        {
            if (!TryProlongSingleLock(lockId, processId))
                throw new Exception(string.Format("Can't prolong lock {0} because process {1} doesn't own it", lockId, processId));
        }

        public void Dispose()
        {
            stopped = true;
        }
    }
}