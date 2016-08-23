using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

using Cassandra;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.CasRemoteLock
{
    internal class LeaseProlonger : IDisposable
    {
        private readonly ConcurrentDictionary<Tuple<string, string>, bool> locksToProlong;
        private readonly ISession session;
        private readonly string tableName;
        private readonly TimeSpan lockTtl;
        private readonly Task prolongTask;
        private bool stopped;

        public LeaseProlonger(IEnumerable<IPEndPoint> endpoints, string keyspaceName, string tableName, TimeSpan lockTtl)//TODO: or create it inside as in CasRemoteLocker?
        {
            var cluster = Cluster
                .Builder()
                .AddContactPoints(endpoints)
                .Build();
            locksToProlong = new ConcurrentDictionary<Tuple<string, string>, bool>();
            session = cluster.Connect(keyspaceName);
            this.tableName = tableName;
            this.lockTtl = lockTtl;
            prolongTask = Task.Run(() => InfinetelyProlongLocks());
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
                Task.Delay(prolongIntervalMs);
            }
        }

        private bool TryProlongSingleLock(string lockId, string processId)
        {
            var rowSet = session.Execute(string.Format("UPDATE \"{0}\" ", tableName) +
                                         string.Format("USING TTL {0} ", lockTtl.Seconds) +
                                         string.Format("SET owner = '{0}' ", processId) +
                                         string.Format("WHERE lock_id = '{0}' ", lockId) +
                                         string.Format("IF owner = '{0}';", processId));
            var applied = rowSet.Single().GetValue<bool>("[applied]");
            return applied;
        }

        private void ProlongSingleLock(string lockId, string processId)
        {
            if (!TryProlongSingleLock(lockId, processId))
                throw new Exception(string.Format("Can't prolong lock {0} because process {1} doesn't own it", lockId, processId));
        }

        private const int prolongIntervalMs = 500;
        public void Dispose()
        {
            stopped = true;
        }
    }
}