using System;
using System.Linq;
using System.Threading;

using Cassandra;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.CasRemoteLock
{
    public class CasRemoteLocker : IDisposable
    {
        private readonly ISession session;
        private readonly string tableName;
        private readonly TimeSpan lockTtl;
        private readonly LeaseProlonger leaseProlonger;
        private static readonly string currentProcessId;
        private readonly ConsistencyLevel consistencyLevel;

        static CasRemoteLocker()
        {
            currentProcessId = Guid.NewGuid().ToString();
        }

        internal CasRemoteLocker(ISession session, string tableName, TimeSpan lockTtl, ConsistencyLevel consistencyLevel)
        {
            this.session = session;
            leaseProlonger = new LeaseProlonger(session, tableName, lockTtl);
            this.tableName = tableName;
            this.lockTtl = lockTtl;
            this.consistencyLevel = consistencyLevel;
        }

        public bool TryAcquire(string lockId, out IDisposable releaser)
        {
            return TryAcquire(lockId, currentProcessId + Thread.CurrentThread.ManagedThreadId, out releaser);
        }

        private bool TryAcquire(string lockId, string processId, out IDisposable releaser)
        {
            var query = string.Format("UPDATE \"{0}\" ", tableName) +
                          string.Format("USING TTL {0} ", lockTtl.Seconds) +
                          string.Format("SET owner = '{0}' ", processId) +
                          string.Format("WHERE lock_id = '{0}' ", lockId) +
                          "IF owner = null;";
            var rowSet = session.Execute(query, consistencyLevel);
            Console.WriteLine("ok");
            var row = rowSet.Single();
            var applied = row.GetValue<bool>("[applied]");//TODO we can get owner here
            if (applied)
            {
                releaser = new LockReleaser(session, tableName, lockId, processId, consistencyLevel);
                leaseProlonger.AddLock(lockId, processId);
                return true;
            }
            releaser = null;
            return false;
        }

        private class LockReleaser : IDisposable
        {
            private readonly ISession session;
            private readonly string lockId;
            private readonly string tableName;
            private readonly string processId;
            private readonly ConsistencyLevel consistencyLevel;

            public LockReleaser(ISession session, string tableName, string lockId, string processId, ConsistencyLevel consistencyLevel)
            {
                this.session = session;
                this.lockId = lockId;
                this.tableName = tableName;
                this.processId = processId;
                this.consistencyLevel = consistencyLevel;
            }
            public void Dispose()
            {
                var rowSet = session.Execute(string.Format("DELETE FROM \"{0}\"", tableName) +
                                             string.Format("WHERE lock_id = '{0}'", lockId) +
                                             string.Format("IF owner = '{0}'", processId), consistencyLevel);//TODO: we can delete without CAS here, actually
                var applied = rowSet.Single().GetValue<bool>("[applied]");
                if (!applied)
                    throw new Exception(string.Format("Can't release lock {0}, because we don't own it", processId));
            }
        }

        public void Dispose()
        {
            leaseProlonger.Dispose();
        }
    }
}