using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;

using Cassandra;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.CasRemoteLock
{
    public class CasRemoteLocker : IDisposable
    {
        private readonly Cluster cluster;
        private readonly ISession session;
        private readonly string keyspaceName;
        private readonly string tableName;
        private readonly TimeSpan lockTtl;
        private readonly LeaseProlonger leaseProlonger;
        private static readonly string currentProcessId;

        static CasRemoteLocker()
        {
            currentProcessId = Guid.NewGuid().ToString();
        }

        public CasRemoteLocker(List<IPEndPoint> endpoints, string keyspaceName, string tableName, TimeSpan lockTtl)
        {
            cluster = Cluster
                .Builder()
                .AddContactPoints(endpoints)
                .Build();
            session = cluster.Connect(keyspaceName);
            leaseProlonger = new LeaseProlonger(endpoints, keyspaceName, tableName, lockTtl);
            this.keyspaceName = keyspaceName;
            this.tableName = tableName;
            this.lockTtl = lockTtl;
        }

        public void ActualiseTables()
        {
            //session.Execute(string.Format("DROP TABLE \"{0}\";", tableName), ConsistencyLevel.Quorum);//TODO
            //if (session.Execute("SELECT * FROM system.schema_columnfamilies;", ConsistencyLevel.Quorum).Any(row => row.GetValue<string>("columnfamily_name") == tableName))
            //    return;
            try
            {
                session.Execute(string.Format("CREATE TABLE \"{0}\" (", tableName) +
                                            "lock_id text PRIMARY KEY," +
                                            "owner text," +
                                            ");", ConsistencyLevel.Quorum);
            }
            catch (Exception)
            {
                //ignore
            }
        }

        public bool TryAcquire(string lockId, out IDisposable releaser)
        {
            return TryAcquire(lockId, currentProcessId + Thread.CurrentThread.ManagedThreadId, out releaser);
        }

        private bool TryAcquire(string lockId, string processId, out IDisposable releaser)
        {
            var rowSet = session.Execute(string.Format("UPDATE \"{0}\" ", tableName) +
                                         string.Format("USING TTL {0} ", lockTtl.Seconds) +
                                         string.Format("SET owner = '{0}' ", processId) +
                                         string.Format("WHERE lock_id = '{0}' ", lockId) +
                                         "IF owner = null;", ConsistencyLevel.Quorum);
            var row = rowSet.Single();
            var applied = row.GetValue<bool>("[applied]");//TODO we can get owner here
            if (applied)
            {
                releaser = new LockReleaser(session, tableName, lockId, processId);
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

            public LockReleaser(ISession session, string tableName, string lockId, string processId)
            {
                this.session = session;
                this.lockId = lockId;
                this.tableName = tableName;
                this.processId = processId;
            }
            public void Dispose()
            {
                var rowSet = session.Execute(string.Format("DELETE FROM \"{0}\"", tableName) +
                                             string.Format("WHERE lock_id = '{0}'", lockId) +
                                             string.Format("IF owner = '{0}'", processId), ConsistencyLevel.Quorum);//TODO: we can delete without CAS here, actually
                var applied = rowSet.Single().GetValue<bool>("[applied]");
                if (!applied)
                    throw new Exception(string.Format("Can't release lock {0}, because we don't own it", processId));
            }
        }

        public void Dispose()
        {
            session.Dispose();
            cluster.Dispose();
            leaseProlonger.Dispose();
        }
    }
}