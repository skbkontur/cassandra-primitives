using System;
using System.Linq;
using System.Threading;

using Cassandra;

using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;

namespace SKBKontur.Catalogue.CassandraPrimitives.CasRemoteLock
{
    public class CasRemoteLocker : IDisposable, IRemoteLockCreator
    {
        private readonly ISession session;
        private readonly LeaseProlonger leaseProlonger;
        //private static readonly string currentProcessId;
        private readonly CasRemoteLockerPreparedStatements preparedStatements;
        private readonly Random random;

        /*static CasRemoteLocker()
        {
            currentProcessId = Guid.NewGuid().ToString();
        }*/

        internal CasRemoteLocker(ISession session, TimeSpan prolongIntervalMs, CasRemoteLockerPreparedStatements preparedStatements)
        {
            this.session = session;
            this.preparedStatements = preparedStatements;
            random = new Random();
            leaseProlonger = new LeaseProlonger(session, prolongIntervalMs, preparedStatements.TryProlongStatement);
        }

        /*public bool TryAcquire(string lockId, out IRemoteLock releaser)
        {
            return TryAcquire(lockId, currentProcessId + Thread.CurrentThread.ManagedThreadId, out releaser);
        }*/

        public bool TryAcquire(string lockId, out IRemoteLock releaser)
        {
            return TryAcquire(lockId, Guid.NewGuid().ToString(), out releaser);
        }

        private bool TryAcquire(string lockId, string processId, out IRemoteLock releaser)
        {
            var rowSet = Execute(session, preparedStatements.TryAcquireStatement.Bind(new {Owner = processId, LockId = lockId}));
            var row = rowSet.Single();
            var applied = row.GetValue<bool>("[applied]");//TODO we can get owner here
            if (applied)
            {
                releaser = new CasRemoteLock(session, lockId, processId, preparedStatements.ReleaseStatement);
                leaseProlonger.AddLock(lockId, processId);
                return true;
            }
            releaser = null;
            return false;
        }

        private IRemoteLock Acquire(string lockId, string processId)
        {
            while (true)
            {
                IRemoteLock releaser;
                if (TryAcquire(lockId, processId, out releaser))
                    return releaser;
                var longSleep = random.Next(1000);
                Thread.Sleep(longSleep);
            }
        }
        public IRemoteLock Acquire(string lockId)
        {
            return Acquire(lockId, Guid.NewGuid().ToString());
        }

        public IRemoteLock Lock(string lockId)
        {
            return Acquire(lockId);
        }

        public bool TryGetLock(string lockId, out IRemoteLock releaser)
        {
            return TryAcquire(lockId, out releaser);
        }

        public string GetLockOwner(string lockId)
        {
            var rowSet = Execute(session, preparedStatements.GetLockOwnerStatement.Bind(new { LockId = lockId })).ToList();
            if (rowSet.Count == 0)
                return null;
            if (rowSet.Count > 1)
                throw new Exception("Lock has more than one owner");
            var row = rowSet.Single();
            var owner = row.GetValue<string>("owner");
            return owner;
        }

        public static RowSet Execute(ISession session, IStatement statement, int attempts = 5)
        {
            Exception lastException = null;
            for (int i = 0; i < attempts; i++)
            {
                try
                {
                    var rowSet = session.Execute(statement);
                    return rowSet;
                }
                catch (Exception e)
                {
                    lastException = e;
                }
            }
            throw new Exception(string.Format("Failed to execute statement after 5 attempts. Last exception:\n{0}", lastException));
        }

        private class CasRemoteLock : IRemoteLock
        {
            private readonly ISession session;
            private readonly string lockId;
            private readonly string processId;
            private readonly PreparedStatement releaseStatement;

            public CasRemoteLock(ISession session, string lockId, string processId, PreparedStatement releaseStatement)
            {
                this.session = session;
                this.lockId = lockId;
                this.processId = processId;
                this.releaseStatement = releaseStatement;
            }
            public void Dispose()
            {
                var rowSet = Execute(session, releaseStatement.Bind(new {Owner = processId, LockId = lockId}));
                var applied = rowSet.Single().GetValue<bool>("[applied]");
                //if (!applied)
                //    throw new Exception(string.Format("Can't release lock {0}, because we don't own it", processId));
            }

            public string LockId { get { return lockId; } }
            public string ThreadId { get { return processId; } }
        }

        public void Dispose()
        {
            leaseProlonger.Dispose();
        }
    }
}