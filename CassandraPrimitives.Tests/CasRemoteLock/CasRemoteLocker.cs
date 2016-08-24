using System;
using System.Linq;
using System.Threading;

using Cassandra;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.CasRemoteLock
{
    public class CasRemoteLocker : IDisposable
    {
        private readonly ISession session;
        private readonly LeaseProlonger leaseProlonger;
        private static readonly string currentProcessId;
        private readonly PreparedStatement tryAcquireStatement, releaseStatement;

        static CasRemoteLocker()
        {
            currentProcessId = Guid.NewGuid().ToString();
        }

        internal CasRemoteLocker(ISession session, TimeSpan lockTtl, PreparedStatement tryProlongStatement, PreparedStatement tryAcquireStatement, PreparedStatement releaseStatement)
        {
            this.session = session;
            this.tryAcquireStatement = tryAcquireStatement;
            this.releaseStatement = releaseStatement;
            leaseProlonger = new LeaseProlonger(session, lockTtl, tryProlongStatement);
        }

        public bool TryAcquire(string lockId, out IDisposable releaser)
        {
            return TryAcquire(lockId, currentProcessId + Thread.CurrentThread.ManagedThreadId, out releaser);
        }

        private bool TryAcquire(string lockId, string processId, out IDisposable releaser)
        {
            var rowSet = session.Execute(tryAcquireStatement.Bind(new {Owner = processId, LockId = lockId}));
            var row = rowSet.Single();
            var applied = row.GetValue<bool>("[applied]");//TODO we can get owner here
            if (applied)
            {
                releaser = new LockReleaser(session, lockId, processId, releaseStatement);
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
            private readonly string processId;
            private readonly PreparedStatement releaseStatement;

            public LockReleaser(ISession session, string lockId, string processId, PreparedStatement releaseStatement)
            {
                this.session = session;
                this.lockId = lockId;
                this.processId = processId;
                this.releaseStatement = releaseStatement;
            }
            public void Dispose()
            {
                var rowSet = session.Execute(releaseStatement.Bind(new { Owner = processId, LockId = lockId }));
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