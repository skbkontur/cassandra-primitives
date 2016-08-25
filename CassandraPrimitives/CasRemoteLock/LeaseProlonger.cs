using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Cassandra;

namespace SKBKontur.Catalogue.CassandraPrimitives.CasRemoteLock
{
    internal class EnqueuedLockToProlong
    {
        public EnqueuedLockToProlong(string lockId, string processId, DateTime nextProlong)
        {
            LockId = lockId;
            ProcessId = processId;
            NextProlong = nextProlong;
        }

        public string LockId { get; private set; }
        public string ProcessId { get; private set; }
        public DateTime NextProlong { get; private set; }
    }
    internal class LeaseProlonger : IDisposable
    {
        private readonly ConcurrentQueue<EnqueuedLockToProlong> locksToProlong;
        private readonly ISession session;
        private readonly TimeSpan prolongInterval;
        private bool stopped;
        private readonly PreparedStatement tryProlongStatement;

        public LeaseProlonger(ISession session, TimeSpan prolongInterval, PreparedStatement tryProlongStatement)
        {
            this.tryProlongStatement = tryProlongStatement;
            locksToProlong = new ConcurrentQueue<EnqueuedLockToProlong>();
            this.session = session;
            this.prolongInterval = prolongInterval;
            new Thread(InfinetelyProlongLocks).Start();
        }

        public void AddLock(string lockId, string processId)
        {
            locksToProlong.Enqueue(new EnqueuedLockToProlong(lockId, processId, DateTime.Now.Add(prolongInterval)));
            //if (!locksToProlong.TryAdd(Tuple.Create(lockId, processId), true))
            //    throw new Exception(string.Format("Failed to add lock {0} of process {1} to prolonger", lockId, processId));
        }

        public void InfinetelyProlongLocks()
        {
            while (!stopped)
            {
                try
                {
                    EnqueuedLockToProlong lockToProlong;
                    if(!locksToProlong.TryDequeue(out lockToProlong))
                    {
                        Thread.Sleep((int)prolongInterval.TotalMilliseconds);
                        continue;
                    }

                    var delta = lockToProlong.NextProlong.Subtract(DateTime.Now);
                    if (delta.TotalMilliseconds < 0)
                        Console.WriteLine("Performing outdated prolong. Delay {0} ms", delta.Duration().TotalMilliseconds);
                    else
                        Thread.Sleep((int)delta.TotalMilliseconds);

                    if (TryProlongSingleLock(lockToProlong.LockId, lockToProlong.ProcessId))
                    {
                        locksToProlong.Enqueue(new EnqueuedLockToProlong(lockToProlong.LockId, lockToProlong.ProcessId, DateTime.Now.Add(prolongInterval)));
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Exception while prolonging:\n{0}", e);
                }
            }
        }

        private bool TryProlongSingleLock(string lockId, string processId)
        {
            var rowSet = CasRemoteLocker.Execute(session, tryProlongStatement.Bind(new {Owner = processId, LockId = lockId}));
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