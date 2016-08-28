using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Cassandra;

using log4net;

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
        private readonly ILog logger;

        public LeaseProlonger(ISession session, TimeSpan prolongInterval, PreparedStatement tryProlongStatement)
        {
            logger = LogManager.GetLogger(typeof(LeaseProlonger));
            this.tryProlongStatement = tryProlongStatement;
            locksToProlong = new ConcurrentQueue<EnqueuedLockToProlong>();
            this.session = session;
            this.prolongInterval = TimeSpan.FromMilliseconds(Math.Max(100, prolongInterval.TotalMilliseconds - 2000));
            Task.Run(() => InfinetelyProlongLocks());
        }

        public void AddLock(string lockId, string processId)
        {
            StartProlongAndEnqueing(lockId, processId);
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
                    if(delta.TotalMilliseconds < 0)
                    {
                        logger.WarnFormat("Performing outdated prolong. Delay {0} ms", delta.Duration().TotalMilliseconds);
                        Console.WriteLine("Performing outdated prolong. Delay {0} ms", delta.Duration().TotalMilliseconds);
                    }
                    else
                    {
                        var sleepInterval = Math.Max(0, Math.Min((int)delta.TotalMilliseconds - 10, (int)prolongInterval.TotalMilliseconds / Math.Max(1, locksToProlong.Count)));
                        if(sleepInterval > 1)
                        {
                            Thread.Sleep(sleepInterval);
                        }
                    }

                    StartProlongAndEnqueing(lockToProlong.LockId, lockToProlong.ProcessId);
                }
                catch (Exception e)
                {
                    logger.ErrorFormat("Exception while prolonging:\n{0}", e);
                    Console.WriteLine("Exception while prolonging:\n{0}", e);
                }
            }
        }

        private void StartProlongAndEnqueing(string lockId, string processId)
        {
            var attempts = 5;
            var timeout = (int)prolongInterval.TotalMilliseconds;

            Task.Run(async () =>
                {
                    try
                    {
                        var nextProlong = DateTime.Now.Add(prolongInterval);
                        var runningTries = new List<Task<bool>>();

                        for(int i = 0; i < attempts; i++)
                        {
                            var prolongTask = TryProlongSingleLockAsync(lockId, processId);
                            runningTries.Add(prolongTask);

                            var delayTask = Task.Delay(timeout / attempts);
                            var task = await Task.WhenAny(runningTries.Concat(new[] {delayTask}));
                            if(task != delayTask)
                            {
                                if(await await Task.WhenAny(runningTries))
                                {
                                    locksToProlong.Enqueue(new EnqueuedLockToProlong(lockId, processId, nextProlong));
                                }
                                else
                                {
                                    logger.WarnFormat("Can't prolong lock {0} because process {1} doesn't own it", lockId, processId);
                                    Console.WriteLine("Can't prolong lock {0} because process {1} doesn't own it", lockId, processId);
                                }
                                return;
                            }
                        }
                        logger.WarnFormat("Can't prolong lock after {0} RoundRobin attempts with timeout {1}", attempts, timeout);
                        Console.WriteLine("Can't prolong lock after {0} RoundRobin attempts with timeout {1}", attempts, timeout);
                    }
                    catch(Exception e)
                    {
                        logger.ErrorFormat("Exception while prolonging:\n{0}", e);
                        Console.WriteLine("Exception while prolonging:\n{0}", e);
                    }
                });
        }

        private async Task<bool> TryProlongSingleLockAsync(string lockId, string processId)
        {
            var rowSet = await CasRemoteLocker.ExecuteAsync(session, tryProlongStatement.Bind(new { Owner = processId, LockId = lockId }));
            var applied = rowSet.Single().GetValue<bool>("[applied]");
            return applied;
        }

        private bool TryProlongSingleLock(string lockId, string processId)
        {
            var rowSet = CasRemoteLocker.Execute(session, tryProlongStatement.Bind(new {Owner = processId, LockId = lockId}));
            var applied = rowSet.Single().GetValue<bool>("[applied]");
            return applied;
        }
        
        public void Dispose()
        {
            stopped = true;
        }
    }
}