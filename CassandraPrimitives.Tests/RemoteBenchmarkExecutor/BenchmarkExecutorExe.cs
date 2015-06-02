using System;
using System.IO;
using System.Linq;
using System.Threading;

using BenchmarkCassandraHelpers;

using GroBuf;
using GroBuf.DataMembersExtracters;

namespace RemoteBenchmarkExecutor
{
    public class BenchmarkExecutorExe
    {
        public static void Main(string[] args)
        {
            new BenchmarkExecutorExe().Run();
        }

        private void Run()
        {
            var benchmarks = new[]
            {
                new BenchmarkSettings
                {
                    LocksCount = 5,
                    ProcessesCount = 3,
                    LockType = LockType.OldLock,
                },
                new BenchmarkSettings
                {
                    LocksCount = 5,
                    ProcessesCount = 3,
                    LockType = LockType.NewLockCassandraTTL,
                },
            };
            foreach (var benchmark in benchmarks)
            {
                Run(benchmark);
            }
        }

        private string[] TakeRandom(string[] source, int count)
        {
            var random = new Random();
            var n = source.Length;
            if(count > n)
                return null;
            for(var i = 0; i < n; i++)
            {
                var to = random.Next(n);
                var tmp = source[to];
                source[to] = source[i];
                source[i] = tmp;
            }
            return source.Take(count).ToArray();
        }

        private void Run(BenchmarkSettings settings)
        {
            IProcessesCommunicator communicator = new ProcessesCommunicator(new Serializer(new AllPropertiesExtractor()));
            var allProcesses = communicator.GetAllRunningProcesses();
            foreach(var process in allProcesses)
                Console.WriteLine(process);
            var lockId = Guid.NewGuid().ToString();
            var activeProcessesIds = TakeRandom(allProcesses, settings.ProcessesCount);
            if(activeProcessesIds == null)
            {
                Console.WriteLine("We need more running processes");
                return;
            }
            communicator.SendStartSignal(new StartSignal
            {
                LockId = lockId,
                ProcessIds = activeProcessesIds,
                LocksCount = settings.LocksCount,
                LockType = settings.LockType,
            });
            while(communicator.GetExecutingProcessesCount(lockId) != activeProcessesIds.Length)
            {
                Console.WriteLine("Sleeping");
                Thread.Sleep(500);
            }
            communicator.WaitAllExecutingProcesses(lockId);
            communicator.RemoveStartSignal();
            var results = new double[settings.ProcessesCount][];
            for(var i = 0; i < settings.ProcessesCount; i++)
            {
                results[i] = communicator.GetResults(lockId, activeProcessesIds[i]);
            }
            var filename = string.Format("Proc_{0}_Locks_{1}_Type_{2}.txt", settings.ProcessesCount, settings.LocksCount, settings.LockType);
            var lines = Enumerable.Range(0, settings.LocksCount).Select(lockIndex => string.Join("\t", Enumerable.Range(0, settings.ProcessesCount).Select(processIndex => results[processIndex][lockIndex]))).ToArray();
            File.WriteAllLines(filename, lines);
        }

        private class BenchmarkSettings
        {
            public int ProcessesCount { get; set; }
            public int LocksCount { get; set; }
            public LockType LockType { get; set; }
        }
    }
}