using System;
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
            IProcessesCommunicator communicator = new ProcessesCommunicator(new Serializer(new AllPropertiesExtractor()));
            var allProcesses = communicator.GetAllRunningProcesses();
            foreach(var process in allProcesses)
                Console.WriteLine(process);
            var lockId = Guid.NewGuid().ToString();
            communicator.SendStartSignal(new StartSignal
            {
                LockId = lockId,
                ProcessIds = allProcesses,
            });
            while(communicator.GetExecutingProcessesCount(lockId) != allProcesses.Length)
            {
                Console.WriteLine("Sleeping");
                Thread.Sleep(500);
            }
            communicator.WaitAllExecutingProcesses(lockId);
            communicator.RemoveStartSignal();
            foreach(var processId in allProcesses)
            {
                var results = communicator.GetResults(lockId, processId);
                foreach(var result in results)
                    Console.WriteLine(result);
            }
        }
    }
}