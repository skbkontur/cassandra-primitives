using System;

using BenchmarkCassandraHelpers;

using GroBuf;
using GroBuf.DataMembersExtracters;

namespace LockProcess
{
    public class LockProcessExe
    {
        public static void Main(string[] args)
        {
            new LockProcessExe().Run();
        }

        private void Run()
        {
            IProcessesCommunicator communicator = new ProcessesCommunicator(new Serializer(new AllPropertiesExtractor()));
            var processId = Guid.NewGuid().ToString();
            string lastLockId = null;
            communicator.AddRunningProcess(processId);
            while(true)
            {
                var signal = communicator.WaitStartSignal(processId, lastLockId);
                var lockId = signal.LockId;
                Console.WriteLine("Got signal");
                communicator.StartExecuting(lockId, processId);
                communicator.WriteResults(lockId, processId, new double[] {1, 2, 3});
                communicator.StopExecuting(lockId, processId);
                lastLockId = lockId;
                Console.WriteLine("Success");
            }
        }
    }
}