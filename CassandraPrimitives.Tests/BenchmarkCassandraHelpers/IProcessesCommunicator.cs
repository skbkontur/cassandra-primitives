using SKBKontur.Cassandra.CassandraClient.Clusters;

namespace BenchmarkCassandraHelpers
{
    public interface IProcessesCommunicator
    {
        void AddRunningProcess(string processId);
        void RemoveAllRunningProcesses();
        string[] GetAllRunningProcesses();

        void StartExecuting(string lockId, string processId);
        void StopExecuting(string lockId, string processId);
        int GetExecutingProcessesCount(string lockId);
        void WaitAllExecutingProcesses(string lockId);

        void WriteResults(string lockId, string processId, double[] times);
        double[] GetResults(string lockId, string processId);

        void SetLockId(string expId, int index, string id);
        string GetLockId(string expId, int index);
        void MarkLockIdAsDone(string lockId, string processId);
        void WaitLockIdDone(string lockId, int processesCount);

        StartSignal WaitStartSignal(string processId, string lastLockId);
        void SendStartSignal(StartSignal signal);
        void RemoveStartSignal();
        ICassandraCluster GetCassandraCluster();
    }
}