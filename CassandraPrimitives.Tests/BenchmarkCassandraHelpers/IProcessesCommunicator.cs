using SKBKontur.Cassandra.CassandraClient.Clusters;

namespace BenchmarkCassandraHelpers
{
    public interface IProcessesCommunicator
    {
        void AddRunningProcess(string processId);
        void RemoveRunningProcess(string processId);
        int GetRunningProcessesCount();

        void WriteResults(string keyspace, string columnFamily, string processId, double[] times);
        double[] GetResults(string keyspace, string columnFamily, string processId);

        void WaitStartSignal(string keyspace, string columnFamily);
        void SendStartSignal(string keyspace, string columnFamily);
        ICassandraCluster GetCassandraCluster();
    }
}