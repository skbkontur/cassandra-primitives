namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.CassandraInitialisation
{
    public class CassandraNodeSettings
    {
        public CassandraNodeSettings(
            string name = null,
            int jmxPort = 7399,
            int gossipPort = 7400,
            int rpcPort = 9360,
            int cqlPort = 9343,
            string dataBaseDirectory = null,
            string listenAddress = null,
            string rpsAddress = null,
            string[] seedAddresses = null,
            string initialToken = null,
            string clusterName = null)
        {
            Name = name ?? "node_at_9360";
            JmxPort = jmxPort;
            GossipPort = gossipPort;
            RpcPort = rpcPort;
            CqlPort = cqlPort;
            DataBaseDirectory = dataBaseDirectory ?? @"../data/";
            ListenAddress = listenAddress ?? "127.0.0.1";
            RpsAddress = rpsAddress ?? "0.0.0.0";
            SeedAddresses = seedAddresses ?? new[] {"127.0.0.1"};
            InitialToken = initialToken ?? "";
            ClusterName = clusterName ?? "TestCluster";
        }

        public string Name { get; private set; }
        public int JmxPort { get; private set; }
        public int GossipPort { get; private set; }
        public int RpcPort { get; private set; }
        public int CqlPort { get; private set; }
        public string DataBaseDirectory { get; private set; }
        public string ListenAddress { get; private set; }
        public string RpsAddress { get; private set; }
        public string[] SeedAddresses { get; private set; }
        public string InitialToken { get; private set; }
        public string ClusterName { get; private set; }
    }
}