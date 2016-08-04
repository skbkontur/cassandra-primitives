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
            string listenAddress = null,
            string rpcAddress = null,
            string[] seedAddresses = null,
            string clusterName = null)
        {
            Name = name ?? "node_at_9360";
            JmxPort = jmxPort;
            GossipPort = gossipPort;
            RpcPort = rpcPort;
            CqlPort = cqlPort;
            ListenAddress = listenAddress ?? "127.0.0.1";
            RpcAddress = rpcAddress ?? "127.0.0.1";
            SeedAddresses = seedAddresses ?? new[] { "127.0.0.1" };
            ClusterName = clusterName ?? "TestCluster";
        }

        public string Name { get; private set; }
        public int JmxPort { get; private set; }
        public int GossipPort { get; private set; }
        public int RpcPort { get; private set; }
        public int CqlPort { get; private set; }
        public string ListenAddress { get; private set; }
        public string RpcAddress { get; private set; }
        public string[] SeedAddresses { get; private set; }
        public string ClusterName { get; private set; }
    }
}