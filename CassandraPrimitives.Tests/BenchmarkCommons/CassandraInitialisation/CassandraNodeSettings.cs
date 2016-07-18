using System;
using System.IO;

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
            string deployDirectory = null,
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
            DeployDirectory = deployDirectory ?? Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\Cassandra1.2");
            ListenAddress = listenAddress ?? "127.0.0.1";
            RpsAddress = rpsAddress ?? "0.0.0.0";
            SeedAddresses = seedAddresses ?? new[] {"127.0.0.1"};
            InitialToken = initialToken ?? "";
            ClusterName = clusterName ?? "TestCluster";
        }

        public string Name { get; set; }
        public int JmxPort { get; set; }
        public int GossipPort { get; set; }
        public int RpcPort { get; set; }
        public int CqlPort { get; set; }
        public string DataBaseDirectory { get; set; }
        public string DeployDirectory { get; set; }
        public string ListenAddress { get; set; }
        public string RpsAddress { get; set; }
        public string[] SeedAddresses { get; set; }
        public string InitialToken { get; set; }
        public string ClusterName { get; set; }
    }
}