namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.ZooKeeper.ZookeeperSettings
{
    public class ZookeeperClusterSettings
    {
        public ZookeeperClusterSettings(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public string ConnectionString { get; private set; }
    }
}