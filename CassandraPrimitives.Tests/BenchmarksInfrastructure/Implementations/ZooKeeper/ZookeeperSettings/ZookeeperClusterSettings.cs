namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Implementations.ZooKeeper.ZookeeperSettings
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