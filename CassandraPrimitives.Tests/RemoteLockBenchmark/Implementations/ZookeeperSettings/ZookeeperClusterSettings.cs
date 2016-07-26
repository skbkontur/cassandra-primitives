namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.ZookeeperSettings
{
    public class ZookeeperClusterSettings
    {
        public ZookeeperClusterSettings(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public string ConnectionString { get; set; }
    }
}