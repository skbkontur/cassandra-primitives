namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.ZookeeperSettings
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