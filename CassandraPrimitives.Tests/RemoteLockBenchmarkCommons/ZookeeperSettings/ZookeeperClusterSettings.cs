namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.ZookeeperSettings
{
    public class ZookeeperClusterSettings
    {
        public string ConnectionString { get; set; }

        public ZookeeperClusterSettings(string connectionString)
        {
            ConnectionString = connectionString;
        }
    }
}