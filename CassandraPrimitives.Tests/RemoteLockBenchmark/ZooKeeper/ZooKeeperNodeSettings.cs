namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ZooKeeper
{
    public class ZookeeperNodeSettings
    {
        public ZookeeperNodeSettings(int? tickTime = 2000, int? initLimit = 10, int? syncLimit = 5, string dataDir = null, int? clientPort = 2181, int? maxClientCnxns = null, int? autopurgeSnapRetainCount = null, int? autopurgePurgeInterval = null, string[] serverAddresses = null, int? id = null)
        {
            TickTime = tickTime;
            InitLimit = initLimit;
            SyncLimit = syncLimit;
            DataDir = dataDir ?? @"../data/";
            ClientPort = clientPort;
            MaxClientCnxns = maxClientCnxns;
            AutopurgeSnapRetainCount = autopurgeSnapRetainCount;
            AutopurgePurgeInterval = autopurgePurgeInterval;
            ServerAddresses = serverAddresses;
            Id = id;
        }

        public int? TickTime { get; private set; }
        public int? InitLimit { get; private set; }
        public int? SyncLimit { get; private set; }
        public string DataDir { get; private set; }
        public int? ClientPort { get; private set; }
        public int? MaxClientCnxns { get; private set; }
        public int? AutopurgeSnapRetainCount { get; private set; }
        public int? AutopurgePurgeInterval { get; private set; }
        public string[] ServerAddresses { get; private set; }
        public int? Id { get; private set; }
    }
}