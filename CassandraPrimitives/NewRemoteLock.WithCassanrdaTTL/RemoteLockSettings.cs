namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithCassanrdaTTL
{
    public class RemoteLockSettings
    {
        public RemoteLockSettings(string keyspace, string columnFamily, int maxRowLength = 1000, int lockTTL = 15000, int extendRentPeriod = 5000, bool useLocalOptimization = true)
        {
            KeyspaceName = keyspace;
            ColumnFamilyName = columnFamily;
            MaxRowLength = maxRowLength;
            ExtendRentPeriod = extendRentPeriod;
            LockTTL = lockTTL;
            UseLocalOptimization = useLocalOptimization;
        }

        public string KeyspaceName { get; private set; }
        public string ColumnFamilyName { get; private set; }
        public int MaxRowLength { get; private set; }
        public int ExtendRentPeriod { get; private set; }
        public int LockTTL { get; private set; }
        public bool UseLocalOptimization { get; private set; }
    }
}