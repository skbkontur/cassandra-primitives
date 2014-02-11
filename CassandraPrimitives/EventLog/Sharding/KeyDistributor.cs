namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Sharding
{
    public class KeyDistributor : IKeyDistributor
    {
        public KeyDistributor(int shardsCount)
        {
            this.shardsCount = shardsCount;
        }

        public int Distribute(string key)
        {
            unchecked
            {
                return (int)((uint)((key ?? "").GetHashCode()) % (uint)ShardsCount);
            }
        }

        public int ShardsCount { get { return shardsCount; } }

        public static KeyDistributor Create(int shardsCount)
        {
            return new KeyDistributor(shardsCount);
        }

        private readonly int shardsCount;
    }
}