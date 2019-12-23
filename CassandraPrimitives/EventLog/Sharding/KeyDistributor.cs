namespace SkbKontur.Cassandra.Primitives.EventLog.Sharding
{
    public class KeyDistributor : IKeyDistributor
    {
        public KeyDistributor(int shardsCount)
        {
            this.ShardsCount = shardsCount;
        }

        public int Distribute(string key)
        {
            unchecked
            {
                return (int)((uint)((key ?? "").GetHashCode()) % (uint)ShardsCount);
            }
        }

        public int ShardsCount { get; }

        public static KeyDistributor Create(int shardsCount)
        {
            return new KeyDistributor(shardsCount);
        }
    }
}