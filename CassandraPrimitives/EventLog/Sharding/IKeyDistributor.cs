namespace SkbKontur.Cassandra.Primitives.EventLog.Sharding
{
    public interface IKeyDistributor
    {
        int Distribute(string key);
        int ShardsCount { get; }
    }
}