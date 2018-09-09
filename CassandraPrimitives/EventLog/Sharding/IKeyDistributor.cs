namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Sharding
{
    public interface IKeyDistributor
    {
        int Distribute(string key);
        int ShardsCount { get; }
    }
}