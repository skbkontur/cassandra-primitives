namespace SKBKontur.Catalogue.CassandraPrimitives.ExpirationServiceCore.Implementation.ExpirationJudge
{
    public class ExpiringObjectState
    {
        public int Lifetime { get; set; }
        public long? LastTimestamp { get; set; }
        public bool IsExpired { get; set; }
    }
}