namespace SKBKontur.Catalogue.CassandraPrimitives.TimeServiceClient
{
    public interface ITimeServiceClient
    {
        void ForceUpdate();
        long GetNowTicks();
    }
}