namespace SKBKontur.Catalogue.CassandraPrimitives.TimeServiceCore.Implementation
{
    public interface ITimeServiceImpl
    {
        void UpdateTime();
        long GetNowTicks();
    }
}