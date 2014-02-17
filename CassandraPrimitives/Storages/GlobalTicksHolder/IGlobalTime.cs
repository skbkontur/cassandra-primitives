namespace SKBKontur.Catalogue.CassandraPrimitives.Storages.GlobalTicksHolder
{
    public interface IGlobalTime
    {
        long UpdateNowTicks();
        long GetNowTicks();
    }
}