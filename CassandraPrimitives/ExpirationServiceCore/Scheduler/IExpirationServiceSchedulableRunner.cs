namespace SKBKontur.Catalogue.CassandraPrimitives.ExpirationServiceCore.Scheduler
{
    public interface IExpirationServiceSchedulableRunner
    {
        void Start();
        void Stop();
    }
}