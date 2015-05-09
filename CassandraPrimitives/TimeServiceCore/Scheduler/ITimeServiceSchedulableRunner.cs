namespace SKBKontur.Catalogue.CassandraPrimitives.TimeServiceCore.Scheduler
{
    public interface ITimeServiceSchedulableRunner
    {
        void Start();
        void Stop();
    }
}