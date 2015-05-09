using SKBKontur.Catalogue.CassandraPrimitives.ExpirationServiceCore.Implementation.ExpiryChecker;
using SKBKontur.Catalogue.CassandraPrimitives.ExpirationServiceCore.Implementation.LogReader;

namespace SKBKontur.Catalogue.CassandraPrimitives.ExpirationServiceCore.Scheduler
{
    public class ExpiryLogReaderTask : IExpiryLogReaderTask
    {
        public ExpiryLogReaderTask(IExpiringObjectLogReader expiringObjectLogReader, IExpiryChecker expiryChecker)
        {
            this.expiringObjectLogReader = expiringObjectLogReader;
            this.expiryChecker = expiryChecker;
        }

        public void Run()
        {
            var newMetas = expiringObjectLogReader.GetNewMetas();
            expiryChecker.AddNewEntries(newMetas);
        }

        public string Id { get { return "ExpiryLogReaderTask"; } }
        private readonly IExpiringObjectLogReader expiringObjectLogReader;
        private readonly IExpiryChecker expiryChecker;
    }
}