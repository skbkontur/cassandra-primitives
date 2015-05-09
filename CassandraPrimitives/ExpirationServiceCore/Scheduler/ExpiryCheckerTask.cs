using SKBKontur.Catalogue.CassandraPrimitives.ExpirationServiceCore.Implementation.ExpiryChecker;

namespace SKBKontur.Catalogue.CassandraPrimitives.ExpirationServiceCore.Scheduler
{
    public class ExpiryCheckerTask : IExpiryCheckerTask
    {
        public ExpiryCheckerTask(IExpiryChecker expiryChecker)
        {
            this.expiryChecker = expiryChecker;
        }

        public void Run()
        {
            expiryChecker.Check();
        }

        public string Id { get { return "ExpiryCheckerTask"; } }
        private readonly IExpiryChecker expiryChecker;
    }
}