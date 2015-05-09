using System;

using log4net;

using SKBKontur.Catalogue.CassandraPrimitives.ExpirationServiceCore.Settings;
using SKBKontur.Catalogue.ServiceLib.Scheduling;

namespace SKBKontur.Catalogue.CassandraPrimitives.ExpirationServiceCore.Scheduler
{
    public class ExpirationServiceSchedulableRunner : IExpirationServiceSchedulableRunner
    {
        public ExpirationServiceSchedulableRunner(IExpiryLogReaderTask expiryLogReaderTask, IExpiryCheckerTask expiryCheckerTask, IPeriodicTaskRunner periodicTaskRunner, IExpirationServiceSettings expirationServiceSettings)
        {
            this.expiryLogReaderTask = expiryLogReaderTask;
            this.expiryCheckerTask = expiryCheckerTask;
            this.periodicTaskRunner = periodicTaskRunner;
            this.expirationServiceSettings = expirationServiceSettings;
        }

        public void Start()
        {
            try
            {
                periodicTaskRunner.Register(expiryLogReaderTask, expirationServiceSettings.ReadPeriod);
            }
            catch(Exception e)
            {
                logger.Error("Starting periodic tasks unsuccess", e);
            }
            try
            {
                periodicTaskRunner.Register(expiryCheckerTask, expirationServiceSettings.ExpiryPeriod);
            }
            catch(Exception e)
            {
                logger.Error("Starting periodic tasks unsuccess", e);
            }
        }

        public void Stop()
        {
            try
            {
                periodicTaskRunner.Unregister(expiryLogReaderTask.Id, 15000);
            }
            catch(Exception e)
            {
                logger.Error("Stoping periodic tasks unsuccess", e);
            }
            try
            {
                periodicTaskRunner.Unregister(expiryCheckerTask.Id, 15000);
            }
            catch(Exception e)
            {
                logger.Error("Stoping periodic tasks unsuccess", e);
            }
        }

        private readonly ILog logger = LogManager.GetLogger(typeof(ExpirationServiceSchedulableRunner));
        private readonly IExpiryLogReaderTask expiryLogReaderTask;
        private readonly IExpiryCheckerTask expiryCheckerTask;
        private readonly IPeriodicTaskRunner periodicTaskRunner;
        private readonly IExpirationServiceSettings expirationServiceSettings;
    }
}