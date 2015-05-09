using System;

using log4net;

using SKBKontur.Catalogue.CassandraPrimitives.TimeServiceCore.Settings;
using SKBKontur.Catalogue.ServiceLib.Scheduling;

namespace SKBKontur.Catalogue.CassandraPrimitives.TimeServiceCore.Scheduler
{
    public class TimeServiceSchedulableRunner : ITimeServiceSchedulableRunner
    {
        public TimeServiceSchedulableRunner(
            IPeriodicTaskRunner periodicTaskRunner,
            ITimeServiceActualizeTask timeServiceActualizeTask,
            ITimeServiceSettings timeServiceSettings)
        {
            this.periodicTaskRunner = periodicTaskRunner;
            this.timeServiceActualizeTask = timeServiceActualizeTask;
            this.timeServiceSettings = timeServiceSettings;
        }

        public void Start()
        {
            try
            {
                periodicTaskRunner.Register(timeServiceActualizeTask, timeServiceSettings.ActualizeInterval);
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
                periodicTaskRunner.Unregister(timeServiceActualizeTask.Id, 15000);
            }
            catch(Exception e)
            {
                logger.Error("Stoping periodic tasks unsuccess", e);
            }
        }

        private readonly ILog logger = LogManager.GetLogger(typeof(TimeServiceSchedulableRunner));
        private readonly IPeriodicTaskRunner periodicTaskRunner;
        private readonly ITimeServiceActualizeTask timeServiceActualizeTask;
        private readonly ITimeServiceSettings timeServiceSettings;
    }
}