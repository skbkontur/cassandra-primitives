using SKBKontur.Catalogue.CassandraPrimitives.TimeServiceCore.Implementation;

namespace SKBKontur.Catalogue.CassandraPrimitives.TimeServiceCore.Scheduler
{
    public class TimeServiceActualizeTask : ITimeServiceActualizeTask
    {
        public TimeServiceActualizeTask(ITimeServiceImpl timeServiceImpl)
        {
            this.timeServiceImpl = timeServiceImpl;
        }

        public void Run()
        {
            timeServiceImpl.UpdateTime();
        }

        public string Id { get { return "TimeServiceActualizeTask"; } }
        private readonly ITimeServiceImpl timeServiceImpl;
    }
}