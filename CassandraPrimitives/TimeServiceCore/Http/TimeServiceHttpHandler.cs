using SKBKontur.Catalogue.CassandraPrimitives.TimeServiceCore.Implementation;
using SKBKontur.Catalogue.CassandraPrimitives.TimeServiceCore.Settings;
using SKBKontur.Catalogue.ServiceLib.HttpHandlers;

namespace SKBKontur.Catalogue.CassandraPrimitives.TimeServiceCore.Http
{
    public class TimeServiceHttpHandler : IHttpHandler
    {
        public TimeServiceHttpHandler(ITimeServiceImpl timeServiceImpl, ITimeServiceSettings timeServiceSettings)
        {
            this.timeServiceImpl = timeServiceImpl;
            this.timeServiceSettings = timeServiceSettings;
        }

        [HttpMethod]
        public void ForceUpdate()
        {
            timeServiceImpl.UpdateTime();
        }

        [HttpMethod]
        public long GetNowTicks()
        {
            if(timeServiceSettings.NeedActualizeBeforeRequest)
                timeServiceImpl.UpdateTime();
            return timeServiceImpl.GetNowTicks();
        }

        private readonly ITimeServiceImpl timeServiceImpl;
        private readonly ITimeServiceSettings timeServiceSettings;
    }
}