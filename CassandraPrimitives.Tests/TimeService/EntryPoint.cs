using GroboTrace;

using SKBKontur.Catalogue.CassandraPrimitives.TimeServiceCore.Implementation;
using SKBKontur.Catalogue.CassandraPrimitives.TimeServiceCore.Scheduler;
using SKBKontur.Catalogue.ServiceLib;
using SKBKontur.Catalogue.ServiceLib.Services;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.TimeService
{
    public class EntryPoint : ApplicationBase
    {
        protected override void ConfigureTracingWrapper(TracingWrapperConfigurator configurator)
        {
        }

        protected override string ConfigFileName { get { return "timeServiceSettings"; } }

        private static void Main()
        {
            new EntryPoint().Run();
        }

        private void Run()
        {
            Container.Get<ITimeServiceImpl>().UpdateTime();
            Container.Get<ITimeServiceSchedulableRunner>().Start();
            Container.Get<HttpService>().Run();
        }
    }
}