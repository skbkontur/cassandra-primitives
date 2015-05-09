using GroboTrace;

using SKBKontur.Catalogue.ServiceLib;

namespace SKBKontur.Catalogue.CassandraPrimitives.TimeService
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
        }
    }
}