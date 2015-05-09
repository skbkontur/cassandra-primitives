using System.Threading;

using GroboTrace;

using SKBKontur.Catalogue.CassandraPrimitives.ExpirationServiceCore.Scheduler;
using SKBKontur.Catalogue.CassandraPrimitives.ExpirationServiceCore.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.ExpirationMonitoringStorage;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;
using SKBKontur.Catalogue.ServiceLib;

namespace SKBKontur.Catalogue.CassandraPrimitives.ExpirationService
{
    public class EntryPoint : ApplicationBase
    {
        protected override void ConfigureTracingWrapper(TracingWrapperConfigurator configurator)
        {
        }

        protected override string ConfigFileName { get { return "expirationServiceSettings"; } }

        private static void Main(string[] args)
        {
            new EntryPoint().Run();
        }

        private void Run()
        {
            var settings = Container.Get<IExpirationServiceSettings>();
            var storageFactory = Container.Get<IExpirationMonitoringStorageFactory>();
            Container.Configurator.ForAbstraction<IExpirationMonitoringStorage>().UseInstances(storageFactory.CreateStorage(new ColumnFamilyFullName(settings.Keyspace, settings.ColumnFamily)));
            Container.Get<IExpirationServiceSchedulableRunner>().Start();
            Thread.Sleep(-1);
        }
    }
}