using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ChildProcessDriver;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Registry;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.SeriesOfLocks;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Timeline;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.WaitForLock;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    class Program
    {
        public static IScenariosRegistry CreateRegistry()
        {
            var scenariosRegistry = new ScenariosRegistry();

            scenariosRegistry.Register<TimelineProgressMessage, TimelineTest, TimelineTestProgressProcessor>(
                TestScenarios.Timeline,
                options =>
                {
                    var remoteLockGetterProvider = new RemoteLockGetterProvider(options.ExternalDataGetter, options.Configuration, options.ExternalProgressLogger);
                    return new TimelineTest(options.Configuration, remoteLockGetterProvider, options.ExternalProgressLogger, options.ExternalDataGetter, options.ProcessInd);
                },
                options => new TimelineTestProgressProcessor(options.Configuration, options.TeamCityLogger, options.MetricsContext));

            scenariosRegistry.Register<WaitForLockProgressMessage, WaitForLockTest, WaitForLockTestProgressProcessor>(
                TestScenarios.WaitForLock,
                options =>
                {
                    var remoteLockGetterProvider = new RemoteLockGetterProvider(options.ExternalDataGetter, options.Configuration, options.ExternalProgressLogger);
                    return new WaitForLockTest(options.Configuration, remoteLockGetterProvider, options.ExternalProgressLogger, options.ExternalDataGetter);
                },
                options => new WaitForLockTestProgressProcessor(options.Configuration, options.TeamCityLogger, options.MetricsContext));

            scenariosRegistry.Register<SeriesOfLocksProgressMessage, SeriesOfLocksTest, SeriesOfLocksTestProgressProcessor>(
                TestScenarios.SeriesOfLocks,
                options =>
                {
                    var remoteLockGetterProvider = new RemoteLockGetterProvider(options.ExternalDataGetter, options.Configuration, options.ExternalProgressLogger);
                    return new SeriesOfLocksTest(options.Configuration, remoteLockGetterProvider, options.ExternalProgressLogger, options.ExternalDataGetter);
                },
                options => new SeriesOfLocksTestProgressProcessor(options.Configuration, options.TeamCityLogger, options.MetricsContext));

            return scenariosRegistry;
        }
        static void Main(string[] args)
        {
            var p = new BenchmarksInfrastructure.Program();
            p.Run(args, CreateRegistry);
        }
    }
}
