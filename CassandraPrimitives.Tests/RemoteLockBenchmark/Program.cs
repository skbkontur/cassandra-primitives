using System;
using System.Linq;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ChildProcessDriver;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Registry;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestOptions;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.SeriesOfLocks;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.Timeline;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.WaitForLock;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class Program
    {
        public static IScenariosRegistry CreateRegistry()
        {
            var scenariosRegistry = new ScenariosRegistry();

            scenariosRegistry.Register<TimelineProgressMessage, TimelineTest, TimelineTestProgressProcessor, TimelineTestOptions>(
                TestScenarios.Timeline.ToString(),
                options =>
                    {
                        var remoteLockGetterProvider = new RemoteLockGetterProvider(options.ExternalDataGetter, options.Configuration, options.ExternalProgressLogger);
                        return new TimelineTest(remoteLockGetterProvider, options.ExternalProgressLogger, options.ExternalDataGetter, options.ProcessInd);
                    },
                options => new TimelineTestProgressProcessor(options.Configuration, options.TestOptions as TimelineTestOptions, options.TeamCityLogger, options.MetricsContext),
                testOptionsProvider =>
                    {
                        return testOptionsProvider
                            .CreateOptions<TimelineTestOptions>()
                            .Select(opt =>
                                {
                                    opt.LockId = Guid.NewGuid().ToString();
                                    return opt;
                                })
                            .Cast<ITestOptions>()
                            .ToList();
                    });

            scenariosRegistry.Register<WaitForLockProgressMessage, WaitForLockTest, WaitForLockTestProgressProcessor, WaitForLockTestOptions>(
                TestScenarios.WaitForLock.ToString(),
                options =>
                    {
                        var remoteLockGetterProvider = new RemoteLockGetterProvider(options.ExternalDataGetter, options.Configuration, options.ExternalProgressLogger);
                        return new WaitForLockTest(remoteLockGetterProvider, options.ExternalProgressLogger, options.ExternalDataGetter);
                    },
                options => new WaitForLockTestProgressProcessor(options.Configuration, options.TestOptions as WaitForLockTestOptions, options.TeamCityLogger, options.MetricsContext),
                testOptionsProvider =>
                    {
                        return testOptionsProvider
                            .CreateOptions<WaitForLockTestOptions>()
                            .Select(opt =>
                                {
                                    opt.LockId = Guid.NewGuid().ToString();
                                    return opt;
                                })
                            .Cast<ITestOptions>()
                            .ToList();
                    });

            scenariosRegistry.Register<SeriesOfLocksProgressMessage, SeriesOfLocksTest, SeriesOfLocksTestProgressProcessor, SeriesOfLocksTestOptions>(
                TestScenarios.SeriesOfLocks.ToString(),
                options =>
                    {
                        var remoteLockGetterProvider = new RemoteLockGetterProvider(options.ExternalDataGetter, options.Configuration, options.ExternalProgressLogger);
                        return new SeriesOfLocksTest(remoteLockGetterProvider, options.ExternalProgressLogger, options.ExternalDataGetter);
                    },
                options => new SeriesOfLocksTestProgressProcessor(options.Configuration, options.TestOptions as SeriesOfLocksTestOptions, options.TeamCityLogger, options.MetricsContext),
                testOptionsProvider =>
                    {
                        return testOptionsProvider
                            .CreateOptions<SeriesOfLocksTestOptions>()
                            .Select(opt =>
                                {
                                    opt.LockIdCommonPrefix = Guid.NewGuid().ToString();
                                    return opt;
                                })
                            .Cast<ITestOptions>()
                            .ToList();
                    });

            return scenariosRegistry;
        }
        
        private static void Main(string[] args)
        {
            new TeamCityRemoteLockBenchmarkConfigurator(CreateRegistry).Run();
        }
    }
}