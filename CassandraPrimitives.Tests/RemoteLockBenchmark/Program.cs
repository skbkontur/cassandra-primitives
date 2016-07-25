using System;
using System.Net.NetworkInformation;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class Program
    {
        private static void Main(string[] args)
        {
            Log4NetConfiguration.InitializeOnce();
            AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;
            var configuration = new TestConfiguration
                {
                    amountOfThreads = 4,
                    amountOfProcesses = 3,
                    amountOfLocksPerThread = 10,
                    minWaitTimeMilliseconds = 10,
                    maxWaitTimeMilliseconds = 20,
                    remoteHostName = IPGlobalProperties.GetIPGlobalProperties().HostName + "." + IPGlobalProperties.GetIPGlobalProperties().DomainName,
                    httpPort = 12345,
                    remoteLockImplementation = TestConfiguration.RemoteLockImplementation.Zookeeper
                };
            var driver = new MainDriver(new TeamCityLogger(Console.Out), configuration, false);
            driver.Run();
        }

        private static void OnUnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            Console.WriteLine(e.ExceptionObject.ToString());
            Environment.Exit(1);
        }
    }
}