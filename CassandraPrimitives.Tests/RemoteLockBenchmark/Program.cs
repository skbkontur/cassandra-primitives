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
                    amountOfThreads = 10,
                    amountOfProcesses = 5,
                    amountOfLocksPerThread = 200,
                    minWaitTimeMilliseconds = 100,
                    maxWaitTimeMilliseconds = 200,
                    remoteHostName = IPGlobalProperties.GetIPGlobalProperties().HostName + "." + IPGlobalProperties.GetIPGlobalProperties().DomainName,
                    httpPort = 12345,
                    remoteLockImplementation = TestConfiguration.RemoteLockImplementation.Zookeeper
                };
            var driver = new MainDriver(new TeamCityLogger(Console.Out), configuration, true);
            driver.Run();
        }

        private static void OnUnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            Console.WriteLine(e.ExceptionObject.ToString());
            Environment.Exit(1);
        }
    }
}