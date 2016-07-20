using System;

using log4net;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkChildProcessDriver.ExternalLogging.Http;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkChildProcessDriver
{
    internal class Program
    {
        private static ILog logger;

        private static void Main(string[] args)
        {
            Log4NetConfiguration.InitializeOnce();
            logger = LogManager.GetLogger(typeof(Program));

            AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;
            int processInd;
            if (!int.TryParse(args[0], out processInd))
            {
                Console.WriteLine("Invalid argument");
                logger.ErrorFormat("Invalid process id {0}", args[0]);
            }

            logger.InfoFormat("Process id is {0}", processInd);
            logger.InfoFormat("Remote http address is {0}", args[1]);

            TestConfiguration configuration;
            using (var httpExternalDataProvider = new HttpExternalDataGetter(args[1]))
                configuration = httpExternalDataProvider.GetTestConfiguration().Result;

            logger.InfoFormat("Configuration was received");
            
            ChildProcessDriver.RunSingleTest(processInd, configuration, AppDomain.CurrentDomain.BaseDirectory);
        }

        private static void OnUnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            logger.Error(e.ExceptionObject.ToString());
        }
    }
}