using System;
using System.Threading;

using log4net;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ExternalLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.ProgressMessages;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.Tests;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestRunning
{
    public class TestRunner<TProgressMessage> : IDisposable
        where TProgressMessage : IProgressMessage
    {
        public TestRunner(TestConfiguration configuration, IExternalLogger externalLogger)
        {
            this.configuration = configuration;
            this.externalLogger = externalLogger;
            logger = LogManager.GetLogger(GetType());
        }

        public void RunTestAndPublishResults(ITest<TProgressMessage> test)
        {
            test.SetUp();
            var threads = new Thread[configuration.AmountOfThreads];
            for (var i = 0; i < configuration.AmountOfThreads; i++)
            {
                var threadInd = i;
                threads[i] = new Thread(() =>
                    {
                        try
                        {
                            externalLogger.Log("Start working in thread {0}", threadInd);
                            test.DoWorkInSingleThread(threadInd);
                        }
                        catch (Exception e)
                        {
                            LogException("Exception occured in one of test threads", e);
                        }
                    });
            }

            try
            {
                foreach (var thread in threads)
                    thread.Start();
                foreach (var thread in threads)
                    thread.Join(); //TODO timeout?
            }
            catch (Exception e)
            {
                LogException("Exception occured while perfoming test", e);
            }

            test.TearDown();
        }

        private void LogException(string description, Exception exception)
        {
            logger.Error(string.Format("{0}:", description), exception);
            externalLogger.Log("{0}:\n{1}", description, exception);
        }

        public void Dispose()
        {
        }

        private readonly TestConfiguration configuration;
        private readonly IExternalLogger externalLogger;
        private readonly ILog logger;
    }
}