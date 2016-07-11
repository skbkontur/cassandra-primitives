using System;
using System.Threading;

using log4net;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.TestConfigurations
{
    public class TestRunner<TTestResult> : IDisposable where TTestResult : ITestResult
    {
        public TestRunner(TestConfiguration configuration, IExternalProgressLogger<TTestResult> externalLogger)
        {
            this.configuration = configuration;
            this.externalLogger = externalLogger;
            logger = LogManager.GetLogger(GetType());
        }

        public void RunTestAndPublishResults(ITest<TTestResult> test)
        {
            test.SetUp();

            var threads = new Thread[configuration.amountOfThreads];
            for (var i = 0; i < configuration.amountOfThreads; i++)
            {
                var threadInd = i;
                threads[i] = new Thread(() =>
                    {
                        try
                        {
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

            var testResult = test.GetTestResult();
            externalLogger.PublishResult(testResult);
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
        private readonly IExternalProgressLogger<TTestResult> externalLogger;
        private readonly ILog logger;
    }
}