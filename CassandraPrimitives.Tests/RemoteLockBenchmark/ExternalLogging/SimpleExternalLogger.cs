using System.IO;

using Newtonsoft.Json;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.TestResults;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging
{
    public class SimpleExternalLogger : IExternalProgressLogger<SimpleTestResult>
    {
        public SimpleExternalLogger(TextWriter logStream)
        {
            this.logStream = logStream;
        }

        public void Log(string message)
        {
        }

        public void Log(string format, params object[] items)
        {
        }

        public void PublishResult(SimpleTestResult testResult)
        {
            logStream.Write(JsonConvert.SerializeObject(testResult));
        }

        private readonly TextWriter logStream;
    }
}