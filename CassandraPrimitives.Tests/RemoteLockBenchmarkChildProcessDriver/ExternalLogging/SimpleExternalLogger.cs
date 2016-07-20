using System.IO;

using Newtonsoft.Json;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkChildProcessDriver.ExternalLogging
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