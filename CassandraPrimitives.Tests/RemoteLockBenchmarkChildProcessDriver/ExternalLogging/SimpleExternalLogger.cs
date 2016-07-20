using System.IO;

using Newtonsoft.Json;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.ExternalLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkChildProcessDriver.ExternalLogging
{
    public class SimpleExternalLogger : IExternalProgressLogger<SimpleProgressMessage>
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
        
        public void PublishProgress(SimpleProgressMessage progressMessage)
        {
            logStream.WriteLine(JsonConvert.SerializeObject(progressMessage));
        }

        private readonly TextWriter logStream;
    }
}