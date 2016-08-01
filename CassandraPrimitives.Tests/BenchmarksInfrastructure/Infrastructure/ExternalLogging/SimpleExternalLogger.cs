using System.IO;

using Newtonsoft.Json;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.ProgressMessages;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ExternalLogging
{
    public class SimpleExternalLogger : IExternalProgressLogger
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

        public void PublishProgress<TProgressMessage>(TProgressMessage progressMessage)
            where TProgressMessage : IProgressMessage
        {
            logStream.WriteLine(JsonConvert.SerializeObject(progressMessage));
        }

        private readonly TextWriter logStream;
    }
}