using System;
using System.IO;

using Newtonsoft.Json;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging
{
    public class SimpleExternalLogProcessor : IExternalLogProcessor<SimpleTestResult>
    {
        public SimpleExternalLogProcessor(TextReader logReader)
        {
            this.logReader = logReader;
        }

        public SimpleTestResult GetTestResult()
        {
            var data = logReader.ReadToEnd();
            return JsonConvert.DeserializeObject<SimpleTestResult>(data);
        }

        public void StartProcessingLog()
        {
        }

        private readonly TextReader logReader;
    }
}