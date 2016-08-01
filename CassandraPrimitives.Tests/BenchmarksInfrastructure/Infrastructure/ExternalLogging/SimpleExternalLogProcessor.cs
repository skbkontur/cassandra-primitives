using System;
using System.IO;

using Newtonsoft.Json;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ExternalLogging
{
    [Obsolete]
    public class SimpleExternalLogProcessor : IExternalLogProcessor
    {
        public SimpleExternalLogProcessor(TextReader logReader)
        {
            this.logReader = logReader;
        }

        public SimpleTestResult GetTestResult(int processInd)
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