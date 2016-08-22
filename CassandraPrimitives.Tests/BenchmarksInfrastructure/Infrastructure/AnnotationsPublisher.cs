using System;
using System.Net.Http;
using System.Text;

using Newtonsoft.Json;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure
{
    class AnnotationsPublisher
    {
        public AnnotationsPublisher(string annotationsUrl = "https://graphite.skbkontur.ru/events/events")
        {
            this.annotationsUrl = annotationsUrl;
        }

        public void PublishAnnotation(string text, params string[] tags)
        {
            text = text.Replace("\r\n", "\n").Replace("\n", "<BR>");
            var data = JsonConvert.SerializeObject(new {desc = text, tags = string.Join(",", tags)});
            var client = new HttpClient();
            client.PostAsync(new Uri(annotationsUrl), new StringContent(data, Encoding.UTF8)).Wait();
        }

        private readonly string annotationsUrl;
    }
}