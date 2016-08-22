namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.JmxInitialisation
{
    public class JmxSettings
    {
        public JmxSettings(string host, string @alias, string graphitePrefix, int port = 7199, string graphiteHost = "graphite-relay.skbkontur.ru", int graphitePort = 2003)
        {
            Alias = alias;
            Host = host;
            Port = port;
            GraphiteHost = graphiteHost;
            GraphitePort = graphitePort;
            GraphitePrefix = graphitePrefix;
        }

        public string Alias { get; private set; }
        public string Host { get; private set; }
        public int Port { get; private set; }
        public string GraphiteHost { get; private set; }
        public int GraphitePort { get; private set; }
        public string GraphitePrefix { get; private set; }
    }
}