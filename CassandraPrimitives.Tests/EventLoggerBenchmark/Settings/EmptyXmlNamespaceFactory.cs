using GroboSerializer.XmlNamespaces;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.EventLoggerBenchmark.Settings
{
    public class EmptyXmlNamespaceFactory : IXmlNamespaceFactory
    {
        public XmlNamespace GetNamespace(string namespacePrefix)
        {
            return new XmlNamespace();
        }
    }
}