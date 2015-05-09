using System;

using SKBKontur.Catalogue.ClientLib.Domains;
using SKBKontur.Catalogue.ClientLib.HttpClientBases;
using SKBKontur.Catalogue.ClientLib.HttpClientBases.Configuration;
using SKBKontur.Catalogue.ClientLib.Topology;

namespace SKBKontur.Catalogue.CassandraPrimitives.TimeServiceClient
{
    public class TimeServiceClient : HttpClientBase, ITimeServiceClient
    {
        public TimeServiceClient(IDomainTopologyFactory domainTopologyFactory, IMethodDomainFactory methodDomainFactory, IHttpServiceClientConfiguration configuration)
            : base(domainTopologyFactory, methodDomainFactory, configuration)
        {
        }

        public void ForceUpdate()
        {
            Method("ForceUpdate").SendToEachReplica(DomainConsistencyLevel.All);
        }

        public long GetNowTicks()
        {
            return Method("GetNowTicks").InvokeOnRandomReplica().ThanReturn<long>();
        }

        protected override IHttpServiceClientConfiguration DoGetConfiguration(IHttpServiceClientConfiguration defaultConfiguration)
        {
            return defaultConfiguration.WithTimeout(TimeSpan.FromSeconds(30));
        }

        protected override string GetDefaultTopologyFileName()
        {
            return "timeServiceTopology";
        }
    }
}