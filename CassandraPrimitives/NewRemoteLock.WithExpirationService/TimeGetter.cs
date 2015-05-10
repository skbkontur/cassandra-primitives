using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core;
using SKBKontur.Catalogue.CassandraPrimitives.TimeServiceClient;

namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService
{
    public class TimeGetter : ITimeGetter
    {
        public TimeGetter(ITimeServiceClient timeServiceClient)
        {
            this.timeServiceClient = timeServiceClient;
        }

        public long GetNowTicks()
        {
            return timeServiceClient.GetNowTicks();
        }

        private readonly ITimeServiceClient timeServiceClient;
    }
}