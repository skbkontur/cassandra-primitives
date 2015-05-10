using System;
using System.Threading;

using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core;

namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithCassanrdaTTL
{
    public class TimeGetter : ITimeGetter
    {
        public long GetNowTicks()
        {
            var ticks = DateTime.UtcNow.Ticks;
            while (true)
            {
                var last = Interlocked.Read(ref lastTicks);
                var cur = Math.Max(ticks, last + 1);
                if (Interlocked.CompareExchange(ref lastTicks, cur, last) == last)
                    return cur;
            }
        }

        private long lastTicks;
    }
}