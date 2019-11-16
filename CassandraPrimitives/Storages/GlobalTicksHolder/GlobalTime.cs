using System;

using SkbKontur.Cassandra.TimeBasedUuid;

namespace SKBKontur.Catalogue.CassandraPrimitives.Storages.GlobalTicksHolder
{
    public class GlobalTime : IGlobalTime
    {
        public GlobalTime(ITicksHolder ticksHolder)
        {
            this.ticksHolder = ticksHolder;
        }

        public long UpdateNowTicks()
        {
            var actualTicks = ticksHolder.UpdateMaxTicks(globalTicksName, Timestamp.Now.Ticks);
            return actualTicks;
        }

        public long GetNowTicks()
        {
            return Math.Max(ticksHolder.GetMaxTicks(globalTicksName), Timestamp.Now.Ticks);
        }

        private const string globalTicksName = "GlobalTicks";

        private readonly ITicksHolder ticksHolder;
    }
}