using SKBKontur.Catalogue.CassandraPrimitives.Storages.GlobalTicksHolder;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests
{
    internal static class MinMaxTicksHolderExtensions
    {
        public static long UpdateAndGetMinTicks(this MinTicksHolder minTicksHolder, string name, long ticks)
        {
            minTicksHolder.UpdateMinTicks(name, ticks);
            return minTicksHolder.GetMinTicks(name).Value;
        }

        public static long UpdateAndGetMaxTicks(this MaxTicksHolder maxTicksHolder, string name, long ticks)
        {
            maxTicksHolder.UpdateMaxTicks(name, ticks);
            return maxTicksHolder.GetMaxTicks(name).Value;
        }
    }
}