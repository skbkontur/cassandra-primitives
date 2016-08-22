using System;

using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons
{
    public static class TeamCityExtensions
    {
        public static IDisposable MessageBlock(this ITeamCityLogger teamCityLogger, string name)
        {
            teamCityLogger.BeginMessageBlock(name);
            return new MessageBlockDisposer(teamCityLogger);
        }

        public static IDisposable Activity(this ITeamCityLogger teamCityLogger, string name)
        {
            teamCityLogger.BeginActivity(name);
            return new ActivityDisposer(teamCityLogger);
        }

        private abstract class Disposer : IDisposable
        {
            protected Disposer(ITeamCityLogger teamCityLogger)
            {
                this.teamCityLogger = teamCityLogger;
            }

            public abstract void Dispose();
            protected readonly ITeamCityLogger teamCityLogger;
        }

        private class MessageBlockDisposer : Disposer
        {
            public MessageBlockDisposer(ITeamCityLogger teamCityLogger)
                : base(teamCityLogger)
            {
            }

            public override void Dispose()
            {
                teamCityLogger.EndMessageBlock();
            }
        }

        private class ActivityDisposer : Disposer
        {
            public ActivityDisposer(ITeamCityLogger teamCityLogger)
                : base(teamCityLogger)
            {
            }

            public override void Dispose()
            {
                teamCityLogger.EndActivity();
            }
        }
    }
}