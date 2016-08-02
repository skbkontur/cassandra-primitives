using System;
using System.Collections.Generic;
using System.Linq;

using Metrics;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestProgressProcessors;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Timeline
{
    public class TimelineTestProgressProcessor : AbstractTestProgressProcessor<TimelineProgressMessage>
    {
        public TimelineTestProgressProcessor(TestConfiguration configuration, TimelineTestOptions testOptions, ITeamCityLogger teamCityLogger, MetricsContext metricsContext)
            : base(configuration, teamCityLogger, metricsContext)
        {
            this.testOptions = testOptions;
            allLockEvents = new List<TimelineProgressMessage.LockEvent>();
            recentLockEvents = new SortedSet<TimelineProgressMessage.LockEvent>(new TimelineProgressMessage.LockEventComparer());

            startTime = long.MaxValue;
            endTime = 0;
            owningTime = 0;

            metricsContext.Gauge("Time owning lock", () => (double)owningTime * 100 / (endTime - startTime), Unit.Percent);
        }

        private long GetAmountOfTimeWhenPredicateIsTrue(List<Event> sortedEvents, Func<int, bool> predicate)
        {
            if (allLockEvents.Count == 0)
                return 0;

            long totalTrueTime = 0;

            var balance = 0;
            bool lastPredicateValue = false;
            long lastTrue = 0;
            foreach (var @event in sortedEvents)
            {
                balance += @event.Type;
                if (balance < 0)
                    teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Warning, "Broken timeline: balance = {0}", balance);

                var predicateValue = predicate(balance);

                if (!lastPredicateValue && predicateValue)
                    lastTrue = @event.Time;
                if (lastPredicateValue && !predicateValue)
                    totalTrueTime += @event.Time - lastTrue;
                lastPredicateValue = predicateValue;
            }
            if (lastPredicateValue)
                totalTrueTime += sortedEvents.Last().Time - lastTrue;
            return totalTrueTime;
        }

        private void AnalyseAllLockEvents()
        {
            if (allLockEvents.Count == 0)
            {
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Warning, "Lock events log is empty, although final analysing of the timeline already started");
                return;
            }

            var sortedEvents = allLockEvents
                .SelectMany(e => new[]
                    {
                        new Event(e.AcquiredAt, 1),
                        new Event(e.ReleasedAt, -1),
                    })
                .OrderBy(e => e)
                .ToList();

            var minTime = sortedEvents.First().Time;
            var maxTime = sortedEvents.Last().Time;
            var timeDelta = maxTime - minTime;

            var overlapRate = GetAmountOfTimeWhenPredicateIsTrue(sortedEvents, x => x > 1);
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Overlap rate: {0}% (total - {1} ms)", overlapRate * 100.0 / timeDelta, overlapRate);

            var owningRate = GetAmountOfTimeWhenPredicateIsTrue(sortedEvents, x => x == 1);
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Owning rate: {0}% (total - {1} ms)", owningRate * 100.0 / timeDelta, owningRate);

            var fightingRate = GetAmountOfTimeWhenPredicateIsTrue(sortedEvents, x => x == 0);
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Fighting rate: {0}% (total - {1} ms)", fightingRate * 100.0 / timeDelta, fightingRate);

            var brokenRate = GetAmountOfTimeWhenPredicateIsTrue(sortedEvents, x => x < 0);
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Broken rate: {0}% (total - {1} ms)", brokenRate * 100.0 / timeDelta, brokenRate);
        }

        private void ProcessLockEvents(List<TimelineProgressMessage.LockEvent> lockEvents)
        {
            if (lockEvents == null || lockEvents.Count == 0)
                return;
            if (lockEvents.Any(e => (e.ReleasedAt - e.AcquiredAt) == 0))
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Warning, "Event of zero duration found");
            lockEvents = lockEvents.Where(e => (e.ReleasedAt - e.AcquiredAt) != 0).ToList();

            allLockEvents.AddRange(lockEvents);
            recentLockEvents.UnionWith(lockEvents);
            //owningTime = lockEvents.Aggregate(owningTime, (prev, e) => prev + e.ReleasedAt - e.AcquiredAt);

            var time = (long)DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1, 0, 0, 0)).TotalMilliseconds;
            while (recentLockEvents.Count != 0 && recentLockEvents.Min.ReleasedAt < time - 1000 * 60 * 3)
            {
                var itemToRemove = recentLockEvents.Min;
                recentLockEvents.Remove(itemToRemove);
                //owningTime -= itemToRemove.ReleasedAt - itemToRemove.AcquiredAt;
            }

            // It's slow, because it's LINQ's min and max, not SortedSet's one. And Aggregate also takes O(recentLockEvents.Count) time.
            // But on the other hand, it helps to recover timeline quite fast if there were some bad events (equal ones, for example)
            // If we will have problems with perfomance, we can rewrite it to dynamic update of owning time (see commented lines above)
            // (But then we will need to have another sorted set with events sorted by AcquiredAt)
            startTime = Math.Min(long.MaxValue, recentLockEvents.Min(e => e.AcquiredAt));
            endTime = Math.Max(0, recentLockEvents.Max(e => e.ReleasedAt));
            owningTime = recentLockEvents.Aggregate((long)0, (prev, e) => prev + e.ReleasedAt - e.AcquiredAt);

            ReportProgressToTeamCity();
        }

        protected override string GetTestName()
        {
            return "TimelineTest";
        }

        public override string HandleProgressMessage(TimelineProgressMessage message, int processInd)
        {
            if (message.Final)
            {
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Process {0} finished work", processInd);
                finishedProcesses++;
                if (finishedProcesses == configuration.AmountOfProcesses)
                    AnalyseAllLockEvents();
            }
            else
                ProcessLockEvents(message.LockEvents);
            return null;
        }

        public override string HandleLogMessage(string message, int processInd)
        {
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Process {0} says: {1}", processInd, message);
            return null;
        }

        protected override double GetProgressInPercents()
        {
            return allLockEvents.Count * 100.0 / (configuration.AmountOfProcesses * configuration.AmountOfThreads * testOptions.AmountOfLocksPerThread);
        }

        private readonly List<TimelineProgressMessage.LockEvent> allLockEvents;
        private readonly SortedSet<TimelineProgressMessage.LockEvent> recentLockEvents;
        private long startTime, endTime, owningTime;
        private int finishedProcesses;
        private readonly TimelineTestOptions testOptions;
    }
}