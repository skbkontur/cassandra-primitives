using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

using Metrics;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.ProgressMessages;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging.TestProgressProcessors
{
    public class TimelineTestProgressProcessor : ITestProgressProcessor, IDisposable
    {
        public TimelineTestProgressProcessor(TestConfiguration configuration, ITeamCityLogger teamCityLogger)
        {
            this.teamCityLogger = teamCityLogger;

            allLockEvents = new List<TimelineProgressMessage.LockEvent>();

            startTime = long.MaxValue;
            endTime = 0;
            owningTime = 0;

            Metric.SetGlobalContextName(string.Format("EDI.Benchmarks.{0}.{1}", Process.GetCurrentProcess().ProcessName.Replace('.', '_'), Environment.MachineName.Replace('.', '_')));
            metric = Metric.Config.WithHttpEndpoint("http://*:1234/").WithAllCounters();
            var graphiteUri = new Uri(string.Format("net.{0}://{1}:{2}", "tcp", "graphite-relay.skbkontur.ru", "2003"));
            Metric.Config.WithReporting(x => x.WithGraphite(graphiteUri, TimeSpan.FromSeconds(5)));

            Metric.Gauge("Time owning lock", () => (double)owningTime * 100 / (endTime - startTime), Unit.Percent);
            Metric.Gauge("Progress", () => allLockEvents.Count * 100.0 / (configuration.amountOfProcesses * configuration.amountOfThreads * configuration.amountOfLocksPerThread), Unit.Percent);
        }

        private Tuple<long, long> GetAmountOfTimeWhenPredicateIsTrue(Func<int, bool> predicate)
        {
            if (allLockEvents.Count == 0)
                return Tuple.Create((long)0, (long)0);

            var events = allLockEvents
                .Where(e => e.ReleasedAt != e.AcquiredAt)
                .SelectMany(e => new[]
                    {
                        new {Time = e.AcquiredAt, Type = 1},
                        new {Time = e.ReleasedAt, Type = -1}
                    })
                .OrderBy(e => e.Time * 3 + e.Type)
                .ToList();

            var min = events.First().Time;
            var max = events.Last().Time;

            long totalTrueTime = 0;

            var balance = 0;
            bool lastPredicateValue = false;
            long lastTrue = 0;
            foreach (var @event in events)
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
                totalTrueTime += max - lastTrue;
            return Tuple.Create(totalTrueTime, (max - min));
        }

        private void AnalyseAllLockEvents()
        {
            if (allLockEvents.Count == 0)
            {
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Warning, "Lock events log is empty, although one of processes already finished work");
                return;
            }

            if (allLockEvents.Any(e => (e.ReleasedAt - e.AcquiredAt) == 0))
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Warning, "Event of zero duration found");

            var overlapRate = GetAmountOfTimeWhenPredicateIsTrue(x => x > 1);
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Overlap rate: {0}% (total - {1} ms)", overlapRate.Item1 * 100.0 / overlapRate.Item2, overlapRate.Item1);

            var owningRate = GetAmountOfTimeWhenPredicateIsTrue(x => x == 1);
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Owning rate: {0}% (total - {1} ms)", owningRate.Item1 * 100.0 / owningRate.Item2, owningRate.Item1);

            var fightingRate = GetAmountOfTimeWhenPredicateIsTrue(x => x == 0);
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Fighting rate: {0}% (total - {1} ms)", fightingRate.Item1 * 100.0 / fightingRate.Item2, fightingRate.Item1);

            var brokenRate = GetAmountOfTimeWhenPredicateIsTrue(x => x < 0);
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Broken rate: {0}% (total - {1} ms)", brokenRate.Item1 * 100.0 / brokenRate.Item2, brokenRate.Item1);
        }

        private void ProcessLockEvents(List<TimelineProgressMessage.LockEvent> lockEvents)
        {
            if (lockEvents == null || lockEvents.Count == 0)
                return;
            allLockEvents.AddRange(lockEvents);
            startTime = Math.Min(startTime, allLockEvents.Min(e => e.AcquiredAt));
            endTime = Math.Max(endTime, allLockEvents.Max(e => e.ReleasedAt));
            owningTime = allLockEvents.Aggregate((long)0, (prev, e) => prev + e.ReleasedAt - e.AcquiredAt);
        }

        public string HandlePublishProgress(string request, int processInd)
        {
            var progressMessage = JsonConvert.DeserializeObject<TimelineProgressMessage>(request);

            if (progressMessage.Final)
            {
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Process {0} finished work", processInd);
                AnalyseAllLockEvents();
            }
            else
            {
                ProcessLockEvents(progressMessage.LockEvents);
                //teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Process {0} published intermediate result: {1} lock events", processInd, progressMessage.LockEvents.Count);
            }
            return null;
        }

        public string HandleLog(string request, int processInd)
        {
            var log = JObject.Parse(request);
            var message = log["message"].ToString();

            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Process {0} says: {1}", processInd, message);
            return null;
        }

        public void Dispose()
        {
            metric.Dispose();
        }

        private readonly List<TimelineProgressMessage.LockEvent> allLockEvents;
        private readonly ITeamCityLogger teamCityLogger;
        private readonly MetricsConfig metric;
        private long startTime, endTime, owningTime;
    }
}