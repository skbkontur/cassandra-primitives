﻿using System;
using System.IO;
using System.Linq;
using System.Threading;

using GroBuf;
using GroBuf.DataMembersExtracters;

using SkbKontur.Cassandra.Local;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Configuration.ColumnFamilies;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Sharding;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.Commons.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.Commons.Speed;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.EventLoggerBenchmark.EventContents;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.EventLoggerBenchmark.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;
using SKBKontur.Catalogue.TeamCity;

using Vostok.Logging;

using CassandraInitializerSettings = SKBKontur.Catalogue.CassandraPrimitives.Tests.EventLoggerBenchmark.Settings.CassandraInitializerSettings;
using CassandraMetaProvider = SKBKontur.Catalogue.CassandraPrimitives.Tests.EventLoggerBenchmark.Settings.CassandraMetaProvider;
using ColumnFamilies = SKBKontur.Catalogue.CassandraPrimitives.Tests.EventLoggerBenchmark.Settings.ColumnFamilies;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.EventLoggerBenchmark
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            var node = CreateCassandraNode();
            node.Restart();
            try
            {
                cassandraClusterSettings = node.CreateSettings();
                var initializerSettings = new CassandraInitializerSettings();
                var cassandraSchemeActualizer = new CassandraSchemeActualizer(new CassandraCluster(cassandraClusterSettings, logger), new CassandraMetaProvider(), initializerSettings);
                cassandraSchemeActualizer.AddNewColumnFamilies();
                Log4NetConfiguration.InitializeOnce();
                var teamCityLogger = new TeamCityLogger(Console.Out);

/*
            var totalSpeed1 = MeasureWriteSpeed(1, OperationsSpeed.PerSecond(10000));
            var totalSpeed2 = MeasureWriteSpeed(25, OperationsSpeed.PerSecond(10000));
            var totalSpeed3 = MeasureWriteSpeed(50, OperationsSpeed.PerSecond(10000));
            var totalSpeed4 = MeasureWriteSpeed(100, OperationsSpeed.PerSecond(10000));
*/

                //var totalSpeed5 = MeasureWriteSpeed(10, OperationsSpeed.PerSecond(2));
                //var totalSpeed6 = MeasureWriteSpeed(100, OperationsSpeed.PerSecond(2));

                teamCityLogger.BeginMessageBlock("Results");
                MeasureSpeed(teamCityLogger, 1, OperationsSpeed.PerSecond(20));
                MeasureSpeed(teamCityLogger, 1, OperationsSpeed.PerSecond(1000));
                MeasureSpeed(teamCityLogger, 25, OperationsSpeed.PerSecond(20));
                MeasureSpeed(teamCityLogger, 25, OperationsSpeed.PerSecond(50));
                var speed = MeasureSpeed(teamCityLogger, 25, OperationsSpeed.PerSecond(100));
                MeasureSpeed(teamCityLogger, 25, OperationsSpeed.PerSecond(200));
                MeasureSpeed(teamCityLogger, 50, OperationsSpeed.PerSecond(100));
                MeasureSpeed(teamCityLogger, 50, OperationsSpeed.PerSecond(200));
                teamCityLogger.EndMessageBlock();

                teamCityLogger.SetBuildStatus(TeamCityBuildStatus.Success, string.Format("20 Threads, {0} [Desired: {1}]", speed, OperationsSpeed.PerSecond(100)));
            }
            finally
            {
                node.Stop();
            }
        }

        protected static IEventRepository CreateBoxEventRepository(Func<EventId, object, string> calculateShard)
        {
            var serializer = new Serializer(new AllPropertiesExtractor());
            var cassandraCluster = new CassandraCluster(cassandraClusterSettings, logger);
            var eventTypeRegistry = new EventTypeRegistry();

            var factory = new EventRepositoryFactory(serializer, cassandraCluster, eventTypeRegistry, logger);
            var eventRepositoryColumnFamilyFullNames = new EventRepositoryColumnFamilyFullNames(
                ColumnFamilies.ticksHolder,
                ColumnFamilies.eventLog,
                ColumnFamilies.eventLogAdditionalInfo,
                ColumnFamilies.remoteLock);
            var shardCalculator = new ShardCalculator(calculateShard);
            var eventRepository = factory.CreateEventRepository(shardCalculator, eventRepositoryColumnFamilyFullNames);
            return eventRepository;
        }

        private static LocalCassandraNode CreateCassandraNode()
        {
            return new LocalCassandraNode(
                Path.Combine(FindCassandraTemplateDirectory(AppDomain.CurrentDomain.BaseDirectory), @"v2.2.x"),
                Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\DeployedCassandra"));
        }

        private static string FindCassandraTemplateDirectory(string currentDir)
        {
            if(currentDir == null)
                throw new Exception("Невозможно найти каталог с Cassandra-шаблонами");
            var cassandraTemplateDirectory = Path.Combine(currentDir, cassandraTemplates);
            return Directory.Exists(cassandraTemplateDirectory) ? cassandraTemplateDirectory : FindCassandraTemplateDirectory(Path.GetDirectoryName(currentDir));
        }

        private static OperationsSpeed MeasureSpeed(TeamCityLogger teamCityLogger, int writeThreadCount, OperationsSpeed operationsSpeed)
        {
            var totalSpeed7 = MeasureWriteSpeed(writeThreadCount, operationsSpeed);
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Threads: {0}, DesiredSpeed: {1}, ActualSpeed: {2}", writeThreadCount, operationsSpeed, totalSpeed7);
            return totalSpeed7;
        }

        private static OperationsSpeed MeasureWriteSpeed(int writeThreadCount, OperationsSpeed desiredSpeed)
        {
            using(var readEventLogRepository = CreateBoxEventRepository((x, y) => "0"))
            {
                var writers = Enumerable.Range(0, writeThreadCount).Select(
                    i =>
                        {
                            var eventLogRepository = CreateBoxEventRepository((x, y) => "0");
                            var writer = new TestEventWriter(eventLogRepository, desiredSpeed, 1);
                            var writeThread = new Thread(() =>
                                {
                                    try
                                    {
                                        writer.BeginExecution();
                                    }
                                    finally
                                    {
                                        eventLogRepository.Dispose();
                                    }
                                });
                            return new
                                {
                                    Writer = writer,
                                    Thread = writeThread
                                };
                        }
                    ).ToList();
                writers.ForEach(x => x.Thread.Start());

                var readThread = new Thread(() => ReadThreadProc(readEventLogRepository));
                readThread.Start();

                Thread.Sleep(TimeSpan.FromSeconds(10));
                writers.ForEach(x => x.Writer.StopExecution());

                Console.WriteLine("Waiting for write complete");
                writers.ForEach(x => x.Thread.Join());
                Console.WriteLine("WriteCompleted");
                readThread.Join();
                Console.WriteLine("Completed");
                var operationsSpeed = writers.Select(x => x.Writer.ResultAverageSpeed).Aggregate((x, y) => x + y);
                Console.WriteLine("Total speed of all threads: {0}", operationsSpeed / writeThreadCount);
                return operationsSpeed / writeThreadCount;
            }
        }

        private static void ReadThreadProc(IEventRepository readEventLogRepository)
        {
            var attempt = 0;
            EventInfo exclusiveEventInfo = null;
            while(true)
            {
                var readEvents = readEventLogRepository.GetEventsWithUnstableZone(exclusiveEventInfo, new[] {"0"}).ToList();
                if(readEvents.Count == 0)
                {
                    if(attempt == 10)
                        break;
                    attempt++;
                    Thread.Sleep((int)Math.Pow(2, attempt));
                    continue;
                }
                attempt = 0;
                foreach(var readEvent in readEvents)
                {
                    if(readEvent.StableZone)
                        exclusiveEventInfo = readEvent.Event.EventInfo;
                }
                Thread.Sleep(1);
            }
        }

        private const string cassandraTemplates = @"cassandra-local\cassandra";

        private static ICassandraClusterSettings cassandraClusterSettings;
        private static readonly ILog logger = new Log4NetWrapper(typeof(Program));
    }
}