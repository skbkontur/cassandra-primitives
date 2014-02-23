﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using MoreLinq;

using NUnit.Framework;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.EventRepositoryTests
{
    [TestFixture]
    public class LastEventInfoForEmptyResultTest : BoxEventRepositoryTestBase
    {
        [Test]
        public void TestLastEventInfoForEmptyResult()
        {
            var eventRepository = CreateBoxEventRepository((x, y) => x.ScopeId);

            const int eventCount = 10;
            const int threadCount = 10;

            var writeThreads = Enumerable
                .Range(0, threadCount)
                .Select(
                    i => (ThreadStart)(() =>
                        {
                            Console.WriteLine("Write {0} started", i);
                            var random = new Random(i);
                            Thread.Sleep(random.Next(1) * 100);
                            var list = new List<EventInfo>();
                            for(var j = 0; j < eventCount; j++)
                            {
                                list.Add(eventRepository.AddEvent(i.ToString(), GenerateEventContent()));
                                if(random.Next(2) == 1)
                                    Thread.Sleep(100);
                            }
                            LogEventBatch("Writer"+i, list.ToArray());
                            Console.WriteLine("Write {0} completed", i);
                        })
                )
                .Select(p => new Thread(p)).ToList();

            var readEvents = new List<List<Event>>();
            var readThreads = Enumerable
                .Range(0, threadCount)
                .Pipe(i => readEvents.Add(new List<Event>()))
                .Select(
                    i => (ThreadStart)(() =>
                        {
                            Console.WriteLine("Read {0} started", i);
                            
                            var lastNotEmptyResultTime = DateTime.Now;
                            EventInfo lastEventInfo = null;
                            while(readEvents[i].Count < eventCount && ((DateTime.Now - lastNotEmptyResultTime) < TimeSpan.FromMinutes(1)))
                            {
                                EventInfo newExclusiveEventInfoIfEmpty;
                                var events = eventRepository.GetEventsWithUnstableZone(lastEventInfo, new[] {i.ToString()}, out newExclusiveEventInfoIfEmpty).ToList();
                                var stableEvents = events.TakeWhile(x => x.StableZone).ToList();

                                Func<EventInfo, string> toString = ei => ei == null ? "" : string.Format("({2}){0}_{1}",ei.Id.ScopeId, ei.Id.Id, ei.Ticks);
                                LogEventBatch("Reader" + i, stableEvents.Select(x => x.Event).ToArray(), string.Format("Read from {0}. Stable:{1}, Total:{2}, LastGoodEvent:{3}", toString(lastEventInfo), stableEvents.Count, events.Count, toString(newExclusiveEventInfoIfEmpty)));

                                if(stableEvents.Count > 0)
                                    lastNotEmptyResultTime = DateTime.Now;

                                readEvents[i].AddRange(stableEvents.Select(x => x.Event));

                                if(events.Count == 0 && newExclusiveEventInfoIfEmpty != null)
                                    lastEventInfo = new[] {lastEventInfo, newExclusiveEventInfoIfEmpty}.Max();
                                
                                if(stableEvents.Count > 0)
                                    lastEventInfo = new[] {lastEventInfo, stableEvents.Last().Event.EventInfo}.Max();

                                Thread.Sleep(1);
                            }
                            Console.WriteLine("Read {0} completed", i);
                        })
                )
                .Select(p => new Thread(p)).ToList();

            readThreads.ForEach(t => t.Start());
            writeThreads.ForEach(x => x.Start());

            writeThreads.ForEach(x => x.Join());
            readThreads.ForEach(x => x.Join());

            Enumerable.Range(0, threadCount).ForEach(i => CheckEqualEvents(eventRepository.GetEvents(null, new[] {i.ToString()}).ToArray(), readEvents[i].ToArray()));
        }
    }
}