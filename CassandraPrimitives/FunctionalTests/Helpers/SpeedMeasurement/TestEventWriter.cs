using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog.EventLog;
using SKBKontur.Catalogue.CassandraPrimitives.FunctionalTests.EventContents.Contents;

namespace SKBKontur.Catalogue.CassandraPrimitives.FunctionalTests.Helpers.SpeedMeasurement
{
    public class TestEventWriter
    {
        public TestEventWriter(IEventRepository repository, OperationsSpeed speed, int objectsPerBatch)
        {
            this.repository = repository;
            this.speed = speed;
            this.objectsPerBatch = objectsPerBatch;
            stopEvent = new ManualResetEvent(false);
        }

        public void StopExecution()
        {
            stopEvent.Set();
        }

        public void BeginExecution()
        {
            var totalStopwatch = Stopwatch.StartNew();
            var totalCount = 0;
            try
            {
                stopEvent.Reset();
                while(true)
                {
                    if(stopEvent.WaitOne(0))
                        break;

                    DoWrite();
                    totalCount += objectsPerBatch;
                    var actualSpeed = OperationsSpeed.FromBatchAction(DoWrite, objectsPerBatch);
                    if(actualSpeed > speed)
                    {
                        var timeoutToGetDesiredSpeed = actualSpeed.TimeoutToGetDesiredSpeed(speed, objectsPerBatch);
                        Console.WriteLine("Sleeping for {0}ms to yield to desired speed", timeoutToGetDesiredSpeed.TotalMilliseconds);
                        Thread.Sleep(timeoutToGetDesiredSpeed);
                    }
                    else
                        Console.WriteLine("Desired speed {0} is more than actual {1}", speed, actualSpeed);
                }
            }
            finally
            {
                totalStopwatch.Stop();
                var totalSpeed = OperationsSpeed.PerTimeSpan(totalCount, totalStopwatch.Elapsed);
                Console.WriteLine("Total average speed: {0}", totalSpeed);
            }
        }

        private void DoWrite()
        {
            repository.AddEvents(
                Guid.NewGuid().ToString(),
                Enumerable
                    .Range(0, objectsPerBatch)
                    .Select(i => Guid.NewGuid().ToString())
                    .Select(x => new TestContent {Id = x})
                    .OfType<object>()
                    .ToArray());
        }

        private readonly IEventRepository repository;
        private readonly OperationsSpeed speed;
        private readonly int objectsPerBatch;

        private readonly ManualResetEvent stopEvent;
    }
}