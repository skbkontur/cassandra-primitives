using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.Commons.Contents;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.Commons.Speed
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

                    totalCount += objectsPerBatch;
                    DoWrite();
                    var actualSpeed = SpeedMeasurement.FromMeasurement(totalCount, totalStopwatch.Elapsed);
                    if(actualSpeed.Speed > speed)
                    {
                        var timeoutToGetDesiredSpeed = actualSpeed.TimeoutToGetDesiredSpeed(speed);
                        Thread.Sleep(timeoutToGetDesiredSpeed);
                    }   
                }
            }
            finally
            {
                totalStopwatch.Stop();
                var totalSpeed = OperationsSpeed.PerTimeSpan(totalCount, totalStopwatch.Elapsed);
                ResultAverageSpeed = totalSpeed;
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

        public OperationsSpeed ResultAverageSpeed { get; private set; }

        private readonly IEventRepository repository;
        private readonly OperationsSpeed speed;
        private readonly int objectsPerBatch;

        private readonly ManualResetEvent stopEvent;
    }
}