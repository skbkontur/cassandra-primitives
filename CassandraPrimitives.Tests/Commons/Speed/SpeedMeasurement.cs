using System;
using System.Diagnostics;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.Commons.Speed
{
    public class SpeedMeasurement
    {
        private readonly int operations;
        private readonly TimeSpan interval;

        public SpeedMeasurement(int operations, TimeSpan interval)
        {
            this.operations = operations;
            this.interval = interval;
        }

        public static SpeedMeasurement FromMeasurement(int operations, TimeSpan interval)
        {
            return new SpeedMeasurement(operations, interval);
        }

        public static SpeedMeasurement FromBatchAction(Action action, int batchSize)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                action();
            }
            finally
            {
                stopwatch.Stop();
            }
            return FromMeasurement(batchSize, stopwatch.Elapsed);
        }

        public OperationsSpeed Speed { get { return OperationsSpeed.PerTimeSpan(operations, interval); } }

        public TimeSpan TimeoutToGetDesiredSpeed(OperationsSpeed speed)
        {
            return TimeSpan.FromMilliseconds(operations / (speed.OperationsPerSecond / 1000) - interval.TotalMilliseconds);
        }
    }
}