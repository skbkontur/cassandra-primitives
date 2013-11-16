using System;
using System.Diagnostics;

namespace SKBKontur.Catalogue.CassandraPrimitives.FunctionalTests.Helpers.SpeedMeasurement
{
    public class OperationsSpeed : IComparable<OperationsSpeed>
    {
        public OperationsSpeed(double operationsPerSecond)
        {
            this.operationsPerSecond = operationsPerSecond;
        }

        public int CompareTo(OperationsSpeed other)
        {
            return operationsPerSecond.CompareTo(other.operationsPerSecond);
        }

        public static OperationsSpeed PerSecond(int operationsPerSecond)
        {
            return new OperationsSpeed(operationsPerSecond);
        }

        public static OperationsSpeed PerTimeSpan(int operationCount, TimeSpan timeSpan)
        {
            return new OperationsSpeed(operationCount / timeSpan.TotalSeconds);
        }

        public static OperationsSpeed SingeOperationPerTimeSpan(TimeSpan timeSpan)
        {
            return PerTimeSpan(1, timeSpan);
        }

        public TimeSpan TimeoutToGetDesiredSpeed(OperationsSpeed desiredSpeed, int operationsPerAttempt)
        {
            return desiredSpeed.GetTimeSpanToExecuteOperations(operationsPerAttempt) - GetTimeSpanToExecuteOperations(operationsPerAttempt);
        }

        public static OperationsSpeed FromBatchAction(Action action, int batchSize)
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
            return PerTimeSpan(batchSize, stopwatch.Elapsed);
        }

        public static bool operator <(OperationsSpeed speed1, OperationsSpeed speed2)
        {
            return speed1.CompareTo(speed2) < 0;
        }

        public static bool operator >(OperationsSpeed speed1, OperationsSpeed speed2)
        {
            return speed1.CompareTo(speed2) > 0;
        }

        public static bool operator <=(OperationsSpeed speed1, OperationsSpeed speed2)
        {
            return speed1.CompareTo(speed2) <= 0;
        }

        public static bool operator >=(OperationsSpeed speed1, OperationsSpeed speed2)
        {
            return speed1.CompareTo(speed2) >= 0;
        }

        private TimeSpan GetTimeSpanToExecuteOperations(int operationsPerAttempt)
        {
            return TimeSpan.FromSeconds(operationsPerAttempt / operationsPerSecond);
        }

        private readonly double operationsPerSecond;

        public override string ToString()
        {
            return string.Format("Speed: {0} ops", operationsPerSecond);
        }
    }
}