using System;
using System.Diagnostics;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.Commons.Speed
{
    public class OperationsSpeed : IComparable<OperationsSpeed>
    {
        public OperationsSpeed(double operationsPerSecond)
        {
            this.OperationsPerSecond = operationsPerSecond;
        }

        public int CompareTo(OperationsSpeed other)
        {
            return OperationsPerSecond.CompareTo(other.OperationsPerSecond);
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

        public override string ToString()
        {
            return $"Speed: {OperationsPerSecond} ops";
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

        public static OperationsSpeed operator /(OperationsSpeed speed1, int speed2)
        {
            return new OperationsSpeed(speed1.OperationsPerSecond / speed2);
        }

        public static OperationsSpeed operator +(OperationsSpeed speed1, OperationsSpeed speed2)
        {
            return new OperationsSpeed(speed1.OperationsPerSecond + speed2.OperationsPerSecond);
        }

        public double OperationsPerSecond { get; }

        private TimeSpan GetTimeSpanToExecuteOperations(int operationsPerAttempt)
        {
            return TimeSpan.FromSeconds(operationsPerAttempt / OperationsPerSecond);
        }
    }
}