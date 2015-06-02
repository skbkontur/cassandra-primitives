using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;

using BenchmarkCassandraHelpers;

using GroBuf;
using GroBuf.DataMembersExtracters;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.ClusterDeployment;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;

namespace ProcessesBasedRemoteLockBenchmark
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            //new Program().Run();
        }

//        private void Run()
//        {
//            var communicator = new ProcessesCommunicator(new Serializer(new AllPropertiesExtractor()));
//            var node = communicator.GetCassandraNode();
//            node.Restart();
//            try
//            {
//                cassandraClusterSettings = node.CreateSettings(IPAddress.Loopback);
//                var initializerSettings = new CassandraInitializerSettings();
//                var benchmarkParameters = new[]
//                {
//                    new BenchmarkParameters(3, 10000, LockType.NewLockCassandraTTL),
//                    new BenchmarkParameters(3, 10000, LockType.NewLockExpirationService),
//                    new BenchmarkParameters(3, 10000, LockType.OldLock),
//                    new BenchmarkParameters(5, 10000, LockType.NewLockCassandraTTL),
//                    new BenchmarkParameters(5, 10000, LockType.NewLockExpirationService),
//                    new BenchmarkParameters(5, 10000, LockType.OldLock),
//                    new BenchmarkParameters(10, 10000, LockType.NewLockCassandraTTL),
//                    new BenchmarkParameters(10, 10000, LockType.NewLockExpirationService),
//                    new BenchmarkParameters(10, 10000, LockType.OldLock),
//                };
//                var columnFamilyFullNames = benchmarkParameters.Select(x => new ColumnFamilyFullName(x.Keyspace, x.ColumnFamily)).Concat(new[]{ColumnFamilies.expirationMonitoring, ColumnFamilies.timeService}).ToArray();
//                var cassandraSchemeActualizer = new CassandraSchemeActualizer(new CassandraCluster(cassandraClusterSettings), new BenchmarkMetaProvider(columnFamilyFullNames), initializerSettings);
//                cassandraSchemeActualizer.AddNewColumnFamilies();
//                Log4NetConfiguration.InitializeOnce();
//                timeServiceProcess = Process.Start(new ProcessStartInfo
//                {
//                    FileName = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\..\..\TimeService\bin\Debug\Catalogue.CassandraPrimitives.Tests.TimeService.exe"),
//                    RedirectStandardOutput = false,
//                    UseShellExecute = true,
//                    WindowStyle = ProcessWindowStyle.Normal,
//                    CreateNoWindow = false,
//                });
//                expirationServiceProcess = Process.Start(new ProcessStartInfo
//                {
//                    FileName = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\..\..\ExpirationService\bin\Debug\Catalogue.CassandraPrimitives.Tests.ExpirationService.exe"),
//                    RedirectStandardOutput = false,
//                    UseShellExecute = true,
//                    WindowStyle = ProcessWindowStyle.Normal,
//                    CreateNoWindow = false,
//                });
//                foreach(var benchmark in benchmarkParameters)
//                    RunBenchmark(benchmark, communicator);
//            }
//            finally
//            {
//                if(timeServiceProcess != null)
//                    timeServiceProcess.Kill();
//                if(expirationServiceProcess != null)
//                    expirationServiceProcess.Kill();
//                node.Stop();
//            }
//        }
//
//        private static void RunBenchmark(BenchmarkParameters benchmark, IProcessesCommunicator communicator)
//        {
//            var processes = new Process[benchmark.ProcessesCount];
//            var processId = Enumerable.Range(0, benchmark.ProcessesCount).Select(x => Guid.NewGuid().ToString()).ToArray();
//            for(var i = 0; i < benchmark.ProcessesCount; i++)
//            {
//                var parameters = new[]
//                {
//                    processId[i],
//                    benchmark.LockId,
//                    benchmark.Keyspace,
//                    benchmark.ColumnFamily,
//                    benchmark.LocksCount.ToString(),
//                    benchmark.LockType.ToString(),
//                };
//                processes[i] = Process.Start(new ProcessStartInfo
//                {
//                    FileName = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\..\..\LockProcess\bin\Debug\LockProcess.exe"),
//                    Arguments = string.Join(" ", parameters),
//                    RedirectStandardOutput = false,
//                    UseShellExecute = true,
//                    WindowStyle = ProcessWindowStyle.Normal,
//                    CreateNoWindow = false,
//                });
//            }
//            int running;
//            while((running = communicator.GetRunningProcessesCount()) != benchmark.ProcessesCount)
//            {
//                Console.WriteLine("Started {0}, need {1}", running, benchmark.ProcessesCount);
//                Thread.Sleep(200);
//            }
//            communicator.SendStartSignal(benchmark.Keyspace, benchmark.ColumnFamily);
//            while((running = communicator.GetRunningProcessesCount()) > 0)
//            {
//                Console.WriteLine("{0}, {1} processes, waiting {2} processes to stop", benchmark.LockType, benchmark.ProcessesCount, running);
//                Thread.Sleep(200);
//            }
//            var results = new double[benchmark.ProcessesCount][];
//            for(var i = 0; i < benchmark.ProcessesCount; i++)
//                results[i] = communicator.GetResults(benchmark.Keyspace, benchmark.ColumnFamily, processId[i]);
//            var filename = string.Format("{0}_{1}_{2}.txt", benchmark.LockType, benchmark.ProcessesCount, benchmark.LocksCount);
//            var lines = Enumerable.Range(0, benchmark.LocksCount).Select(lockIndex => string.Join("\t", Enumerable.Range(0, benchmark.ProcessesCount).Select(processIndex => results[processIndex][lockIndex]))).ToArray();
//            File.WriteAllLines(filename, lines);
//        }
//
//        private ICassandraClusterSettings cassandraClusterSettings;
//        private Process timeServiceProcess;
//        private Process expirationServiceProcess;
    }
}