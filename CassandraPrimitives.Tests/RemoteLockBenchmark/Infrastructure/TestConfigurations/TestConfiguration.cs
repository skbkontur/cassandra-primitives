using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.NetworkInformation;
using System.Text.RegularExpressions;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations
{
    public class TestConfiguration
    {
        public TestConfiguration(
            int amountOfThreads, 
            int amountOfProcesses, 
            int amountOfLocksPerThread, 
            int minWaitTimeMilliseconds, 
            int maxWaitTimeMilliseconds, 
            int amountOfClusterNodes, 
            string remoteHostName, 
            int httpPort, 
            RemoteLockImplementations remoteLockImplementation, 
            TestScenarios testScenario)
        {
            AmountOfThreads = amountOfThreads;
            AmountOfProcesses = amountOfProcesses;
            AmountOfLocksPerThread = amountOfLocksPerThread;
            MinWaitTimeMilliseconds = minWaitTimeMilliseconds;
            MaxWaitTimeMilliseconds = maxWaitTimeMilliseconds;
            AmountOfClusterNodes = amountOfClusterNodes;
            RemoteHostName = remoteHostName;
            HttpPort = httpPort;
            RemoteLockImplementation = remoteLockImplementation;
            TestScenario = testScenario;
        }

        public static TestConfiguration GetFromEnvironmentWithoutRanges()
        {
            var amountOfThreads = GetIntVariableFromEnvironment("benchmark.AmountOfThreads");
            var amountOfProcesses = GetIntVariableFromEnvironment("benchmark.AmountOfProcesses");
            var amountOfLocksPerThread = GetIntVariableFromEnvironment("benchmark.AmountOfLocksPerThread");
            var minWaitTimeMilliseconds = GetIntVariableFromEnvironment("benchmark.MinWaitTimeMilliseconds");
            var maxWaitTimeMilliseconds = GetIntVariableFromEnvironment("benchmark.MaxWaitTimeMilliseconds");
            var amountOfClusterNodes = GetIntVariableFromEnvironment("benchmark.AmountOfClusterNodes");
            var httpPort = GetIntVariableFromEnvironment("benchmark.HttpPort");

            RemoteLockImplementations remoteLockImplementation;
            if (!Enum.TryParse(Environment.GetEnvironmentVariable("benchmark.RemoteLockImplementation"), out remoteLockImplementation))
                throw new Exception(string.Format("Invalid value was given for parameter {0}", "benchmark.RemoteLockImplementation"));

            TestScenarios testScenario;
            if (!Enum.TryParse(Environment.GetEnvironmentVariable("benchmark.TestScenario"), out testScenario))
                throw new Exception(string.Format("Invalid value was given for parameter {0}", "benchmark.TestScenario"));

            var remoteHostName = IPGlobalProperties.GetIPGlobalProperties().HostName + "." + IPGlobalProperties.GetIPGlobalProperties().DomainName;

            return new TestConfiguration(
                amountOfThreads, 
                amountOfProcesses, 
                amountOfLocksPerThread, 
                minWaitTimeMilliseconds, 
                maxWaitTimeMilliseconds, 
                amountOfClusterNodes, 
                remoteHostName, 
                httpPort, 
                remoteLockImplementation, 
                testScenario);
        }

        public static List<TestConfiguration> GetFromEnvironmentWithRanges()
        {
            var amountOfThreads = GetRangeOrIntFromEnvironment("benchmark.AmountOfThreads");
            var amountOfProcesses = GetRangeOrIntFromEnvironment("benchmark.AmountOfProcesses");
            var amountOfLocksPerThread = GetRangeOrIntFromEnvironment("benchmark.AmountOfLocksPerThread");
            var minWaitTimeMilliseconds = GetRangeOrIntFromEnvironment("benchmark.MinWaitTimeMilliseconds");
            var maxWaitTimeMilliseconds = GetRangeOrIntFromEnvironment("benchmark.MaxWaitTimeMilliseconds");
            var amountOfClusterNodes = GetRangeOrIntFromEnvironment("benchmark.AmountOfClusterNodes");
            var httpPort = GetRangeOrIntFromEnvironment("benchmark.HttpPort");

            RemoteLockImplementations remoteLockImplementation;
            if (!Enum.TryParse(Environment.GetEnvironmentVariable("benchmark.RemoteLockImplementation"), out remoteLockImplementation))
                throw new Exception(string.Format("Invalid value was given for parameter {0}", "benchmark.RemoteLockImplementation"));

            TestScenarios testScenario;
            if (!Enum.TryParse(Environment.GetEnvironmentVariable("benchmark.TestScenario"), out testScenario))
                throw new Exception(string.Format("Invalid value was given for parameter {0}", "benchmark.TestScenario"));

            var remoteHostName = IPGlobalProperties.GetIPGlobalProperties().HostName + "." + IPGlobalProperties.GetIPGlobalProperties().DomainName;

            var combinations = Product(
                amountOfThreads, 
                amountOfProcesses, 
                amountOfLocksPerThread, 
                minWaitTimeMilliseconds, 
                maxWaitTimeMilliseconds, 
                amountOfClusterNodes, 
                httpPort);

            return combinations
                .Select(combination => new TestConfiguration(
                    combination[0], 
                    combination[1], 
                    combination[2], 
                    combination[3], 
                    combination[4], 
                    combination[5], 
                    remoteHostName, 
                    combination[6], 
                    remoteLockImplementation, 
                    testScenario))
                .ToList();
        }

        public static List<List<int>> Product(params List<int>[] lists)
        {
            return Product(0, lists);
        }

        private static List<List<int>> Product(int pos, params List<int>[] lists)
        {
            if (lists.Length == pos)
                return new List<List<int>> {new List<int>()};
            var subResults = Product(pos + 1, lists);
            var results = new List<List<int>>();
            foreach (var value in lists[pos])
                results.AddRange(subResults.Select(subResult => new List<int> {value}.Concat(subResult).ToList()));
            return results;
        }

        private static List<int> GetRangeOrIntFromEnvironment(string name)
        {
            var rawValue = Environment.GetEnvironmentVariable(name);
            if (rawValue == null)
                throw new Exception(string.Format("Parameter {0} was not set", name));

            rawValue = Regex.Replace(rawValue, @"\s*", "");
            int parsedInt;
            if (int.TryParse(Environment.GetEnvironmentVariable(name), out parsedInt))
                return new List<int>{parsedInt};
            var match = Regex.Match(rawValue, @"^range\((\d+),(\d+),(\d+)\)$");
            if (match.Success)
            {
                var start = int.Parse(match.Groups[0].Value);
                var end = int.Parse(match.Groups[1].Value);
                var step = int.Parse(match.Groups[2].Value);
                if (start > end)
                    throw new Exception("Invalid range: start is greater than end");
                if (step <= 0)
                    throw new Exception("Invalid range: step is less or equal to 0");
                if ((end - start) / step > maxIterationsOfSingleParameter)
                    throw new Exception("Invalid range: too many iterations");
                var result = new List<int>();
                for (int i = start; i < end; i += step)
                    result.Add(i);
                return result;
            }
            throw new Exception(string.Format("Invalid value of parameter {0}", name));
        }

        private const int maxIterationsOfSingleParameter = 100;

        private static int GetIntVariableFromEnvironment(string name)
        {
            int result;
            if (!int.TryParse(Environment.GetEnvironmentVariable(name), out result))
                throw new Exception(string.Format("Invalid value was given for parameter {0}", name));
            return result;
        }

        public int AmountOfThreads { get; private set; }
        public int AmountOfProcesses { get; private set; }
        public int AmountOfLocksPerThread { get; private set; }
        public int MinWaitTimeMilliseconds { get; private set; }
        public int MaxWaitTimeMilliseconds { get; private set; }
        public int AmountOfClusterNodes { get; private set; }
        public string RemoteHostName { get; private set; }
        public int HttpPort { get; private set; }
        public RemoteLockImplementations RemoteLockImplementation { get; private set; }
        public TestScenarios TestScenario { get; private set; }
    }
}