using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.NetworkInformation;
using System.Runtime.CompilerServices;
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
            var amountOfThreads = GetIntegerValuesOfParameterFromEnvironment("benchmark.AmountOfThreads");
            var amountOfProcesses = GetIntegerValuesOfParameterFromEnvironment("benchmark.AmountOfProcesses");
            var amountOfLocksPerThread = GetIntegerValuesOfParameterFromEnvironment("benchmark.AmountOfLocksPerThread");
            var minWaitTimeMilliseconds = GetIntegerValuesOfParameterFromEnvironment("benchmark.MinWaitTimeMilliseconds");
            var maxWaitTimeMilliseconds = GetIntegerValuesOfParameterFromEnvironment("benchmark.MaxWaitTimeMilliseconds");
            var amountOfClusterNodes = GetIntegerValuesOfParameterFromEnvironment("benchmark.AmountOfClusterNodes");
            var httpPort = GetIntegerValuesOfParameterFromEnvironment("benchmark.HttpPort");

            var remoteLockImplementation = GetEnumValuesOfParameterFromEnvironment<RemoteLockImplementations>("benchmark.RemoteLockImplementation");
            var testScenario = GetEnumValuesOfParameterFromEnvironment<TestScenarios>("benchmark.TestScenario");

            var remoteHostName = IPGlobalProperties.GetIPGlobalProperties().HostName + "." + IPGlobalProperties.GetIPGlobalProperties().DomainName;

            var combinations = Product(
                amountOfThreads.Cast<object>().ToList(),
                amountOfProcesses.Cast<object>().ToList(),
                amountOfLocksPerThread.Cast<object>().ToList(),
                minWaitTimeMilliseconds.Cast<object>().ToList(),
                maxWaitTimeMilliseconds.Cast<object>().ToList(),
                amountOfClusterNodes.Cast<object>().ToList(),
                httpPort.Cast<object>().ToList(),
                remoteLockImplementation.Cast<object>().ToList(),
                testScenario.Cast<object>().ToList());

            return combinations
                .Select(combination => new TestConfiguration(
                    (int)combination[0], 
                    (int)combination[1], 
                    (int)combination[2], 
                    (int)combination[3], 
                    (int)combination[4], 
                    (int)combination[5], 
                    remoteHostName, 
                    (int)combination[6], 
                    (RemoteLockImplementations)combination[7],
                    (TestScenarios)combination[8]))
                .ToList();
        }

        private static List<List<object>> Product(params List<object>[] lists)
        {
            return Product(0, lists);
        }

        private static List<List<object>> Product(int pos, params List<object>[] lists)
        {
            if (lists.Length == pos)
                return new List<List<object>> { new List<object>() };
            var subResults = Product(pos + 1, lists);
            var results = new List<List<object>>();
            foreach (var value in lists[pos])
                results.AddRange(subResults.Select(subResult => new List<object> { value }.Concat(subResult).ToList()));
            return results;
        }

        private static List<string> GetStringValuesOfParameterFromEnvironment(string parameterName)
        {
            var rawValue = Environment.GetEnvironmentVariable(parameterName);
            if (rawValue == null)
                throw new Exception(string.Format("Parameter {0} was not set", parameterName));

            rawValue = Regex.Replace(rawValue, @"\s*", "");
            List<string> resultList;
            if (TryParseList(rawValue, out resultList))
                return resultList;
            return new List<string>{rawValue};
        }

        private static List<TEnum> GetEnumValuesOfParameterFromEnvironment<TEnum>(string parameterName)
            where TEnum : struct 
        {
            var values = GetStringValuesOfParameterFromEnvironment(parameterName);
            var result = new List<TEnum>();
            foreach (var value in values)
            {
                TEnum currentResult;
                if (!Enum.TryParse(value, out currentResult))
                    throw new Exception(string.Format("Invalid value of enum parameter {0}", parameterName));
                result.Add(currentResult);
            }
            return result;
        }

        private static List<int> GetIntegerValuesOfParameterFromEnvironment(string parameterName)
        {
            var rawValue = Environment.GetEnvironmentVariable(parameterName);
            if (rawValue == null)
                throw new Exception(string.Format("Parameter {0} was not set", parameterName));

            rawValue = Regex.Replace(rawValue, @"\s*", "");

            int result;
            if (int.TryParse(rawValue, out result))
                return new List<int> {result};
            List<int> resultList;
            if (TryParseRange(rawValue, out resultList))
                return resultList;
            if (TryParseListOfInts(rawValue, out resultList))
                return resultList;

            throw new Exception(string.Format("Invalid value of integer parameter {0}", parameterName));
        }

        private static bool TryParseRange(string source, out List<int> result)
        {
            source = Regex.Replace(source, @"\s*", "");
            var match = Regex.Match(source, @"^range\((\d+),(\d+),(\d+)\)$");
            if (match.Success)
            {
                var start = int.Parse(match.Groups[1].Value);
                var end = int.Parse(match.Groups[2].Value);
                var step = int.Parse(match.Groups[3].Value);
                if (start > end || step <= 0 || (end - start) / step > maxIterationsOfSingleParameter)
                {
                    result = null;
                    return false;
                }
                result = new List<int>();
                for (int i = start; i < end; i += step)
                    result.Add(i);
                return true;
            }
            result = null;
            return false;
        }

        private static bool TryParseListOfInts(string source, out List<int> result)
        {
            List<string> parsedTokens;
            if (!TryParseList(source, out parsedTokens))
            {
                result = null;
                return false;
            }
            var ints = new List<int>();
            foreach (var token in parsedTokens)
            {
                int value;
                if (!int.TryParse(token, out value))
                {
                    result = null;
                    return false;
                }
                ints.Add(value);
            }
            result = ints;
            return true;
        }

        private static bool TryParseList(string source, out List<string> result)
        {
            source = Regex.Replace(source, @"\s*", "");
            var match = Regex.Match(source, @"^\[([^,]+)(,[^,]+)*\]$");
            if (match.Success)
            {
                var array = new string[match.Groups.Count];
                match.Groups.CopyTo(array, 0);
                result = array.ToList();
                return true;
            }
            result = null;
            return false;
        }

        private const int maxIterationsOfSingleParameter = 100000;

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