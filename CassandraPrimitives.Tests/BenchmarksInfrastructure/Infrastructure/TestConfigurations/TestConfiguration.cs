using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.NetworkInformation;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations
{
    public class TestConfiguration
    {
        public TestConfiguration(
            int amountOfThreads,
            int amountOfProcesses,
            int amountOfClusterNodes,
            string remoteHostName,
            int httpPort,
            ClusterTypes clusterType,
            string testScenario)
        {
            AmountOfThreads = amountOfThreads;
            AmountOfProcesses = amountOfProcesses;
            AmountOfClusterNodes = amountOfClusterNodes;
            RemoteHostName = remoteHostName;
            HttpPort = httpPort;
            ClusterType = clusterType;
            TestScenario = testScenario;
        }

        public static TestConfiguration GetFromEnvironmentWithoutRanges(ITestEnvironment environment)
        {
            var amountOfThreads = OptionsParser.ParseInt("AmountOfThreads", environment.AmountOfThreads);
            var amountOfProcesses = OptionsParser.ParseInt("AmountOfProcesses", environment.AmountOfProcesses);
            var amountOfClusterNodes = OptionsParser.ParseInt("AmountOfClusterNodes", environment.AmountOfClusterNodes);
            var httpPort = OptionsParser.ParseInt("HttpPort", environment.HttpPort);

            ClusterTypes clusterType;
            if (!Enum.TryParse(environment.ClusterType, out clusterType))
                throw new Exception(string.Format("Invalid value was given for parameter {0}", "ClusterType"));

            var testScenario = environment.TestScenario;

            var remoteHostName = IPGlobalProperties.GetIPGlobalProperties().HostName + "." + IPGlobalProperties.GetIPGlobalProperties().DomainName;

            return new TestConfiguration(
                amountOfThreads,
                amountOfProcesses,
                amountOfClusterNodes,
                remoteHostName,
                httpPort,
                clusterType,
                testScenario);
        }

        public static List<TestConfiguration> ParseWithRanges(ITestEnvironment environment)
        {
            var amountOfThreads = OptionsParser.ParseInts("AmountOfThreads", environment.AmountOfThreads);
            var amountOfProcesses = OptionsParser.ParseInts("AmountOfProcesses", environment.AmountOfProcesses);
            var amountOfClusterNodes = OptionsParser.ParseInts("AmountOfClusterNodes", environment.AmountOfClusterNodes);
            var httpPort = OptionsParser.ParseInts("HttpPort", environment.HttpPort);

            var clusterTypes = OptionsParser.ParseEnums<ClusterTypes>("ClusterType", environment.ClusterType);
            var testScenario = OptionsParser.ParseStrings(environment.TestScenario);

            var remoteHostName = IPGlobalProperties.GetIPGlobalProperties().HostName + "." + IPGlobalProperties.GetIPGlobalProperties().DomainName;

            var combinations = OptionsParser.Product(
                amountOfThreads.Cast<object>().ToList(),
                amountOfProcesses.Cast<object>().ToList(),
                amountOfClusterNodes.Cast<object>().ToList(),
                httpPort.Cast<object>().ToList(),
                clusterTypes.Cast<object>().ToList(),
                testScenario.Cast<object>().ToList());

            return combinations
                .Select(combination => new TestConfiguration(
                                           (int)combination[0],
                                           (int)combination[1],
                                           (int)combination[2],
                                           remoteHostName,
                                           (int)combination[3],
                                           (ClusterTypes)combination[4],
                                           (string)combination[5]))
                .ToList();
        }

        public int AmountOfThreads { get; private set; }
        public int AmountOfProcesses { get; private set; }
        public int AmountOfClusterNodes { get; private set; }
        public string RemoteHostName { get; private set; }
        public int HttpPort { get; private set; }
        public ClusterTypes ClusterType { get; private set; }
        public string TestScenario { get; private set; }

        public override string ToString()
        {
            return string.Format(
                @"AmountOfThreads = {0}
AmountOfProcesses = {1}
AmountOfClusterNodes = {2}
RemoteHostName = {3}
HttpPort = {4}
ClusterType = {5}
TestScenario = {6}",
                AmountOfThreads,
                AmountOfProcesses,
                AmountOfClusterNodes,
                RemoteHostName,
                HttpPort,
                ClusterType,
                TestScenario);
        }
    }
}