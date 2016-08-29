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
            string testScenario,
            string[] clusterEndpoints,
            int clusterPort)
        {
            AmountOfThreads = amountOfThreads;
            AmountOfProcesses = amountOfProcesses;
            AmountOfClusterNodes = amountOfClusterNodes;
            RemoteHostName = remoteHostName;
            HttpPort = httpPort;
            ClusterType = clusterType;
            TestScenario = testScenario;
            ClusterEndpoints = clusterEndpoints;
            ClusterPort = clusterPort;
        }

        public static TestConfiguration GetFromEnvironmentWithoutRanges(ITestEnvironment environment)
        {
            var amountOfThreads = ParametersParser.ParseInt("AmountOfThreads", environment.AmountOfThreads);
            var amountOfProcesses = ParametersParser.ParseInt("AmountOfProcesses", environment.AmountOfProcesses);
            var amountOfClusterNodes = ParametersParser.ParseInt("AmountOfClusterNodes", environment.AmountOfClusterNodes);
            var httpPort = ParametersParser.ParseInt("HttpPort", environment.HttpPort);

            ClusterTypes clusterType;
            if (!Enum.TryParse(environment.ClusterType, out clusterType))
                throw new Exception(string.Format("Invalid value was given for parameter {0}", "ClusterType"));

            var testScenario = environment.TestScenario;
            var clusterEndpoints = environment.ClusterEndpoints;
            var clusterPort = ParametersParser.ParseInt("ClusterPort", environment.ClusterPort);

            var remoteHostName = IPGlobalProperties.GetIPGlobalProperties().HostName + "." + IPGlobalProperties.GetIPGlobalProperties().DomainName;

            return new TestConfiguration(
                amountOfThreads,
                amountOfProcesses,
                amountOfClusterNodes,
                remoteHostName,
                httpPort,
                clusterType,
                testScenario,
                clusterEndpoints.Split('|'),
                clusterPort);
        }

        public static List<TestConfiguration> ParseWithRanges(ITestEnvironment environment)
        {
            var amountOfThreads = ParametersParser.ParseInts("AmountOfThreads", environment.AmountOfThreads);
            var amountOfProcesses = ParametersParser.ParseInts("AmountOfProcesses", environment.AmountOfProcesses);
            var amountOfClusterNodes = ParametersParser.ParseInts("AmountOfClusterNodes", environment.AmountOfClusterNodes);
            var httpPort = ParametersParser.ParseInts("HttpPort", environment.HttpPort);

            var clusterTypes = ParametersParser.ParseEnums<ClusterTypes>("ClusterType", environment.ClusterType);
            var testScenario = ParametersParser.ParseStrings(environment.TestScenario);
            var clusterEndpoints = environment.ClusterEndpoints;
            var clusterPort = ParametersParser.ParseInt("ClusterPort", environment.ClusterPort);

            var remoteHostName = IPGlobalProperties.GetIPGlobalProperties().HostName + "." + IPGlobalProperties.GetIPGlobalProperties().DomainName;

            var combinations = ParametersParser.Product(
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
                                           (string)combination[5],
                                           clusterEndpoints.Split('|'),
                                           clusterPort))
                .ToList();
        }

        public int AmountOfThreads { get; private set; }
        public int AmountOfProcesses { get; private set; }
        public int AmountOfClusterNodes { get; private set; }
        public string RemoteHostName { get; private set; }
        public int HttpPort { get; private set; }
        public ClusterTypes ClusterType { get; private set; }
        public string TestScenario { get; private set; }
        public string[] ClusterEndpoints { get; private set; }
        public int ClusterPort { get; private set; }

        public override string ToString()
        {
            return string.Format(
                @"AmountOfThreads = {0}
AmountOfProcesses = {1}
AmountOfClusterNodes = {2}
RemoteHostName = {3}
HttpPort = {4}
ClusterType = {5}
TestScenario = {6}
ClusterEndpoints = {7}
ClusterPort = {8}",
                AmountOfThreads,
                AmountOfProcesses,
                AmountOfClusterNodes,
                RemoteHostName,
                HttpPort,
                ClusterType,
                TestScenario,
                string.Join(", ", ClusterEndpoints),
                ClusterPort);
        }
    }
}