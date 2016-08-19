using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations
{
    public class TestEnvironment : ITestEnvironment
    {
        public string AmountOfThreads { get; private set; }
        public string AmountOfProcesses { get; private set; }
        public string AmountOfClusterNodes { get; private set; }
        public string HttpPort { get; private set; }
        public string TestScenario { get; private set; }
        public string ClusterType { get; private set; }

        public static TestEnvironment GetFromEnvironment(IEnvironmentVariableProvider variableProvider)
        {
            AssertEnvirinmentVariableDefined(variableProvider, "AmountOfThreads");
            AssertEnvirinmentVariableDefined(variableProvider, "AmountOfProcesses");
            AssertEnvirinmentVariableDefined(variableProvider, "AmountOfClusterNodes");
            AssertEnvirinmentVariableDefined(variableProvider, "HttpPort");
            AssertEnvirinmentVariableDefined(variableProvider, "ClusterType");
            AssertEnvirinmentVariableDefined(variableProvider, "TestScenario");
            return new TestEnvironment
                {
                    AmountOfThreads = variableProvider.GetValue("AmountOfThreads"),
                    AmountOfProcesses = variableProvider.GetValue("AmountOfProcesses"),
                    AmountOfClusterNodes = variableProvider.GetValue("AmountOfClusterNodes"),
                    HttpPort = variableProvider.GetValue("HttpPort"),
                    ClusterType = variableProvider.GetValue("ClusterType"),
                    TestScenario = variableProvider.GetValue("TestScenario"),
                };
        }

        private static void AssertEnvirinmentVariableDefined(IEnvironmentVariableProvider variableProvider, string name)
        {
            if (string.IsNullOrEmpty(variableProvider.GetValue(name)))
                throw new Exception(string.Format("Environment variable {0} was not defined", name));
        }
    }
}