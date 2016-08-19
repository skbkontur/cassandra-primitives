using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestOptions;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.BenchmarkConfiguration.TestOptions
{
    public class TestOptionsProvider : ITestOptionsProvider
    {
        public TestOptionsProvider(IEnvironmentVariableProvider variableProvider)
        {
            this.variableProvider = variableProvider;
        }

        public List<TTestOptions> CreateOptions<TTestOptions>()
            where TTestOptions : ITestOptions, new()
        {
            List<TTestOptions> testOptionsList = new List<TTestOptions>();

            var properties = typeof(TTestOptions)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.GetCustomAttribute<TestOptionAttribute>() != null)
                .ToList();

            var rawValues = properties
                .Select(GetValueForProperyFromRange)
                .ToArray();

            var product = ParametersParser.Product(rawValues);

            foreach (var values in product)
            {
                var testOptions = new TTestOptions();
                foreach (var propertyWithValue in properties.Zip<PropertyInfo, object, Tuple<PropertyInfo, object>>(values, Tuple.Create))
                {
                    propertyWithValue.Item1.SetValue(testOptions, propertyWithValue.Item2);
                }
                testOptionsList.Add(testOptions);
            }

            return testOptionsList;
        }

        private List<object> GetValueForProperyFromRange(PropertyInfo propertyInfo)
        {
            var testOptionAttribute = propertyInfo.GetCustomAttribute<TestOptionAttribute>();
            var value = variableProvider.GetValue(testOptionAttribute.Name ?? propertyInfo.Name);
            if (string.IsNullOrEmpty(value))
            {
                if (!testOptionAttribute.HasDefaultValue)
                    throw new Exception("Can't determine option value {0} because neither environment variable {1} nor default value was defined");
                return new List<object> {testOptionAttribute.DefaultValue};
            }
            var propertyType = propertyInfo.PropertyType;
            if (propertyType.IsSubclassOf(typeof(int)) || propertyType == typeof(int))
                return ParametersParser.ParseInts(propertyInfo.Name, value).Cast<object>().ToList();
            if (propertyType.IsSubclassOf(typeof(Enum)) || propertyType == typeof(Enum))
                return ParametersParser.ParseEnums(propertyType, propertyInfo.Name, value).Cast<object>().ToList();
            if (propertyType.IsSubclassOf(typeof(string)) || propertyType == typeof(string))
                return ParametersParser.ParseStrings(value).Cast<object>().ToList();
            throw new Exception(string.Format("Type {0} is not supported as test option", propertyType));
        }

        private readonly IEnvironmentVariableProvider variableProvider;
    }
}