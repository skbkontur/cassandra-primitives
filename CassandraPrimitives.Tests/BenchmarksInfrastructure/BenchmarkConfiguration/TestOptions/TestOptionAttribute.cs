using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.BenchmarkConfiguration.TestOptions
{
    [AttributeUsage(AttributeTargets.Property)]
    public class TestOptionAttribute : Attribute
    {
        public TestOptionAttribute(string name = null, object defaultValue = null)
        {
            Name = name;
            DefaultValue = defaultValue;
        }

        public string Name { get; private set; }
        public object DefaultValue { get; private set; }

        public bool HasDefaultValue { get { return DefaultValue != null; } }
    }
}