using System;
using System.Linq;
using System.Threading;

using NUnit.Framework;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Sharding;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.Commons.Speed;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.EventRepositoryTests
{
    [TestFixture]
    public class BoxEventRepositoryShardTests : BoxEventRepositoryTestBase
    {
        [Test]
        public void TestWriteWithDesiredSpeed()
        {
            var keyDistributor = new KeyDistributor(64);

            var threads = Enumerable.Range(0, 5).Select(number =>
                {
                    var repository = CreateBoxEventRepository((id, obj) => keyDistributor.Distribute(id.ScopeId).ToString());

                    var testEventWriter = new TestEventWriter(repository, OperationsSpeed.PerSecond(10000), 1000);
                    var thread = new Thread(testEventWriter.BeginExecution);
                    thread.Start();
                    return new {Thread = thread, Writer = testEventWriter};
                }).ToList();

            Thread.Sleep(TimeSpan.FromSeconds(30));

            threads.ForEach(x => x.Writer.StopExecution());
            threads.ForEach(x => x.Thread.Join());
        }
    }
}