using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;

using NUnit.Framework;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Helpers
{
    public static class MultithreadingTestHelper
    {
        public static Thread CreateThread(ConcurrentBag<Exception> errors, Action action, string threadId = null)
        {
            return new Thread(() =>
                {
                    try
                    {
                        action();
                    }
                    catch(Exception e)
                    {
                        errors.Add(e);
                    }
                })
                {
                    IsBackground = true,
                    Name = string.Format("test-{0}", threadId),
                };
        }

        public static void RunOnSeparateThreads(TimeSpan timeout, params Action[] actions)
        {
            var errors = new ConcurrentBag<Exception>();
            var threads = actions.Select((a, threadId) => CreateThread(errors, a, threadId.ToString())).ToList();
            foreach(var t in threads)
                t.Start();
            foreach(var t in threads)
            {
                if(!t.Join(timeout))
                    Assert.Fail("Thread did not terminate in: {0}", timeout);
                Assert.That(errors, Is.Empty);
            }
        }
    }
}