using System;
using System.Collections.Generic;
using System.Net;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging.HttpLogging
{
    public class HttpServer : IDisposable
    {
        public HttpServer()
        {
            listeners = new List<HttpListener>();
            running = true;
        }

        private async void ProcessRequests(HttpListener listener, Action<HttpListenerContext> method)
        {
            while (running)
                await listener.GetContextAsync().ContinueWith(async t => method(await t)); //TODO exceptions are hiding here
        }

        public void AddMethod(string name, Action<HttpListenerContext> method)
        {
            var listener = new HttpListener();
            listener.Prefixes.Add(String.Format("http://*:{0}/{1}/", port, name));
            listener.Start();
            listeners.Add(listener);
            ProcessRequests(listener, method);
        }

        public void Dispose()
        {
            running = false;
            foreach (var listener in listeners)
                listener.Stop();
        }

        private const int port = 12345;

        private readonly List<HttpListener> listeners;
        private bool running;
    }
}