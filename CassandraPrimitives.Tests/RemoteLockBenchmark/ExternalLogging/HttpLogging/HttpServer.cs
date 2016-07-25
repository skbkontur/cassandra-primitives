using System;
using System.Collections.Generic;
using System.Net;

using log4net;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging.HttpLogging
{
    public class HttpServer : IDisposable
    {
        public HttpServer(int port)
        {
            listeners = new List<HttpListener>();
            running = true;
            logger = LogManager.GetLogger(GetType());
            this.port = port;
        }

        private async void ProcessRequests(HttpListener listener, Action<HttpListenerContext> method)
        {
            if (!running || !listener.IsListening)
                return;
            try
            {
                var context = await await listener.GetContextAsync().ContinueWith(async t =>
                    {
                        ProcessRequests(listener, method);
                        return await t;
                    });
                method(context);
            }
            catch (Exception e)
            {
                logger.WarnFormat("Exception while processing http request:\n{0}", e);
            }
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
            {
                listener.Stop();
                listener.Close();
            }
        }

        private readonly List<HttpListener> listeners;
        private bool running;
        private readonly ILog logger;
        private readonly int port;
    }
}