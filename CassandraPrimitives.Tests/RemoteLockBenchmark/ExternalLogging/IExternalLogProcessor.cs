namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging
{
    public interface IExternalLogProcessor<out TTestResult>
    {
        void StartProcessingLog();
        //TTestResult GetTestResult(int processInd);//TODO
    }
}