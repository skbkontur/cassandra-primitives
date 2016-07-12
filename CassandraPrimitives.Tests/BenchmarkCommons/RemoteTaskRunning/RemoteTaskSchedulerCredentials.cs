namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning
{
    public class RemoteTaskSchedulerCredentials
    {
        public string MachineName { get; set; }
        public string UserName { get; set; }
        public string AccountDomain { get; set; }
        public string Password { get; set; }

        public RemoteTaskSchedulerCredentials(string machineName, string userName = null, string accountDomain = null, string password = null)
        {
            MachineName = machineName;
            UserName = userName;
            AccountDomain = accountDomain;
            Password = password;
        }
    }
}