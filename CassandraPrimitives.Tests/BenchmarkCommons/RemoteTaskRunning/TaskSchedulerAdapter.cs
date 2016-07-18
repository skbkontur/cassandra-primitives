using System;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using System.Threading;

using log4net;

using Microsoft.Win32.TaskScheduler;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning
{
    public class TaskSchedulerAdapter : IDisposable
    {
        public TaskSchedulerAdapter(RemoteMachineCredentials credentials, string wrapperPath)
        {
            taskService = credentials == null ? new TaskService() : new TaskService(credentials.MachineName, credentials.UserName, credentials.AccountDomain, credentials.Password);
            this.credentials = credentials;
            logger = LogManager.GetLogger(GetType());
            this.wrapperPath = wrapperPath;
        }

        public Task RunTask(string taskName, string path, string[] arguments = null, string directory = null)
        {
            var argsRepresentation = string.Format("[{0}]", String.Join(", ", arguments ?? new string[0]));
            logger.InfoFormat("Scheduling task. Path = {0}, name = {1}, arguments = {2}, machine = {3}", path, taskName, argsRepresentation, credentials.MachineName);

            var existingTask = taskService.FindTask(taskName);
            if (existingTask != null)
            {
                logger.InfoFormat("Found existing task with name {0}", taskName);
                existingTask.Stop();
                taskService.RootFolder.DeleteTask(taskName);
                logger.InfoFormat("Existing task with name {0} successfully stopped and deleted", taskName);
            }
            else
                logger.InfoFormat("Existing task with name {0} not found", taskName);

            try
            {
                Task task = CreateTask(taskName, path, arguments, directory);
                task.Run();
                if (!IsTaskRunning(task, taskStartTimeoutMilliseconds))
                {
                    if (task.State == TaskState.Ready)
                        logger.ErrorFormat("The task with name {0} on machine {1} started, but immediately finished", taskName, credentials.MachineName);
                    else
                    {
                        logger.ErrorFormat("The task with name {0} on machine {1} didn't start for some unknown reasons", taskName, credentials.MachineName);
                        throw new Exception("Task didn't start for some unknown reasons");
                    }
                }
                else
                    logger.InfoFormat("The task with name {0} on machine {1} successfully started", taskName, credentials.MachineName);
                return task;
            }
            catch (COMException e)
            {
                logger.ErrorFormat("ErrorCode: {0}", e.ErrorCode);
                if (e.ErrorCode == -2147023108)
                    throw new Exception(String.Format("Unknown user {0}", credentials.UserName));
                if (e.ErrorCode == -2147023570)
                    throw new Exception(String.Format("Invalid user or password (username - {0})", credentials.UserName));
                throw;
            }
        }

        private bool IsTaskRunning(Task task, int timeoutMilliseconds)
        {
            var stopwatch = Stopwatch.StartNew();
            while (task.State != TaskState.Running && stopwatch.ElapsedMilliseconds < timeoutMilliseconds)
            {
                Thread.Sleep(100);
                return false;
            }
            return true;
        }

        private Task CreateTask(string taskName, string path, string[] arguments = null, string directory = null)
        {
            path = String.Format("\"{0}\"", path);

            var taskDefinition = taskService.NewTask();

            var joinedArgs = String.Join(" ", (arguments ?? new string[0]).Select(EscapeCmdArgument));

            var action = new ExecAction(path, joinedArgs, directory);

            taskDefinition.Actions.Add(action);
            taskDefinition.Triggers.Add(new TimeTrigger());
            taskDefinition.Principal.RunLevel = TaskRunLevel.Highest;
            taskDefinition.Principal.LogonType = TaskLogonType.Password;
            taskDefinition.Settings.AllowDemandStart = true;
            taskDefinition.Settings.StartWhenAvailable = true;
            taskDefinition.Settings.AllowHardTerminate = true;
            taskDefinition.Settings.ExecutionTimeLimit = TimeSpan.Zero;
            taskDefinition.Settings.Priority = ProcessPriorityClass.High;
            var task = taskService.RootFolder.RegisterTaskDefinition(taskName, taskDefinition, TaskCreation.CreateOrUpdate, "SYSTEM", null, TaskLogonType.Password);

            return task;
        }

        public Task RunTaskInWrapper(string taskName, string path, string[] arguments = null, string directory = null)
        {
            return RunTask(taskName, wrapperPath, new[] {"Normal", path}.Concat(arguments ?? new string[0]).ToArray(), directory);
        }

        private string EscapeCmdArgument(string argument)
        {
            var r = new Regex(@"\\(?=""|$)");
            var semiEscaped = r.Replace(argument, @"\\\\");
            var result = String.Format("\"{0}\"", semiEscaped.Replace(@"""", @""""""""""));
            return result;
        }

        public void Dispose()
        {
            taskService.Dispose();
        }

        private const int taskStartTimeoutMilliseconds = 3000;

        private readonly string wrapperPath;
        private readonly TaskService taskService;
        private readonly ILog logger;
        private readonly RemoteMachineCredentials credentials;
    }
}