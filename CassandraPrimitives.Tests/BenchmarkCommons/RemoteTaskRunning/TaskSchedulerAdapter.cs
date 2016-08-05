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
        public TaskSchedulerAdapter(RemoteMachineCredentials credentials, string tasksGroup)
        {
            taskService = credentials == null ? new TaskService() : new TaskService(credentials.MachineName, credentials.UserName, credentials.AccountDomain, credentials.Password);
            this.credentials = credentials ?? new RemoteMachineCredentials(Environment.MachineName);
            logger = LogManager.GetLogger(GetType());
            this.tasksGroup = tasksGroup;
        }

        public TaskSchedulerAdapter(string tasksGroup)
            : this(null, tasksGroup)
        {
        }

        public Task RunTask(string taskName, string path, string[] arguments = null, string directory = null)
        {
            var taskNameWithGroup = GetTaskNameWithGroup(taskName);

            var argsRepresentation = string.Format("[{0}]", string.Join(", ", arguments ?? new string[0]));
            logger.InfoFormat("Scheduling task. Path = {0}, name = {1}, arguments = {2}, machine = {3}", path, taskNameWithGroup, argsRepresentation, credentials.MachineName);

            StopAndDeleteTask(taskName);

            try
            {
                Task task = CreateTask(taskNameWithGroup, path, arguments, directory);
                task.Run();
                if (!IsTaskRunning(task, taskStartTimeoutMilliseconds))
                {
                    if (task.State == TaskState.Ready)
                        logger.ErrorFormat("The task with name {0} on machine {1} started, but almost immediately finished", taskNameWithGroup, credentials.MachineName);
                    else
                    {
                        logger.ErrorFormat("The task with name {0} on machine {1} didn't start for some unknown reasons. Current task state - {2}", taskNameWithGroup, credentials.MachineName, task.State);
                        throw new Exception(string.Format("Task didn't start for some unknown reasons. Current task state - {0}", task.State));
                    }
                }
                else
                    logger.InfoFormat("The task with name {0} on machine {1} successfully started", taskNameWithGroup, credentials.MachineName);
                return task;
            }
            catch (COMException e)
            {
                logger.ErrorFormat("ErrorCode: {0}", e.ErrorCode);
                if (e.ErrorCode == -2147023108)
                    throw new Exception(string.Format("Unknown user {0}", credentials.UserName));
                if (e.ErrorCode == -2147023570)
                    throw new Exception(string.Format("Invalid user or password (username - {0})", credentials.UserName));
                throw;
            }
        }

        public void StopAndDeleteTask(string taskName)
        {
            var taskNameWithGroup = GetTaskNameWithGroup(taskName);
            var existingTask = taskService.FindTask(taskNameWithGroup);
            if (existingTask != null)
            {
                logger.InfoFormat("Found existing task with name {0}", taskNameWithGroup);
                existingTask.Stop();
                taskService.RootFolder.DeleteTask(taskNameWithGroup);
                logger.InfoFormat("Existing task with name {0} successfully stopped and deleted", taskNameWithGroup);
            }
            else
                logger.InfoFormat("Existing task with name {0} not found", taskNameWithGroup);
        }

        public Task RunTaskInWrapper(string wrapperPath, string taskName, string path, string[] arguments = null, string directory = null)
        {
            var realArguments = new[] { "--priority", "Normal", "--path", path, "--arguments", string.Join(" ", (arguments ?? new string[0]).Select(EscapeArgumentForCmd)) };
            return RunTask(taskName, wrapperPath, realArguments, directory);
        }

        public void StopAllTasksFromGroup()
        {
            logger.InfoFormat("Going to stop all tasks from group {0} on machine {1}", tasksGroup, credentials.MachineName);
            var pattern = string.Format("{{TaskGroup-{0}}}_.*_{{TaskGroup-{0}}}", tasksGroup);
            logger.InfoFormat("Will search by pattern {0}", pattern);
            var tasks = taskService.FindAllTasks(new Regex(pattern));
            logger.InfoFormat("Found {0} tasks", tasks.Length);
            foreach (var task in tasks)
            {
                logger.InfoFormat("Going to stop task {0}", task.Name);
                task.Stop();
                logger.InfoFormat("Task {0} stopped", task.Name);
                logger.InfoFormat("Going to delete task {0}", task.Name);
                taskService.RootFolder.DeleteTask(task.Name);
                logger.InfoFormat("Task {0} deleted", task.Name);
            }
        }

        private bool IsTaskRunning(Task task, int timeoutMilliseconds)
        {
            var stopwatch = Stopwatch.StartNew();
            while (task.State != TaskState.Running && stopwatch.ElapsedMilliseconds < timeoutMilliseconds)
            {
                Thread.Sleep(100);
            }
            return task.State == TaskState.Running;
        }

        private Task CreateTask(string taskNameWithGroup, string path, string[] arguments = null, string directory = null)
        {
            path = string.Format("\"{0}\"", path);

            var taskDefinition = taskService.NewTask();

            var joinedArgs = string.Join(" ", (arguments ?? new string[0]).Select(EscapeArgumentForCmd));
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
            var task = taskService.RootFolder.RegisterTaskDefinition(taskNameWithGroup, taskDefinition, TaskCreation.CreateOrUpdate, "SYSTEM", null, TaskLogonType.Password);

            return task;
        }

        private string GetTaskNameWithGroup(string taskName)
        {
            return string.Format("{{TaskGroup-{0}}}_{1}_{{TaskGroup-{0}}}", tasksGroup, taskName);
        }

        private string EscapeArgumentForCmd(string argument)
        {
            var r = new Regex(@"\\(?=[\\]*(""|$))");
            var semiEscaped = r.Replace(argument, @"\\");
            var result = string.Format("\"{0}\"", semiEscaped.Replace(@"""", @""""""));
            return result;
        }

        public void Dispose()
        {
            taskService.Dispose();
        }

        private const int taskStartTimeoutMilliseconds = 10000;

        private readonly TaskService taskService;
        private readonly ILog logger;
        private readonly RemoteMachineCredentials credentials;
        private readonly string tasksGroup;
    }
}