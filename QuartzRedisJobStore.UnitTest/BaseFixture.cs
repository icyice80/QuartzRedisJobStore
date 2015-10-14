using System;
using System.Collections.Generic;
using System.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Quartz;
using Quartz.Impl.Calendar;
using Quartz.Impl.Triggers;
using Quartz.Spi;
using QuartzRedisJobStore.JobStore;
using StackExchange.Redis;

namespace QuartzRedisJobStore.UnitTest
{
    /// <summary>
    /// base test fixture 
    /// </summary>
    public abstract class BaseFixture {
        /// <summary>
        /// RedisJobStore
        /// </summary>
        protected static RedisJobStore JobStore;
        /// <summary>
        /// RedisJobStoreSchema
        /// </summary>
        protected static RedisJobStoreSchema Schema;
        protected static int counter = 1;
        /// <summary>
        /// KeyPrefix
        /// </summary>
        private const string KeyPrefix = "UnitTest1";
        /// <summary>
        /// IDatabase
        /// </summary>
        protected static IDatabase Db;

        /// <summary>
        /// ISchedulerSignaler
        /// </summary>
        protected static Mock<ISchedulerSignaler> MockedSignaler;

        /// <summary>
        /// JsonSerializerSettings
        /// </summary>
        protected readonly JsonSerializerSettings _serializerSettings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.All, DateTimeZoneHandling = DateTimeZoneHandling.Utc, NullValueHandling = NullValueHandling.Ignore, ContractResolver = new CamelCasePropertyNamesContractResolver() };

        /// <summary>
        /// constructor
        /// </summary>
        protected BaseFixture() {
            InitializeJobStore();
        }

        /// <summary>
        /// initialize the job store 
        /// </summary>
        private static void InitializeJobStore()
        {
            JobStore = new RedisJobStore
            {
                RedisConfiguration = ConfigurationManager.AppSettings["RedisConfiguration"],
                KeyPrefix = KeyPrefix,
                InstanceId = "UnitTestInstanceId"
            };
            MockedSignaler = new Mock<ISchedulerSignaler>();
            MockedSignaler.Setup(x => x.NotifySchedulerListenersJobDeleted(null));
            MockedSignaler.Setup(x => x.SignalSchedulingChange(null));
            JobStore.Initialize(null, MockedSignaler.Object);
            Schema = new RedisJobStoreSchema(KeyPrefix);
            Db = ConnectionMultiplexer.Connect(JobStore.RedisConfiguration).GetDatabase();
        }

        /// <summary>
        /// flush redis db.
        /// </summary>
        public static void CleanUp()
        {
            var endpoints = Db?.Multiplexer.GetEndPoints();

            if (endpoints != null)
            {
                foreach (var endpoint in endpoints)
                {
                    Db?.Multiplexer.GetServer(endpoint).FlushDatabase();
                }
            }
        }



        /// <summary>
        /// create a dummy job
        /// </summary>
        /// <param name="name">Name</param>
        /// <param name="group">Group</param>
        /// <param name="description">Description</param>
        /// <returns>IJobDetail</returns>
        protected static IJobDetail CreateJob(string name = "testJob", string group = "testGroup", string description = "JobTesting")
        {
            return
                JobBuilder.Create<TestJob>()
                          .WithIdentity(name, group)
                          .WithDescription(description)
                          .UsingJobData("testJob", "testJob")
                          .Build();
        }

        /// <summary>
        /// Create a dummy Trigger
        /// </summary>
        /// <param name="name">Name</param>
        /// <param name="group">Group</param>
        /// <param name="jobKey">JobKey</param>
        /// <param name="cronExpression">unix cron expression</param>
        /// <param name="calendarName">unix cron expression</param>
        /// <returns>IOperableTrigger</returns>
        protected static IOperableTrigger CreateTrigger(string name, string group, JobKey jobKey, string cronExpression = "0 0 0 * * ?", string calendarName = "testCalendar")
        {
            var trigger =
                TriggerBuilder.Create()
                              .ForJob(jobKey)
                              .WithIdentity(name, group)
                              .WithSchedule(CronScheduleBuilder.CronSchedule(cronExpression))
                              .UsingJobData("testTrigger", "testTrigger")
                              .WithDescription("TriggerTesting")
                              .Build();

            var abstractTrigger = (AbstractTrigger)trigger;

            if (abstractTrigger != null)
            {
                var calendar = new WeeklyCalendar { DaysExcluded = null };
                abstractTrigger.ComputeFirstFireTimeUtc(calendar);
                abstractTrigger.CalendarName = calendarName;
            }

            return abstractTrigger ?? (IOperableTrigger)trigger;
        }

        /// <summary>
        /// create a dummy calendar
        /// </summary>
        /// <param name="description">Description</param>
        /// <returns>ICalendar</returns>
        protected static ICalendar CreateCalendar(string description="week days only") {
            var calendar = new WeeklyCalendar();

            calendar.SetDayExcluded(DayOfWeek.Saturday, true);
            calendar.SetDayExcluded(DayOfWeek.Sunday,true);

            calendar.Description = description;

            return calendar;
        }

        /// <summary>
        /// StoreJob and its related Triggers
        /// </summary>
        /// <param name="job">IJobDetail</param>
        /// <param name="triggers">Triggers</param>
        protected void StoreJobAndTriggers(IJobDetail job,global::Quartz.Collection.ISet<ITrigger> triggers) {
            var dictionary = new Dictionary<IJobDetail, global::Quartz.Collection.ISet<ITrigger>> {{job, triggers}};

            JobStore.StoreJobsAndTriggers(dictionary,true);
        }

        /// <summary>
        /// create jobs and triggers
        /// </summary>
        /// <param name="jobGroups">Number of JobGroup</param>
        /// <param name="jobsPerGroup">Number of jobs per Group</param>
        /// <param name="triggerGroupsPerJob">number of Trigger Group per Job</param>
        /// <param name="triggersPerGroup">number of triggers per group</param>
        /// <param name="cronExpression">unix cron expression</param>
        /// <returns>jobs and triggers</returns>
        protected static IDictionary<IJobDetail, global::Quartz.Collection.ISet<ITrigger>> CreateJobsAndTriggers(int jobGroups, int jobsPerGroup, int triggerGroupsPerJob,
                                             int triggersPerGroup, string cronExpression = "")
        {

            var jobsAndTriggers = new Dictionary<IJobDetail, global::Quartz.Collection.ISet<ITrigger>>();

            for (int g = 0; g < jobGroups; g++)
            {
                var jobGroup = "jobGroup_" + g;
                for (int j = 0; j < jobsPerGroup; j++)
                {
                    var jobName = "jobName_" + j;
                    var job = CreateJob(jobName, jobGroup);
                    var triggerSet = new global::Quartz.Collection.HashSet<ITrigger>();
                    for (int tg = 0; tg < triggerGroupsPerJob; tg++)
                    {
                        var triggerGroup = "triggerGroup_" + tg + "_" + j + g;

                        for (int t = 0; t < triggersPerGroup; t++)
                        {
                            var triggerName = "trigger_" + t;
                            if (string.IsNullOrEmpty(cronExpression))
                            {
                                triggerSet.Add(CreateTrigger(triggerName, triggerGroup, job.Key));
                            }
                            else
                            {
                                triggerSet.Add(CreateTrigger(triggerName, triggerGroup, job.Key, cronExpression));
                            }
                        }
                    }
                    jobsAndTriggers.Add(job, triggerSet);
                }
            }

            return jobsAndTriggers;

        }
    }
}
