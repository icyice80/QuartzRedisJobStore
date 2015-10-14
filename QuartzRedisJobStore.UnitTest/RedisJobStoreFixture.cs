using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Quartz;
using Quartz.Impl;

namespace QuartzRedisJobStore.UnitTest
{
    /// <summary>
    /// redis job store related tests.
    /// </summary>
    [TestClass]
    public class RedisJobStoreFixture : BaseFixture
    {
        /// <summary>
        /// Clean up after the test completes
        /// </summary>
        [TestCleanup]
        public void ClearAllJobStoreData()
        {
            System.Diagnostics.Debug.Write("here");
            JobStore?.ClearAllSchedulingData();
            System.Diagnostics.Debug.Write(counter++);
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            CleanUp();
        }

        /// <summary>
        /// start a schedular, storejob and trigger
        /// check job and trigger are actually saved.
        /// </summary>
        [TestMethod]
        public void StartScheduler_WithJobsAndTriggers_SavedSuccessfully()
        {
            //arrange
            ISchedulerFactory sf = new StdSchedulerFactory();
            IScheduler sched = sf.GetScheduler();
            var job = CreateJob();
            var trigger = CreateTrigger("testTrigger1", "triggerGroup", job.Key, "0/5 * * * * ?");
            var calendar = CreateCalendar();

            //act
            sched.AddCalendar("testCalendar", calendar, false, false);
            sched.ScheduleJob(job, trigger);
            sched.Start();
            sched.Shutdown();
            //assert
            Assert.IsNotNull(JobStore.RetrieveJob(job.Key));
            Assert.IsNotNull(JobStore.RetrieveTrigger(trigger.Key));
        }

        /// <summary>
        /// start a schedular, storejob and trigger then delete the job
        /// check job and its trigger are deleted from the store.
        /// </summary>
        [TestMethod]
        public void StartScheduler_WithJobsAndTriggers_DeletedSuccessfully()
        {
            //arrange
            ISchedulerFactory sf = new StdSchedulerFactory();
            IScheduler sched = sf.GetScheduler();
            var job = CreateJob();
            var trigger = CreateTrigger("testTrigger1", "triggerGroup", job.Key, "0/5 * * * * ?");
            var calendar = CreateCalendar();
            sched.AddCalendar("testCalendar", calendar, false, false);
            sched.ScheduleJob(job, trigger);
            sched.Start();

            //act
            sched.DeleteJob(job.Key);
            sched.Shutdown();

            //assert
            Assert.IsNull(JobStore.RetrieveJob(job.Key));
            Assert.IsNull(JobStore.RetrieveTrigger(trigger.Key));

        }
    }
}
