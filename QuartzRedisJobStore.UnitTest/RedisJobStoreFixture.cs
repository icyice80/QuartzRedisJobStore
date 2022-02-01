using System;
using System.Threading.Tasks;
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
            JobStore?.ClearAllSchedulingData().Wait();
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
        public async Task StartScheduler_WithJobsAndTriggers_SavedSuccessfully()
        {
            //arrange
            ISchedulerFactory sf = new StdSchedulerFactory();
            IScheduler sched = await sf.GetScheduler();
            var job = CreateJob();
            var trigger = CreateTrigger("testTrigger1", "triggerGroup", job.Key, "0/5 * * * * ?");
            var calendar = CreateCalendar();

            //act
            await sched.AddCalendar("testCalendar", calendar, false, false);
            await sched.ScheduleJob(job, trigger);
            await sched.Start();
            await sched.Shutdown();
            //assert
            Assert.IsNotNull(await JobStore.RetrieveJob(job.Key));
            Assert.IsNotNull(await JobStore.RetrieveTrigger(trigger.Key));
        }

        /// <summary>
        /// start a schedular, storejob and trigger then delete the job
        /// check job and its trigger are deleted from the store.
        /// </summary>
        [TestMethod]
        public async Task StartScheduler_WithJobsAndTriggers_DeletedSuccessfully()
        {
            //arrange
            ISchedulerFactory sf = new StdSchedulerFactory();
            IScheduler sched = await sf.GetScheduler();
            var job = CreateJob();
            var trigger = CreateTrigger("testTrigger1", "triggerGroup", job.Key, "0/5 * * * * ?");
            var calendar = CreateCalendar();
            await sched.AddCalendar("testCalendar", calendar, false, false);
            await sched.ScheduleJob(job, trigger);
            await sched.Start();

            //act
            await sched.DeleteJob(job.Key);
            await sched.Shutdown();

            //assert
            Assert.IsNull(await JobStore.RetrieveJob(job.Key));
            Assert.IsNull(await JobStore.RetrieveTrigger(trigger.Key));

        }
    }
}
