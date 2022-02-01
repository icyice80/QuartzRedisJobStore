using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Quartz;
using Quartz.Impl.Calendar;
using Quartz.Impl.Matchers;
using Quartz.Impl.Triggers;
using QuartzRedisJobStore.JobStore;

namespace QuartzRedisJobStore.UnitTest
{
    /// <summary>
    /// trigger related tests
    /// </summary>
    [TestClass]
    public class TriggerFixture : BaseFixture
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
        /// store a trigger
        /// </summary>
        [TestMethod]
        public async Task StoreTriggerSuccessfully()
        {
            //arrange
            var job = CreateJob();
            await JobStore.StoreJob(job, true);
            var trigger = CreateTrigger("tt", "tt", job.Key);
            var triggerHashKey = Schema.TriggerHashkey(trigger.Key);

            //act
            await JobStore.StoreTrigger(trigger, false);

            //assert
            var triggerProperties = await Db.HashGetAllAsync(triggerHashKey);
            Assert.IsNotNull(triggerProperties);
            var className = (string)
                (from hashEntry in triggerProperties
                 where hashEntry.Name == RedisJobStoreSchema.TriggerType
                 select hashEntry.Value).FirstOrDefault();
            Assert.AreEqual(RedisJobStoreSchema.TriggerTypeCron, className);
            Assert.IsTrue(await Db.SetContainsAsync(Schema.TriggersSetKey(), triggerHashKey));
            Assert.IsTrue(await Db.SetContainsAsync(Schema.TriggerGroupsSetKey(), Schema.TriggerGroupSetKey(trigger.Key.Group)));
            Assert.IsTrue(await Db.SetContainsAsync(Schema.TriggerGroupSetKey(trigger.Key.Group), triggerHashKey));
            Assert.IsTrue(await Db.SetContainsAsync(Schema.JobTriggersSetKey(trigger.JobKey), triggerHashKey));
        }

        /// <summary>
        /// try to store another trigger with the same name, set replacing to false, then 
        /// the original one will not be overriden.
        /// </summary>
        [TestMethod]
        public async Task StoreTrigger_WithoutReplacingExisting_NoOverride()
        {
            //arrange
            var job = CreateJob();
            await JobStore.StoreJob(job, true);
            var trigger = CreateTrigger("tt", "tt", job.Key);
            var trigger1 = CreateTrigger("tt", "tt", job.Key, "0 0 12 * * ?");

            //act
            await JobStore.StoreTrigger(trigger, false);
            try
            {
                await JobStore.StoreTrigger(trigger1, false);
            }
            catch { }

            //assert
            var retrievedTrigger = (CronTriggerImpl)(await JobStore.RetrieveTrigger(trigger.Key));
            Assert.AreEqual(retrievedTrigger.CronExpressionString, "0 0 0 * * ?");
        }

        /// <summary>
        /// try to store another trigger with the same name, set replacing to true, then 
        /// the original one will be overriden.
        /// </summary>
        [TestMethod]
        public async Task StoreTrigger_WithReplacingExisting_OverrideSuccessfully()
        {
            //arrange
            var job = CreateJob();
            await JobStore.StoreJob(job, true);
            var trigger = CreateTrigger("tt", "tt", job.Key);
            var trigger1 = CreateTrigger("tt", "tt", job.Key, "0 0 12 * * ?");

            //act
            await JobStore.StoreTrigger(trigger, true);
            await JobStore.StoreTrigger(trigger1, true);

            //assert
            var retrievedTrigger = (CronTriggerImpl)(await JobStore.RetrieveTrigger(trigger.Key));
            Assert.AreEqual(retrievedTrigger.CronExpressionString, "0 0 12 * * ?");
        }

        /// <summary>
        /// retrieve a trigger
        /// </summary>
        [TestMethod]
        public async Task RetrieveTriggerSuccessfully()
        {
            //arrange
            var job = CreateJob();
            await JobStore.StoreJob(job, true);
            var trigger = CreateTrigger("tt", "tt", job.Key);

            //act
            await JobStore.StoreTrigger(trigger, true);

            //assert
            var retrievedTrigger = (CronTriggerImpl)(await JobStore.RetrieveTrigger(trigger.Key));
            Assert.AreEqual(retrievedTrigger.CronExpressionString, "0 0 0 * * ?");
        }

        /// <summary>
        /// add a job and its two triggers, then delete a trigger, 
        /// check the job iteself still there and only that deleted trigger is removed. 
        /// </summary>
        [TestMethod]
        public async Task RemoveTrigger_RemoveOneOutOfTwo_Successfully()
        {
            //arrange
            var job = CreateJob();

            var trigger1 = CreateTrigger("trigger1", "triggerGroup", job.Key);
            var trigger2 = CreateTrigger("trigger2", "triggerGroup", job.Key);

            await JobStore.StoreJob(job, false);
            await JobStore.StoreTrigger(trigger1, false);
            await JobStore.StoreTrigger(trigger2, false);

            //act
            await JobStore.RemoveTrigger(trigger1.Key);

            //assert
            Assert.IsNull(await JobStore.RetrieveTrigger(trigger1.Key));
            Assert.IsNotNull(await JobStore.RetrieveJob(trigger1.JobKey));
        }

        /// <summary>
        /// add a job and its two triggers, then delete both, 
        /// check the job and triggers are removed since the job itself is not durable. 
        /// </summary>
        [TestMethod]
        public async Task RemoveTrigger_RemoveTwoOutOfTwo_Successfully()
        {
            //arrange
            var job = CreateJob();

            var trigger1 = CreateTrigger("trigger1", "triggerGroup", job.Key);
            var trigger2 = CreateTrigger("trigger2", "triggerGroup", job.Key);

            await JobStore.StoreJob(job, false);
            await JobStore.StoreTrigger(trigger1, false);
            await JobStore.StoreTrigger(trigger2, false);

            //act
            await JobStore.RemoveTrigger(trigger1.Key);
            await JobStore.RemoveTrigger(trigger2.Key);

            //assert
            Assert.IsNull(await JobStore.RetrieveTrigger(trigger1.Key));
            Assert.IsNull(await JobStore.RetrieveTrigger(trigger2.Key));
            Assert.IsNull(await JobStore.RetrieveJob(trigger1.JobKey));
        }

        /// <summary>
        /// add a job and its two triggers, then delete both, 
        /// check the job still there only triggers are removed since the job itself is durable. 
        /// </summary>
        [TestMethod]
        public async Task RemoveTriggerForDurableJob_RemoveTwoOutOfTwo_Successfully()
        {
            //arrange
            var job = JobBuilder.Create<TestJob>()
                          .WithIdentity("test", "test")
                          .WithDescription("description")
                          .StoreDurably(true)
                          .UsingJobData("testJob", "testJob")
                          .Build();

            var trigger1 = CreateTrigger("trigger1", "triggerGroup", job.Key);
            var trigger2 = CreateTrigger("trigger2", "triggerGroup", job.Key);

            await JobStore.StoreJob(job, false);
            await JobStore.StoreTrigger(trigger1, false);
            await JobStore.StoreTrigger(trigger2, false);

            //act
            await JobStore.RemoveTrigger(trigger1.Key);
            await JobStore.RemoveTrigger(trigger2.Key);

            //assert
            Assert.IsNull(await JobStore.RetrieveTrigger(trigger1.Key));
            Assert.IsNull(await JobStore.RetrieveTrigger(trigger2.Key));
            Assert.IsNotNull(await JobStore.RetrieveJob(trigger1.JobKey));
        }

        /// <summary>
        /// retrieve the triggers for the job
        /// </summary>
        [TestMethod]
        public async Task GetTriggersForJob_Successfully()
        {
            //arrange
            var job = CreateJob();

            var trigger1 = CreateTrigger("trigger1", "triggerGroup", job.Key);
            var trigger2 = CreateTrigger("trigger2", "triggerGroup", job.Key);

            await JobStore.StoreJob(job, false);
            await JobStore.StoreTrigger(trigger1, false);
            await JobStore.StoreTrigger(trigger2, false);

            //act
            var triggers = await JobStore.GetTriggersForJob(job.Key);

            //assert
            Assert.IsTrue(triggers.Count == 2);
        }

        /// <summary>
        /// get the total number of triggers in the store. 
        /// </summary>
        [TestMethod]
        public async Task GetNumberOfTriggers_Successfully()
        {
            //arrange
            var jobsAndTriggers = CreateJobsAndTriggers(2, 1, 1, 2);
            await JobStore.StoreJobsAndTriggers(jobsAndTriggers, false);

            //act
            var result = await JobStore.GetNumberOfTriggers();

            //assert
            Assert.AreEqual(result, 4);
        }

        /// <summary>
        /// get all the trigger which their group equals triggerGroup
        /// </summary>
        public async Task GetTriggerKeys_UseEqualOperator_Successfully()
        {
            //arrange
            var job = CreateJob();
            await JobStore.StoreJob(job, false);
            await JobStore.StoreTrigger(CreateTrigger("trigger1", "triggerGroup", job.Key), false);
            await JobStore.StoreTrigger(CreateTrigger("trigger2", "triggerGroup", job.Key), false);
            await JobStore.StoreTrigger(CreateTrigger("trigger3", "triggerGroup1", job.Key), false);
            await JobStore.StoreTrigger(CreateTrigger("trigger4", "triggerGroup1", job.Key), false);

            //act
            var triggerKeys = await JobStore.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("triggerGroup"));

            //assert
            Assert.IsTrue(triggerKeys.Count == 2);
        }

        /// <summary>
        /// get all the trigger which their group starts with triggerGroup
        /// </summary>
        [TestMethod]
        public async Task GetTriggerKeys_UseStartsWithOperator_Successfully()
        {
            //arrange
            var job = CreateJob();
            await JobStore.StoreJob(job, false);
            await JobStore.StoreTrigger(CreateTrigger("trigger1", "tGroup", job.Key), false);
            await JobStore.StoreTrigger(CreateTrigger("trigger2", "tGroup", job.Key), false);
            await JobStore.StoreTrigger(CreateTrigger("trigger3", "tGroup1", job.Key), false);
            await JobStore.StoreTrigger(CreateTrigger("trigger4", "tGroup1", job.Key), false);

            //act
            var triggerKeys = await JobStore.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupStartsWith("tGroup"));

            //assert
            Assert.IsTrue(triggerKeys.Count == 4);
        }

        /// <summary>
        /// get all the trigger which their group ends with 1
        /// </summary>
        [TestMethod]
        public async Task GetTriggerKeys_UseEndsWithOperator_Successfully()
        {
            //arrange
            var job = CreateJob();
            await JobStore.StoreJob(job, false);
            await JobStore.StoreTrigger(CreateTrigger("trigger1", "triggerGroup", job.Key), false);
            await JobStore.StoreTrigger(CreateTrigger("trigger2", "triggerGroup", job.Key), false);
            await JobStore.StoreTrigger(CreateTrigger("trigger3", "triggerGroup1", job.Key), false);
            await JobStore.StoreTrigger(CreateTrigger("trigger4", "triggerGroup1", job.Key), false);

            //act
            var triggerKeys = await JobStore.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEndsWith("1"));

            //assert
            Assert.IsTrue(triggerKeys.Count == 2);
        }

        /// <summary>
        /// get all the trigger which their group contains foobar
        /// </summary>
        [TestMethod]
        public async Task GetTriggerKeys_UseContainsOperator_Successfully()
        {
            //arrange
            var job = CreateJob();
            await JobStore.StoreJob(job, false);
            await JobStore.StoreTrigger(CreateTrigger("trigger1", "triggerGroup", job.Key), false);
            await JobStore.StoreTrigger(CreateTrigger("trigger2", "triggerGroup", job.Key), false);
            await JobStore.StoreTrigger(CreateTrigger("trigger3", "triggerfoobarGroup1", job.Key), false);
            await JobStore.StoreTrigger(CreateTrigger("trigger4", "triggerfoobarGroup1", job.Key), false);

            //act
            var triggerKeys = await JobStore.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupContains("foobar"));

            //assert
            Assert.IsTrue(triggerKeys.Count == 2);
        }

        /// <summary>
        /// get all the trigger group in the store
        /// </summary>
        [TestMethod]
        public async Task GetTriggerGroupNames_Successfully()
        {
            //arrange
            var jobsAndTriggers = CreateJobsAndTriggers(2, 1, 1, 2);
            await JobStore.StoreJobsAndTriggers(jobsAndTriggers, false);

            //act
            var result = await JobStore.GetTriggerGroupNames();

            //assert
            Assert.AreEqual(result.Count, 2);
        }

        /// <summary>
        /// check the newly saved trigger is in the normal state. 
        /// </summary>
        [TestMethod]
        public async Task GetTriggerState_NewlyStoredTrigger_NormalStateIsReturnedSuccessfully()
        {
            //arrange
            var job = CreateJob();
            await JobStore.StoreJob(job, false);
            var trigger = CreateTrigger("trigger1", "triggerGroup1", job.Key);

            //act
            await JobStore.StoreTrigger(trigger, false);

            //assert
            Assert.AreEqual(TriggerState.Normal, await JobStore.GetTriggerState(trigger.Key));
        }

        /// <summary>
        /// pause all the trigger which their group equals triggerfoobarGroup1
        /// </summary>
        [TestMethod]
        public async Task PauseTriggers_UseEqualOperator_Successfully()
        {
            //arrange
            var job = CreateJob();
            await JobStore.StoreJob(job, true);
            await JobStore.StoreTrigger(CreateTrigger("trigger1", "triggerGroup", job.Key), true);
            await JobStore.StoreTrigger(CreateTrigger("trigger2", "triggerGroup", job.Key), true);
            await JobStore.StoreTrigger(CreateTrigger("trigger3", "triggerfoobarGroup1", job.Key), true);
            await JobStore.StoreTrigger(CreateTrigger("trigger4", "triggerfoobarGroup1", job.Key), true);

            //act
            var pausedGroups = await JobStore.PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals("triggerfoobarGroup1"));

            //assert
            Assert.IsTrue(pausedGroups.Count == 1);

            Assert.AreEqual(TriggerState.Paused, await JobStore.GetTriggerState(new TriggerKey("trigger3", "triggerfoobarGroup1")));
            Assert.AreEqual(TriggerState.Paused, await JobStore.GetTriggerState(new TriggerKey("trigger4", "triggerfoobarGroup1")));
        }

        /// <summary>
        /// pause all the trigger which their group starts with triggerfooba
        /// </summary>
        [TestMethod]
        public async Task PauseTriggers_UseStartsWithOperator_Successfully()
        {
            //arrange
            var job = CreateJob();
            await JobStore.StoreJob(job, true);
            await JobStore.StoreTrigger(CreateTrigger("trigger1", "triggerGroup", job.Key), true);
            await JobStore.StoreTrigger(CreateTrigger("trigger2", "triggerGroup", job.Key), true);
            await JobStore.StoreTrigger(CreateTrigger("trigger3", "triggerfoobarGroup1", job.Key), true);
            await JobStore.StoreTrigger(CreateTrigger("trigger4", "triggerfoobarGroup1", job.Key), true);

            //act
            var pausedGroups = await JobStore.PauseTriggers(GroupMatcher<TriggerKey>.GroupStartsWith("triggerfooba"));

            //assert
            Assert.IsTrue(pausedGroups.Count == 1);

            Assert.AreEqual(TriggerState.Paused, await JobStore.GetTriggerState(new TriggerKey("trigger3", "triggerfoobarGroup1")));
            Assert.AreEqual(TriggerState.Paused, await JobStore.GetTriggerState(new TriggerKey("trigger4", "triggerfoobarGroup1")));
        }

        /// <summary>
        /// pause all the trigger which their group ends with 1
        /// </summary>
        [TestMethod]
        public async Task PauseTriggers_UseEndsWithOperator_Successfully()
        {
            //arrange
            var job = CreateJob();
            await JobStore.StoreJob(job, true);
            await JobStore.StoreTrigger(CreateTrigger("trigger1", "triggerGroup", job.Key), true);
            await JobStore.StoreTrigger(CreateTrigger("trigger2", "triggerGroup", job.Key), true);
            await JobStore.StoreTrigger(CreateTrigger("trigger3", "triggerfoobarGroup1", job.Key), true);
            await JobStore.StoreTrigger(CreateTrigger("trigger4", "triggerfoobarGroup1", job.Key), true);

            //act
            var pausedGroups = await JobStore.PauseTriggers(GroupMatcher<TriggerKey>.GroupEndsWith("1"));

            //assert
            Assert.IsTrue(pausedGroups.Count == 1);

            Assert.AreEqual(TriggerState.Paused, await JobStore.GetTriggerState(new TriggerKey("trigger3", "triggerfoobarGroup1")));
            Assert.AreEqual(TriggerState.Paused, await JobStore.GetTriggerState(new TriggerKey("trigger4", "triggerfoobarGroup1")));
        }

        /// <summary>
        /// pause all the trigger which their group contains foobar
        /// </summary>
        [TestMethod]
        public async Task PauseTriggers_UseContainsOperator_Successfully()
        {
            //arrange
            var job = CreateJob();
            await JobStore.StoreJob(job, true);
            await JobStore.StoreTrigger(CreateTrigger("trigger1", "triggerGroup", job.Key), true);
            await JobStore.StoreTrigger(CreateTrigger("trigger2", "triggerGroup", job.Key), true);
            await JobStore.StoreTrigger(CreateTrigger("trigger3", "triggerfoobarGroup1", job.Key), true);
            await JobStore.StoreTrigger(CreateTrigger("trigger4", "triggerfoobarGroup1", job.Key), true);

            //act
            var pausedGroups = await JobStore.PauseTriggers(GroupMatcher<TriggerKey>.GroupContains("foobar"));

            //assert
            Assert.IsTrue(pausedGroups.Count == 1);

            Assert.AreEqual(TriggerState.Paused, await JobStore.GetTriggerState(new TriggerKey("trigger3", "triggerfoobarGroup1")));
            Assert.AreEqual(TriggerState.Paused, await JobStore.GetTriggerState(new TriggerKey("trigger4", "triggerfoobarGroup1")));
        }

        /// <summary>
        /// resume a trigger
        /// </summary>
        [TestMethod]
        public async Task ResumeTriggerSuccessfully()
        {
            //arrange
            var job = CreateJob();
            await JobStore.StoreJob(job, false);
            var trigger = CreateTrigger("trigger1", "triggerGroup", job.Key);
            await JobStore.StoreTrigger(trigger, false);
            await JobStore.PauseTrigger(trigger.Key);

            //act
            await JobStore.ResumeTrigger(trigger.Key);

            //assert
            Assert.AreEqual(TriggerState.Normal, await JobStore.GetTriggerState(trigger.Key));
        }

        /// <summary>
        /// resume all the trigger which their group equals triggerGroup
        /// </summary>
        [TestMethod]
        public async Task ResumeTrigger_UseEqualOperator_Successfully()
        {
            //arrange
            var job = CreateJob();

            var trigger1 = CreateTrigger("trigger1", "triggerGroup", job.Key);
            var trigger2 = CreateTrigger("trigger2", "triggerGroup", job.Key);
            var trigger3 = CreateTrigger("trigger3", "triggerGroup1", job.Key);
            var trigger4 = CreateTrigger("trigger4", "triggerGroup1", job.Key);

            await JobStore.StoreJob(job, true);
            await JobStore.StoreTrigger(trigger1, true);
            await JobStore.StoreTrigger(trigger2, true);
            await JobStore.StoreTrigger(trigger3, true);
            await JobStore.StoreTrigger(trigger4, true);

            await JobStore.PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals("triggerGroup"));

            //act
            var resumedGroups = await JobStore.ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals("triggerGroup"));

            //assert
            Assert.IsTrue(resumedGroups.Count == 1);
            Assert.AreEqual(TriggerState.Normal, await JobStore.GetTriggerState(trigger1.Key));
            Assert.AreEqual(TriggerState.Normal, await JobStore.GetTriggerState(trigger2.Key));
        }

        /// <summary>
        /// resume all the trigger which their group starts with trigger1
        /// </summary>
        [TestMethod]
        public async Task ResumeTrigger_UseStartsWithOperator_Successfully()
        {
            //arrange
            var job = CreateJob();

            var trigger1 = CreateTrigger("trigger1", "trigger1Group", job.Key);
            var trigger2 = CreateTrigger("trigger2", "trigger1Group", job.Key);
            var trigger3 = CreateTrigger("trigger3", "triggerGroup1", job.Key);
            var trigger4 = CreateTrigger("trigger4", "triggerGroup1", job.Key);

            await JobStore.StoreJob(job, true);
            await JobStore.StoreTrigger(trigger1, true);
            await JobStore.StoreTrigger(trigger2, true);
            await JobStore.StoreTrigger(trigger3, true);
            await JobStore.StoreTrigger(trigger4, true);

            await JobStore.PauseTriggers(GroupMatcher<TriggerKey>.GroupStartsWith("trigger1"));

            //act
            var resumedGroups = await JobStore.ResumeTriggers(GroupMatcher<TriggerKey>.GroupStartsWith("trigger1"));

            //assert
            Assert.IsTrue(resumedGroups.Count == 1);
            Assert.AreEqual(TriggerState.Normal, await JobStore.GetTriggerState(trigger1.Key));
            Assert.AreEqual(TriggerState.Normal, await JobStore.GetTriggerState(trigger2.Key));
        }

        /// <summary>
        /// resume all the trigger which their group ends with Group1
        /// </summary>
        [TestMethod]
        public async Task ResumeTrigger_UseEndsWithOperator_Successfully()
        {
            //arrange
            var job = CreateJob();

            var trigger1 = CreateTrigger("trigger1", "triggerGroup", job.Key);
            var trigger2 = CreateTrigger("trigger2", "triggerGroup", job.Key);
            var trigger3 = CreateTrigger("trigger3", "triggerGroup1", job.Key);
            var trigger4 = CreateTrigger("trigger4", "triggerGroup1", job.Key);

            await JobStore.StoreJob(job, true);
            await JobStore.StoreTrigger(trigger1, true);
            await JobStore.StoreTrigger(trigger2, true);
            await JobStore.StoreTrigger(trigger3, true);
            await JobStore.StoreTrigger(trigger4, true);

            await JobStore.PauseTriggers(GroupMatcher<TriggerKey>.GroupEndsWith("Group1"));

            //act
            var resumedGroups = await JobStore.ResumeTriggers(GroupMatcher<TriggerKey>.GroupEndsWith("Group1"));

            //assert
            Assert.IsTrue(resumedGroups.Count == 1);
            Assert.AreEqual(TriggerState.Normal, await JobStore.GetTriggerState(trigger3.Key));
            Assert.AreEqual(TriggerState.Normal, await JobStore.GetTriggerState(trigger4.Key));
        }

        /// <summary>
        /// resume all the trigger which their group contains foobar
        /// </summary>
        [TestMethod]
        public async Task ResumeTrigger_UseContainsOperator_Successfully()
        {
            //arrange
            var job = CreateJob();

            var trigger1 = CreateTrigger("trigger1", "triggerGroup", job.Key);
            var trigger2 = CreateTrigger("trigger2", "triggerGroup", job.Key);
            var trigger3 = CreateTrigger("trigger3", "triggerfoobarGroup", job.Key);
            var trigger4 = CreateTrigger("trigger4", "triggerfoobarGroup", job.Key);

            await JobStore.StoreJob(job, true);
            await JobStore.StoreTrigger(trigger1, true);
            await JobStore.StoreTrigger(trigger2, true);
            await JobStore.StoreTrigger(trigger3, true);
            await JobStore.StoreTrigger(trigger4, true);

            await JobStore.PauseTriggers(GroupMatcher<TriggerKey>.GroupContains("foobar"));

            //act
            var resumedGroups = await JobStore.ResumeTriggers(GroupMatcher<TriggerKey>.GroupContains("foobar"));

            //assert
            Assert.IsTrue(resumedGroups.Count == 1);
            Assert.AreEqual(TriggerState.Normal, await JobStore.GetTriggerState(trigger3.Key));
            Assert.AreEqual(TriggerState.Normal, await JobStore.GetTriggerState(trigger4.Key));
        }

        /// <summary>
        /// get all the triggergroup whose state is in paused.
        /// </summary>
        [TestMethod]
        public async Task GetPausedTriggerGroupsSuccessfully()
        {
            //arrange
            var job = CreateJob();
            await JobStore.StoreJob(job, true);
            await JobStore.StoreTrigger(CreateTrigger("trigger1", "triggerGroup", job.Key), true);
            await JobStore.StoreTrigger(CreateTrigger("trigger2", "triggerGroup", job.Key), true);
            await JobStore.StoreTrigger(CreateTrigger("trigger3", "triggerfoobarGroup1", job.Key), true);
            await JobStore.StoreTrigger(CreateTrigger("trigger4", "triggerfoobarGroup2", job.Key), true);
            await JobStore.PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals("triggerGroup"));

            //act
            var pausedGroups = await JobStore.GetPausedTriggerGroups();

            //Assert
            Assert.IsTrue(pausedGroups.Count == 1);
        }

        /// <summary>
        /// Pause all the triggers in the store
        /// </summary>
        [TestMethod]
        public async Task PauseAllSuccessfully()
        {
            //arrange
            var jobsAndTriggers = CreateJobsAndTriggers(2, 2, 2, 2);
            await JobStore.StoreJobsAndTriggers(jobsAndTriggers, false);

            //act
            await JobStore.PauseAll();

            //assert
            foreach (var jobAndTriggers in jobsAndTriggers)
            {
                foreach (var trigger in jobAndTriggers.Value)
                {
                    Assert.AreEqual(TriggerState.Paused, await JobStore.GetTriggerState(trigger.Key));
                }
            }

        }

        /// <summary>
        /// Resume all the triggers in the store
        /// </summary>
        [TestMethod]
        public async Task ResumeAll_AfterPauseAll_Successfully()
        {
            //arrange
            var jobsAndTriggers = CreateJobsAndTriggers(2, 1, 1, 2);
            await JobStore.StoreJobsAndTriggers(jobsAndTriggers, false);
            await JobStore.PauseAll();

            //act
            await JobStore.ResumeAll();

            //assert
            foreach (var jobAndTriggers in jobsAndTriggers)
            {
                foreach (var trigger in jobAndTriggers.Value)
                {
                    Assert.AreEqual(TriggerState.Normal, await JobStore.GetTriggerState(trigger.Key));
                }
            }
        }

        /// <summary>
        /// when acquireNextTriggers is called, the triggers returned are in the acquired state.
        /// </summary>
        [TestMethod]
        public async Task AcquireNextTriggers_TriggersAreInAcquiredState_Successfully()
        {
            //arrange
            var calendarName = "testCalendar1";
            await JobStore.StoreCalendar(calendarName, new WeeklyCalendar(), true, true);
            var job = CreateJob();
            await JobStore.StoreJob(job, true);
            await JobStore.StoreTrigger(CreateTrigger("trigger1", "triggerGroup", job.Key, "* * * * * ?", calendarName), true);
            await JobStore.StoreTrigger(CreateTrigger("trigger2", "triggerGroup", job.Key, "* * * * * ?", calendarName), true);
            await JobStore.StoreTrigger(CreateTrigger("trigger3", "triggerfoobarGroup1", job.Key, "* * * * * ?", calendarName), true);
            await JobStore.StoreTrigger(CreateTrigger("trigger4", "triggerfoobarGroup2", job.Key, "* * * * * ?", calendarName), true);

            //act
            var acquiredTriggers = await JobStore.AcquireNextTriggers(new DateTimeOffset(DateTime.UtcNow), 20, TimeSpan.FromMilliseconds(1000));

            //assert
            Assert.IsTrue(acquiredTriggers.Count == 4);
            foreach (var trigger in acquiredTriggers)
            {
                Assert.AreEqual(TriggerState.Normal, await JobStore.GetTriggerState(trigger.Key));
                var triggerHashKey = Schema.TriggerHashkey(trigger.Key);
                Assert.IsTrue(Db.SortedSetScore(Schema.TriggerStateSetKey(RedisTriggerState.Acquired), triggerHashKey) > 0);
            }
        }

        /// <summary>
        /// Trigger Firing 
        /// </summary>
        [TestMethod]
        public async Task TriggerFiredSuccessfully()
        {
            //arrange
            var calendarName = "testCalendar10";
            await JobStore.StoreCalendar(calendarName, new WeeklyCalendar(), true, true);
            var job = CreateJob();
            await JobStore.StoreJob(job, true);
            await JobStore.StoreTrigger(CreateTrigger("trigger1", "triggerGroup", job.Key, "* * * * * ?", calendarName), true);
            await JobStore.StoreTrigger(CreateTrigger("trigger2", "triggerGroup", job.Key, "* * * * * ?", calendarName), true);
            await JobStore.StoreTrigger(CreateTrigger("trigger3", "triggerfoobarGroup1", job.Key, "* * * * * ?", calendarName), true);
            await JobStore.StoreTrigger(CreateTrigger("trigger4", "triggerfoobarGroup2", job.Key, "* * * * * ?", calendarName), true);
            var acquiredTriggers = await JobStore.AcquireNextTriggers(new DateTimeOffset(DateTime.UtcNow), 20, TimeSpan.FromMilliseconds(1000));

            //act
            var triggerFiredResult = await JobStore.TriggersFired(acquiredTriggers);

            //assert 
            Assert.IsTrue(triggerFiredResult.Count == 4);
        }

        /// <summary>
        /// during firing a trigger, need to add the job which does allow concurrent execution into blockedJobSet in the store.
        /// </summary>
        [TestMethod]
        public async Task TriggerFired_JobIsDisallowedConcurrent_AddToBlockedJobSet()
        {
            //arrange
            var job =
                JobBuilder.Create<NonConcurrentJob>()
                          .WithIdentity("testDisallowConcurrentJob", "JobGroup")
                          .WithDescription("I am a DisallowConcurrent job")
                          .Build();

            var trigger1 = CreateTrigger("trigger1", "triggerGroup1", job.Key, "* * * * * ?","testCalendar33");
            await JobStore.StoreCalendar("testCalendar33", new WeeklyCalendar(), true, true);
            await JobStore.StoreJob(job, false);
            await JobStore.StoreTrigger(trigger1, false);
            var jobHashKey = Schema.JobHashKey(job.Key);
            var acquiredTriggers = await JobStore.AcquireNextTriggers(new DateTimeOffset(DateTime.UtcNow), 20, TimeSpan.FromMilliseconds(1000));

            //act
            await JobStore.TriggersFired(acquiredTriggers);

            //assert    
            Assert.IsTrue(Db.SetContains(Schema.BlockedJobsSet(), jobHashKey));
        }

        /// <summary>
        /// could not replace trigger, if the triggers are different in group.
        /// </summary>
        [TestMethod]
        public async Task ReplaceTrigger_WithDifferentNameAndGroup_Unsuccessfully()
        {
            //arrange
            var job = CreateJob();
            var trigger = CreateTrigger("testTrigger", "testTriggerGroup", job.Key);
            await JobStore.StoreJob(job, false);
            await JobStore.StoreTrigger(trigger, false);

            //act
            var result = await JobStore.ReplaceTrigger(new TriggerKey("foo", "bar"), trigger);

            //assert
            Assert.IsFalse(result);
        }

        /// <summary>
        /// when the old trigger is replaced, then it should be removed from store as well. 
        /// </summary>
        [TestMethod]
        public async Task ReplaceTrigger_OldTriggerRemoved_Successfully()
        {
            //arrange
            var job = CreateJob();
            var trigger1 = CreateTrigger("trigger1", "triggerGroup", job.Key);
            var trigger2 = CreateTrigger("trigger2", "triggerGroup", job.Key);
            await JobStore.StoreJob(job, false);
            await JobStore.StoreTrigger(trigger1, false);
            await JobStore.StoreTrigger(trigger2, false);
            var newTrigger = CreateTrigger("newTrigger", "triggerGroup", job.Key);

            //act
            var result = await JobStore.ReplaceTrigger(trigger1.Key, newTrigger);

            //assert
            Assert.IsTrue(result);
            Assert.IsNull(await JobStore.RetrieveTrigger(trigger1.Key));
        }

        /// <summary>
        /// when the old trigger is replaced, then the new trigger is saved in the store.
        /// </summary>
        [TestMethod]
        public async Task ReplaceTrigger_NewTriggerAdded_Successfully()
        {
            //arrange
            var job = CreateJob();
            var trigger1 = CreateTrigger("trigger1", "triggerGroup", job.Key);
            var trigger2 = CreateTrigger("trigger2", "triggerGroup", job.Key);
            await JobStore.StoreJob(job, false);
            await JobStore.StoreTrigger(trigger1, false);
            await JobStore.StoreTrigger(trigger2, false);
            var newTrigger = CreateTrigger("newTrigger", "triggerGroup", job.Key);

            //act
            await JobStore.ReplaceTrigger(trigger1.Key, newTrigger);

            //assert
            Assert.IsTrue((await JobStore.GetTriggersForJob(job.Key)).Count == 2);
            Assert.IsTrue((await JobStore.GetTriggersForJob(job.Key)).Select(x => x.Key.Equals(newTrigger.Key)).Any());
        }

        /// <summary>
        /// when a non durable job has a trigger, that trigger is replaced by a new trigger. the job will not be removed.
        /// </summary>
        [TestMethod]
        public async Task RepaceTrigger_ForNonDurableJobWithSingleTrigger_JobStillExistsAfterTriggerReplacement()
        {
            //arrange
            var job = CreateJob("job11");
            var trigger1 = CreateTrigger("trigger3", "triGroup", job.Key);
            await JobStore.StoreJob(job, false);
            await JobStore.StoreTrigger(trigger1, false);
            var newTrigger = CreateTrigger("newTrigger", "triGroup", job.Key);

            //act
            await JobStore.ReplaceTrigger(trigger1.Key, newTrigger);
            var result = await JobStore.RetrieveJob(job.Key);

            //assert
            Assert.IsNotNull(result);
        }

        /// <summary>
        /// coulnt replace trigger if its for a dffernt job.
        /// </summary>
        [TestMethod, ExpectedException(typeof(JobPersistenceException))]
        public async Task ReplaceTrigger_WithDifferentJob_Unsuccessfully()
        {
            //arrange
            var job = CreateJob();
            var trigger1 = CreateTrigger("trigger1", "triggerGroup", job.Key);
            await JobStore.StoreJob(job, false);
            await JobStore.StoreTrigger(trigger1, false);
            var newTrigger = CreateTrigger("newTrigger", "triggerGroup", new JobKey("foo", "bar"));

            //act
            await JobStore.ReplaceTrigger(trigger1.Key, newTrigger);
        }
    }
}
