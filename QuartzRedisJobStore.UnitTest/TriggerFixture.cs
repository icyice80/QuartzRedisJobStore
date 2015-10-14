using System;
using System.Linq;
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
            JobStore?.ClearAllSchedulingData();
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
        public void StoreTriggerSuccessfully()
        {
            //arrange
            var job = CreateJob();
            JobStore.StoreJob(job, true);
            var trigger = CreateTrigger("tt", "tt", job.Key);
            var triggerHashKey = Schema.TriggerHashkey(trigger.Key);

            //act
            JobStore.StoreTrigger(trigger, false);

            //assert
            var triggerProperties = Db.HashGetAll(triggerHashKey);
            Assert.IsNotNull(triggerProperties);
            var className =
                (from hashEntry in triggerProperties
                 where hashEntry.Name == RedisJobStoreSchema.TriggerType
                 select hashEntry.Value).FirstOrDefault();
            Assert.AreEqual(className, RedisJobStoreSchema.TriggerTypeCron);
            Assert.IsTrue(Db.SetContains(Schema.TriggersSetKey(), triggerHashKey));
            Assert.IsTrue(Db.SetContains(Schema.TriggerGroupsSetKey(), Schema.TriggerGroupSetKey(trigger.Key.Group)));
            Assert.IsTrue(Db.SetContains(Schema.TriggerGroupSetKey(trigger.Key.Group), triggerHashKey));
            Assert.IsTrue(Db.SetContains(Schema.JobTriggersSetKey(trigger.JobKey), triggerHashKey));
        }

        /// <summary>
        /// try to store another trigger with the same name, set replacing to false, then 
        /// the original one will not be overriden.
        /// </summary>
        [TestMethod]
        public void StoreTrigger_WithoutReplacingExisting_NoOverride()
        {
            //arrange
            var job = CreateJob();
            JobStore.StoreJob(job, true);
            var trigger = CreateTrigger("tt", "tt", job.Key);
            var trigger1 = CreateTrigger("tt", "tt", job.Key, "0 0 12 * * ?");

            //act
            JobStore.StoreTrigger(trigger, false);
            JobStore.StoreTrigger(trigger1, false);

            //assert
            var retrievedTrigger = (CronTriggerImpl)JobStore.RetrieveTrigger(trigger.Key);
            Assert.AreEqual(retrievedTrigger.CronExpressionString, "0 0 0 * * ?");
        }

        /// <summary>
        /// try to store another trigger with the same name, set replacing to true, then 
        /// the original one will be overriden.
        /// </summary>
        [TestMethod]
        public void StoreTrigger_WithReplacingExisting_OverrideSuccessfully()
        {
            //arrange
            var job = CreateJob();
            JobStore.StoreJob(job, true);
            var trigger = CreateTrigger("tt", "tt", job.Key);
            var trigger1 = CreateTrigger("tt", "tt", job.Key, "0 0 12 * * ?");

            //act
            JobStore.StoreTrigger(trigger, true);
            JobStore.StoreTrigger(trigger1, true);

            //assert
            var retrievedTrigger = (CronTriggerImpl)JobStore.RetrieveTrigger(trigger.Key);
            Assert.AreEqual(retrievedTrigger.CronExpressionString, "0 0 12 * * ?");
        }

        /// <summary>
        /// retrieve a trigger
        /// </summary>
        [TestMethod]
        public void RetrieveTriggerSuccessfully()
        {
            //arrange
            var job = CreateJob();
            JobStore.StoreJob(job, true);
            var trigger = CreateTrigger("tt", "tt", job.Key);

            //act
            JobStore.StoreTrigger(trigger, true);

            //assert
            var retrievedTrigger = (CronTriggerImpl)JobStore.RetrieveTrigger(trigger.Key);
            Assert.AreEqual(retrievedTrigger.CronExpressionString, "0 0 0 * * ?");
        }

        /// <summary>
        /// add a job and its two triggers, then delete a trigger, 
        /// check the job iteself still there and only that deleted trigger is removed. 
        /// </summary>
        [TestMethod]
        public void RemoveTrigger_RemoveOneOutOfTwo_Successfully()
        {
            //arrange
            var job = CreateJob();

            var trigger1 = CreateTrigger("trigger1", "triggerGroup", job.Key);
            var trigger2 = CreateTrigger("trigger2", "triggerGroup", job.Key);

            JobStore.StoreJob(job, false);
            JobStore.StoreTrigger(trigger1, false);
            JobStore.StoreTrigger(trigger2, false);

            //act
            JobStore.RemoveTrigger(trigger1.Key);

            //assert
            Assert.IsNull(JobStore.RetrieveTrigger(trigger1.Key));
            Assert.IsNotNull(JobStore.RetrieveJob(trigger1.JobKey));
        }

        /// <summary>
        /// add a job and its two triggers, then delete both, 
        /// check the job and triggers are removed since the job itself is not durable. 
        /// </summary>
        [TestMethod]
        public void RemoveTrigger_RemoveTwoOutOfTwo_Successfully()
        {
            //arrange
            var job = CreateJob();

            var trigger1 = CreateTrigger("trigger1", "triggerGroup", job.Key);
            var trigger2 = CreateTrigger("trigger2", "triggerGroup", job.Key);

            JobStore.StoreJob(job, false);
            JobStore.StoreTrigger(trigger1, false);
            JobStore.StoreTrigger(trigger2, false);

            //act
            JobStore.RemoveTrigger(trigger1.Key);
            JobStore.RemoveTrigger(trigger2.Key);

            //assert
            Assert.IsNull(JobStore.RetrieveTrigger(trigger1.Key));
            Assert.IsNull(JobStore.RetrieveTrigger(trigger2.Key));
            Assert.IsNull(JobStore.RetrieveJob(trigger1.JobKey));
        }

        /// <summary>
        /// add a job and its two triggers, then delete both, 
        /// check the job still there only triggers are removed since the job itself is durable. 
        /// </summary>
        [TestMethod]
        public void RemoveTriggerForDurableJob_RemoveTwoOutOfTwo_Successfully()
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

            JobStore.StoreJob(job, false);
            JobStore.StoreTrigger(trigger1, false);
            JobStore.StoreTrigger(trigger2, false);

            //act
            JobStore.RemoveTrigger(trigger1.Key);
            JobStore.RemoveTrigger(trigger2.Key);

            //assert
            Assert.IsNull(JobStore.RetrieveTrigger(trigger1.Key));
            Assert.IsNull(JobStore.RetrieveTrigger(trigger2.Key));
            Assert.IsNotNull(JobStore.RetrieveJob(trigger1.JobKey));
        }

        /// <summary>
        /// retrieve the triggers for the job
        /// </summary>
        [TestMethod]
        public void GetTriggersForJob_Successfully()
        {
            //arrange
            var job = CreateJob();

            var trigger1 = CreateTrigger("trigger1", "triggerGroup", job.Key);
            var trigger2 = CreateTrigger("trigger2", "triggerGroup", job.Key);

            JobStore.StoreJob(job, false);
            JobStore.StoreTrigger(trigger1, false);
            JobStore.StoreTrigger(trigger2, false);

            //act
            var triggers = JobStore.GetTriggersForJob(job.Key);

            //assert
            Assert.IsTrue(triggers.Count == 2);
        }

        /// <summary>
        /// get the total number of triggers in the store. 
        /// </summary>
        [TestMethod]
        public void GetNumberOfTriggers_Successfully()
        {
            //arrange
            var jobsAndTriggers = CreateJobsAndTriggers(2, 1, 1, 2);
            JobStore.StoreJobsAndTriggers(jobsAndTriggers, false);

            //act
            var result = JobStore.GetNumberOfTriggers();

            //assert
            Assert.AreEqual(result, 4);
        }

        /// <summary>
        /// get all the trigger which their group equals triggerGroup
        /// </summary>
        public void GetTriggerKeys_UseEqualOperator_Successfully()
        {
            //arrange
            var job = CreateJob();
            JobStore.StoreJob(job, false);
            JobStore.StoreTrigger(CreateTrigger("trigger1", "triggerGroup", job.Key), false);
            JobStore.StoreTrigger(CreateTrigger("trigger2", "triggerGroup", job.Key), false);
            JobStore.StoreTrigger(CreateTrigger("trigger3", "triggerGroup1", job.Key), false);
            JobStore.StoreTrigger(CreateTrigger("trigger4", "triggerGroup1", job.Key), false);

            //act
            var triggerKeys = JobStore.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("triggerGroup"));

            //assert
            Assert.IsTrue(triggerKeys.Count == 2);
        }

        /// <summary>
        /// get all the trigger which their group starts with triggerGroup
        /// </summary>
        [TestMethod]
        public void GetTriggerKeys_UseStartsWithOperator_Successfully()
        {
            //arrange
            var job = CreateJob();
            JobStore.StoreJob(job, false);
            JobStore.StoreTrigger(CreateTrigger("trigger1", "tGroup", job.Key), false);
            JobStore.StoreTrigger(CreateTrigger("trigger2", "tGroup", job.Key), false);
            JobStore.StoreTrigger(CreateTrigger("trigger3", "tGroup1", job.Key), false);
            JobStore.StoreTrigger(CreateTrigger("trigger4", "tGroup1", job.Key), false);

            //act
            var triggerKeys = JobStore.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupStartsWith("tGroup"));

            //assert
            Assert.IsTrue(triggerKeys.Count == 4);
        }

        /// <summary>
        /// get all the trigger which their group ends with 1
        /// </summary>
        [TestMethod]
        public void GetTriggerKeys_UseEndsWithOperator_Successfully()
        {
            //arrange
            var job = CreateJob();
            JobStore.StoreJob(job, false);
            JobStore.StoreTrigger(CreateTrigger("trigger1", "triggerGroup", job.Key), false);
            JobStore.StoreTrigger(CreateTrigger("trigger2", "triggerGroup", job.Key), false);
            JobStore.StoreTrigger(CreateTrigger("trigger3", "triggerGroup1", job.Key), false);
            JobStore.StoreTrigger(CreateTrigger("trigger4", "triggerGroup1", job.Key), false);

            //act
            var triggerKeys = JobStore.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEndsWith("1"));

            //assert
            Assert.IsTrue(triggerKeys.Count == 2);
        }

        /// <summary>
        /// get all the trigger which their group contains foobar
        /// </summary>
        [TestMethod]
        public void GetTriggerKeys_UseContainsOperator_Successfully()
        {
            //arrange
            var job = CreateJob();
            JobStore.StoreJob(job, false);
            JobStore.StoreTrigger(CreateTrigger("trigger1", "triggerGroup", job.Key), false);
            JobStore.StoreTrigger(CreateTrigger("trigger2", "triggerGroup", job.Key), false);
            JobStore.StoreTrigger(CreateTrigger("trigger3", "triggerfoobarGroup1", job.Key), false);
            JobStore.StoreTrigger(CreateTrigger("trigger4", "triggerfoobarGroup1", job.Key), false);

            //act
            var triggerKeys = JobStore.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupContains("foobar"));

            //assert
            Assert.IsTrue(triggerKeys.Count == 2);
        }

        /// <summary>
        /// get all the trigger group in the store
        /// </summary>
        [TestMethod]
        public void GetTriggerGroupNames_Successfully()
        {
            //arrange
            var jobsAndTriggers = CreateJobsAndTriggers(2, 1, 1, 2);
            JobStore.StoreJobsAndTriggers(jobsAndTriggers, false);

            //act
            var result = JobStore.GetTriggerGroupNames();

            //assert
            Assert.AreEqual(result.Count, 2);
        }

        /// <summary>
        /// check the newly saved trigger is in the normal state. 
        /// </summary>
        [TestMethod]
        public void GetTriggerState_NewlyStoredTrigger_NormalStateIsReturnedSuccessfully()
        {
            //arrange
            var job = CreateJob();
            var trigger = CreateTrigger("trigger1", "triggerGroup1", job.Key);

            //act
            JobStore.StoreTrigger(trigger, false);

            //assert
            Assert.AreEqual(TriggerState.Normal, JobStore.GetTriggerState(trigger.Key));
        }

        /// <summary>
        /// pause all the trigger which their group equals triggerfoobarGroup1
        /// </summary>
        [TestMethod]
        public void PauseTriggers_UseEqualOperator_Successfully()
        {
            //arrange
            var job = CreateJob();
            JobStore.StoreJob(job, true);
            JobStore.StoreTrigger(CreateTrigger("trigger1", "triggerGroup", job.Key), true);
            JobStore.StoreTrigger(CreateTrigger("trigger2", "triggerGroup", job.Key), true);
            JobStore.StoreTrigger(CreateTrigger("trigger3", "triggerfoobarGroup1", job.Key), true);
            JobStore.StoreTrigger(CreateTrigger("trigger4", "triggerfoobarGroup1", job.Key), true);

            //act
            var pausedGroups = JobStore.PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals("triggerfoobarGroup1"));

            //assert
            Assert.IsTrue(pausedGroups.Count == 1);

            Assert.AreEqual(TriggerState.Paused, JobStore.GetTriggerState(new TriggerKey("trigger3", "triggerfoobarGroup1")));
            Assert.AreEqual(TriggerState.Paused, JobStore.GetTriggerState(new TriggerKey("trigger4", "triggerfoobarGroup1")));
        }

        /// <summary>
        /// pause all the trigger which their group starts with triggerfooba
        /// </summary>
        [TestMethod]
        public void PauseTriggers_UseStartsWithOperator_Successfully()
        {
            //arrange
            var job = CreateJob();
            JobStore.StoreJob(job, true);
            JobStore.StoreTrigger(CreateTrigger("trigger1", "triggerGroup", job.Key), true);
            JobStore.StoreTrigger(CreateTrigger("trigger2", "triggerGroup", job.Key), true);
            JobStore.StoreTrigger(CreateTrigger("trigger3", "triggerfoobarGroup1", job.Key), true);
            JobStore.StoreTrigger(CreateTrigger("trigger4", "triggerfoobarGroup1", job.Key), true);

            //act
            var pausedGroups = JobStore.PauseTriggers(GroupMatcher<TriggerKey>.GroupStartsWith("triggerfooba"));

            //assert
            Assert.IsTrue(pausedGroups.Count == 1);

            Assert.AreEqual(TriggerState.Paused, JobStore.GetTriggerState(new TriggerKey("trigger3", "triggerfoobarGroup1")));
            Assert.AreEqual(TriggerState.Paused, JobStore.GetTriggerState(new TriggerKey("trigger4", "triggerfoobarGroup1")));
        }

        /// <summary>
        /// pause all the trigger which their group ends with 1
        /// </summary>
        [TestMethod]
        public void PauseTriggers_UseEndsWithOperator_Successfully()
        {
            //arrange
            var job = CreateJob();
            JobStore.StoreJob(job, true);
            JobStore.StoreTrigger(CreateTrigger("trigger1", "triggerGroup", job.Key), true);
            JobStore.StoreTrigger(CreateTrigger("trigger2", "triggerGroup", job.Key), true);
            JobStore.StoreTrigger(CreateTrigger("trigger3", "triggerfoobarGroup1", job.Key), true);
            JobStore.StoreTrigger(CreateTrigger("trigger4", "triggerfoobarGroup1", job.Key), true);

            //act
            var pausedGroups = JobStore.PauseTriggers(GroupMatcher<TriggerKey>.GroupEndsWith("1"));

            //assert
            Assert.IsTrue(pausedGroups.Count == 1);

            Assert.AreEqual(TriggerState.Paused, JobStore.GetTriggerState(new TriggerKey("trigger3", "triggerfoobarGroup1")));
            Assert.AreEqual(TriggerState.Paused, JobStore.GetTriggerState(new TriggerKey("trigger4", "triggerfoobarGroup1")));
        }

        /// <summary>
        /// pause all the trigger which their group contains foobar
        /// </summary>
        [TestMethod]
        public void PauseTriggers_UseContainsOperator_Successfully()
        {
            //arrange
            var job = CreateJob();
            JobStore.StoreJob(job, true);
            JobStore.StoreTrigger(CreateTrigger("trigger1", "triggerGroup", job.Key), true);
            JobStore.StoreTrigger(CreateTrigger("trigger2", "triggerGroup", job.Key), true);
            JobStore.StoreTrigger(CreateTrigger("trigger3", "triggerfoobarGroup1", job.Key), true);
            JobStore.StoreTrigger(CreateTrigger("trigger4", "triggerfoobarGroup1", job.Key), true);

            //act
            var pausedGroups = JobStore.PauseTriggers(GroupMatcher<TriggerKey>.GroupContains("foobar"));

            //assert
            Assert.IsTrue(pausedGroups.Count == 1);

            Assert.AreEqual(TriggerState.Paused, JobStore.GetTriggerState(new TriggerKey("trigger3", "triggerfoobarGroup1")));
            Assert.AreEqual(TriggerState.Paused, JobStore.GetTriggerState(new TriggerKey("trigger4", "triggerfoobarGroup1")));
        }

        /// <summary>
        /// resume a trigger
        /// </summary>
        [TestMethod]
        public void ResumeTriggerSuccessfully()
        {
            //arrange
            var job = CreateJob();
            JobStore.StoreJob(job, false);
            var trigger = CreateTrigger("trigger1", "triggerGroup", job.Key);
            JobStore.StoreTrigger(trigger, false);
            JobStore.PauseTrigger(trigger.Key);

            //act
            JobStore.ResumeTrigger(trigger.Key);

            //assert
            Assert.AreEqual(TriggerState.Normal, JobStore.GetTriggerState(trigger.Key));
        }

        /// <summary>
        /// resume all the trigger which their group equals triggerGroup
        /// </summary>
        [TestMethod]
        public void ResumeTrigger_UseEqualOperator_Successfully()
        {
            //arrange
            var job = CreateJob();

            var trigger1 = CreateTrigger("trigger1", "triggerGroup", job.Key);
            var trigger2 = CreateTrigger("trigger2", "triggerGroup", job.Key);
            var trigger3 = CreateTrigger("trigger3", "triggerGroup1", job.Key);
            var trigger4 = CreateTrigger("trigger4", "triggerGroup1", job.Key);

            JobStore.StoreJob(job, true);
            JobStore.StoreTrigger(trigger1, true);
            JobStore.StoreTrigger(trigger2, true);
            JobStore.StoreTrigger(trigger3, true);
            JobStore.StoreTrigger(trigger4, true);

            JobStore.PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals("triggerGroup"));

            //act
            var resumedGroups = JobStore.ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals("triggerGroup"));

            //assert
            Assert.IsTrue(resumedGroups.Count == 1);
            Assert.AreEqual(TriggerState.Normal, JobStore.GetTriggerState(trigger1.Key));
            Assert.AreEqual(TriggerState.Normal, JobStore.GetTriggerState(trigger2.Key));
        }

        /// <summary>
        /// resume all the trigger which their group starts with trigger1
        /// </summary>
        [TestMethod]
        public void ResumeTrigger_UseStartsWithOperator_Successfully()
        {
            //arrange
            var job = CreateJob();

            var trigger1 = CreateTrigger("trigger1", "trigger1Group", job.Key);
            var trigger2 = CreateTrigger("trigger2", "trigger1Group", job.Key);
            var trigger3 = CreateTrigger("trigger3", "triggerGroup1", job.Key);
            var trigger4 = CreateTrigger("trigger4", "triggerGroup1", job.Key);

            JobStore.StoreJob(job, true);
            JobStore.StoreTrigger(trigger1, true);
            JobStore.StoreTrigger(trigger2, true);
            JobStore.StoreTrigger(trigger3, true);
            JobStore.StoreTrigger(trigger4, true);

            JobStore.PauseTriggers(GroupMatcher<TriggerKey>.GroupStartsWith("trigger1"));

            //act
            var resumedGroups = JobStore.ResumeTriggers(GroupMatcher<TriggerKey>.GroupStartsWith("trigger1"));

            //assert
            Assert.IsTrue(resumedGroups.Count == 1);
            Assert.AreEqual(TriggerState.Normal, JobStore.GetTriggerState(trigger1.Key));
            Assert.AreEqual(TriggerState.Normal, JobStore.GetTriggerState(trigger2.Key));
        }

        /// <summary>
        /// resume all the trigger which their group ends with Group1
        /// </summary>
        [TestMethod]
        public void ResumeTrigger_UseEndsWithOperator_Successfully()
        {
            //arrange
            var job = CreateJob();

            var trigger1 = CreateTrigger("trigger1", "triggerGroup", job.Key);
            var trigger2 = CreateTrigger("trigger2", "triggerGroup", job.Key);
            var trigger3 = CreateTrigger("trigger3", "triggerGroup1", job.Key);
            var trigger4 = CreateTrigger("trigger4", "triggerGroup1", job.Key);

            JobStore.StoreJob(job, true);
            JobStore.StoreTrigger(trigger1, true);
            JobStore.StoreTrigger(trigger2, true);
            JobStore.StoreTrigger(trigger3, true);
            JobStore.StoreTrigger(trigger4, true);

            JobStore.PauseTriggers(GroupMatcher<TriggerKey>.GroupEndsWith("Group1"));

            //act
            var resumedGroups = JobStore.ResumeTriggers(GroupMatcher<TriggerKey>.GroupEndsWith("Group1"));

            //assert
            Assert.IsTrue(resumedGroups.Count == 1);
            Assert.AreEqual(TriggerState.Normal, JobStore.GetTriggerState(trigger3.Key));
            Assert.AreEqual(TriggerState.Normal, JobStore.GetTriggerState(trigger4.Key));
        }

        /// <summary>
        /// resume all the trigger which their group contains foobar
        /// </summary>
        [TestMethod]
        public void ResumeTrigger_UseContainsOperator_Successfully()
        {
            //arrange
            var job = CreateJob();

            var trigger1 = CreateTrigger("trigger1", "triggerGroup", job.Key);
            var trigger2 = CreateTrigger("trigger2", "triggerGroup", job.Key);
            var trigger3 = CreateTrigger("trigger3", "triggerfoobarGroup", job.Key);
            var trigger4 = CreateTrigger("trigger4", "triggerfoobarGroup", job.Key);

            JobStore.StoreJob(job, true);
            JobStore.StoreTrigger(trigger1, true);
            JobStore.StoreTrigger(trigger2, true);
            JobStore.StoreTrigger(trigger3, true);
            JobStore.StoreTrigger(trigger4, true);

            JobStore.PauseTriggers(GroupMatcher<TriggerKey>.GroupContains("foobar"));

            //act
            var resumedGroups = JobStore.ResumeTriggers(GroupMatcher<TriggerKey>.GroupContains("foobar"));

            //assert
            Assert.IsTrue(resumedGroups.Count == 1);
            Assert.AreEqual(TriggerState.Normal, JobStore.GetTriggerState(trigger3.Key));
            Assert.AreEqual(TriggerState.Normal, JobStore.GetTriggerState(trigger4.Key));
        }

        /// <summary>
        /// get all the triggergroup whose state is in paused.
        /// </summary>
        [TestMethod]
        public void GetPausedTriggerGroupsSuccessfully()
        {
            //arrange
            var job = CreateJob();
            JobStore.StoreJob(job, true);
            JobStore.StoreTrigger(CreateTrigger("trigger1", "triggerGroup", job.Key), true);
            JobStore.StoreTrigger(CreateTrigger("trigger2", "triggerGroup", job.Key), true);
            JobStore.StoreTrigger(CreateTrigger("trigger3", "triggerfoobarGroup1", job.Key), true);
            JobStore.StoreTrigger(CreateTrigger("trigger4", "triggerfoobarGroup2", job.Key), true);
            JobStore.PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals("triggerGroup"));

            //act
            var pausedGroups = JobStore.GetPausedTriggerGroups();

            //Assert
            Assert.IsTrue(pausedGroups.Count == 1);
        }

        /// <summary>
        /// Pause all the triggers in the store
        /// </summary>
        [TestMethod]
        public void PauseAllSuccessfully()
        {
            //arrange
            var jobsAndTriggers = CreateJobsAndTriggers(2, 2, 2, 2);
            JobStore.StoreJobsAndTriggers(jobsAndTriggers, false);

            //act
            JobStore.PauseAll();

            //assert
            foreach (var jobAndTriggers in jobsAndTriggers)
            {
                foreach (var trigger in jobAndTriggers.Value)
                {
                    Assert.AreEqual(TriggerState.Paused, JobStore.GetTriggerState(trigger.Key));
                }
            }

        }

        /// <summary>
        /// Resume all the triggers in the store
        /// </summary>
        [TestMethod]
        public void ResumeAll_AfterPauseAll_Successfully()
        {
            //arrange
            var jobsAndTriggers = CreateJobsAndTriggers(2, 1, 1, 2);
            JobStore.StoreJobsAndTriggers(jobsAndTriggers, false);
            JobStore.PauseAll();

            //act
            JobStore.ResumeAll();

            //assert
            foreach (var jobAndTriggers in jobsAndTriggers)
            {
                foreach (var trigger in jobAndTriggers.Value)
                {
                    Assert.AreEqual(TriggerState.Normal, JobStore.GetTriggerState(trigger.Key));
                }
            }
        }

        /// <summary>
        /// when acquireNextTriggers is called, the triggers returned are in the acquired state.
        /// </summary>
        [TestMethod]
        public void AcquireNextTriggers_TriggersAreInAcquiredState_Successfully()
        {
            //arrange
            var calendarName = "testCalendar1";
            JobStore.StoreCalendar(calendarName, new WeeklyCalendar(), true, true);
            var job = CreateJob();
            JobStore.StoreJob(job, true);
            JobStore.StoreTrigger(CreateTrigger("trigger1", "triggerGroup", job.Key, "* * * * * ?", calendarName), true);
            JobStore.StoreTrigger(CreateTrigger("trigger2", "triggerGroup", job.Key, "* * * * * ?", calendarName), true);
            JobStore.StoreTrigger(CreateTrigger("trigger3", "triggerfoobarGroup1", job.Key, "* * * * * ?", calendarName), true);
            JobStore.StoreTrigger(CreateTrigger("trigger4", "triggerfoobarGroup2", job.Key, "* * * * * ?", calendarName), true);

            //act
            var acquiredTriggers = JobStore.AcquireNextTriggers(new DateTimeOffset(DateTime.UtcNow), 20, TimeSpan.FromMilliseconds(1000));

            //assert
            Assert.IsTrue(acquiredTriggers.Count == 4);
            foreach (var trigger in acquiredTriggers)
            {
                Assert.AreEqual(TriggerState.Normal, JobStore.GetTriggerState(trigger.Key));
                var triggerHashKey = Schema.TriggerHashkey(trigger.Key);
                Assert.IsTrue(Db.SortedSetScore(Schema.TriggerStateSetKey(RedisTriggerState.Acquired), triggerHashKey) > 0);
            }
        }

        /// <summary>
        /// Trigger Firing 
        /// </summary>
        [TestMethod]
        public void TriggerFiredSuccessfully()
        {
            //arrange
            var calendarName = "testCalendar10";
            JobStore.StoreCalendar(calendarName, new WeeklyCalendar(), true, true);
            var job = CreateJob();
            JobStore.StoreJob(job, true);
            JobStore.StoreTrigger(CreateTrigger("trigger1", "triggerGroup", job.Key, "* * * * * ?", calendarName), true);
            JobStore.StoreTrigger(CreateTrigger("trigger2", "triggerGroup", job.Key, "* * * * * ?", calendarName), true);
            JobStore.StoreTrigger(CreateTrigger("trigger3", "triggerfoobarGroup1", job.Key, "* * * * * ?", calendarName), true);
            JobStore.StoreTrigger(CreateTrigger("trigger4", "triggerfoobarGroup2", job.Key, "* * * * * ?", calendarName), true);
            var acquiredTriggers = JobStore.AcquireNextTriggers(new DateTimeOffset(DateTime.UtcNow), 20, TimeSpan.FromMilliseconds(1000));

            //act
            var triggerFiredResult = JobStore.TriggersFired(acquiredTriggers);

            //assert 
            Assert.IsTrue(triggerFiredResult.Count == 4);
        }

        /// <summary>
        /// during firing a trigger, need to add the job which does allow concurrent execution into blockedJobSet in the store.
        /// </summary>
        [TestMethod]
        public void TriggerFired_JobIsDisallowedConcurrent_AddToBlockedJobSet()
        {
            //arrange
            var job =
                JobBuilder.Create<NonConcurrentJob>()
                          .WithIdentity("testDisallowConcurrentJob", "JobGroup")
                          .WithDescription("I am a DisallowConcurrent job")
                          .Build();

            var trigger1 = CreateTrigger("trigger1", "triggerGroup1", job.Key, "* * * * * ?","testCalendar33");
            JobStore.StoreCalendar("testCalendar33", new WeeklyCalendar(), true, true);
            JobStore.StoreJob(job, false);
            JobStore.StoreTrigger(trigger1, false);
            var jobHashKey = Schema.JobHashKey(job.Key);
            var acquiredTriggers = JobStore.AcquireNextTriggers(new DateTimeOffset(DateTime.UtcNow), 20, TimeSpan.FromMilliseconds(1000));

            //act
            JobStore.TriggersFired(acquiredTriggers);

            //assert    
            Assert.IsTrue(Db.SetContains(Schema.BlockedJobsSet(), jobHashKey));
        }

        /// <summary>
        /// could not replace trigger, if the triggers are different in group.
        /// </summary>
        [TestMethod]
        public void ReplaceTrigger_WithDifferentNameAndGroup_Unsuccessfully()
        {
            //arrange
            var job = CreateJob();
            var trigger = CreateTrigger("testTrigger", "testTriggerGroup", job.Key);
            JobStore.StoreJob(job, false);
            JobStore.StoreTrigger(trigger, false);

            //act
            var result = JobStore.ReplaceTrigger(new TriggerKey("foo", "bar"), trigger);

            //assert
            Assert.IsFalse(result);
        }

        /// <summary>
        /// when the old trigger is replaced, then it should be removed from store as well. 
        /// </summary>
        [TestMethod]
        public void ReplaceTrigger_OldTriggerRemoved_Successfully()
        {
            //arrange
            var job = CreateJob();
            var trigger1 = CreateTrigger("trigger1", "triggerGroup", job.Key);
            var trigger2 = CreateTrigger("trigger2", "triggerGroup", job.Key);
            JobStore.StoreJob(job, false);
            JobStore.StoreTrigger(trigger1, false);
            JobStore.StoreTrigger(trigger2, false);
            var newTrigger = CreateTrigger("newTrigger", "triggerGroup", job.Key);

            //act
            var result = JobStore.ReplaceTrigger(trigger1.Key, newTrigger);

            //assert
            Assert.IsTrue(result);
            Assert.IsNull(JobStore.RetrieveTrigger(trigger1.Key));
        }

        /// <summary>
        /// when the old trigger is replaced, then the new trigger is saved in the store.
        /// </summary>
        [TestMethod]
        public void ReplaceTrigger_NewTriggerAdded_Successfully()
        {
            //arrange
            var job = CreateJob();
            var trigger1 = CreateTrigger("trigger1", "triggerGroup", job.Key);
            var trigger2 = CreateTrigger("trigger2", "triggerGroup", job.Key);
            JobStore.StoreJob(job, false);
            JobStore.StoreTrigger(trigger1, false);
            JobStore.StoreTrigger(trigger2, false);
            var newTrigger = CreateTrigger("newTrigger", "triggerGroup", job.Key);

            //act
            JobStore.ReplaceTrigger(trigger1.Key, newTrigger);

            //assert
            Assert.IsTrue(JobStore.GetTriggersForJob(job.Key).Count == 2);
            Assert.IsTrue(JobStore.GetTriggersForJob(job.Key).Select(x => x.Key.Equals(newTrigger.Key)).Any());
        }

        /// <summary>
        /// when a non durable job has a trigger, that trigger is replaced by a new trigger. the job will not be removed.
        /// </summary>
        [TestMethod]
        public void RepaceTrigger_ForNonDurableJobWithSingleTrigger_JobStillExistsAfterTriggerReplacement()
        {
            //arrange
            var job = CreateJob("job11");
            var trigger1 = CreateTrigger("trigger3", "triGroup", job.Key);
            JobStore.StoreJob(job, false);
            JobStore.StoreTrigger(trigger1, false);
            var newTrigger = CreateTrigger("newTrigger", "triGroup", job.Key);

            //act
            JobStore.ReplaceTrigger(trigger1.Key, newTrigger);
            var result = JobStore.RetrieveJob(job.Key);

            //assert
            Assert.IsNotNull(result);
        }

        /// <summary>
        /// coulnt replace trigger if its for a dffernt job.
        /// </summary>
        [TestMethod, ExpectedException(typeof(JobPersistenceException))]
        public void ReplaceTrigger_WithDifferentJob_Unsuccessfully()
        {
            //arrange
            var job = CreateJob();
            var trigger1 = CreateTrigger("trigger1", "triggerGroup", job.Key);
            JobStore.StoreJob(job, false);
            JobStore.StoreTrigger(trigger1, false);
            var newTrigger = CreateTrigger("newTrigger", "triggerGroup", new JobKey("foo", "bar"));

            //act
            JobStore.ReplaceTrigger(trigger1.Key, newTrigger);

        }
    }
}
