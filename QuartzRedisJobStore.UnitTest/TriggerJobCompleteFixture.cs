using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Quartz;
using Quartz.Impl;
using Quartz.Spi;

namespace QuartzRedisJobStore.UnitTest
{
    /// <summary>
    /// trigger job completed related tests.
    /// </summary>
    [TestClass]
    public class TriggerJobCompleteFixture : BaseFixture
    {

        /// <summary>
        /// IJobDetail
        /// </summary>
        private static IJobDetail _job;
        /// <summary>
        /// IOperableTrigger
        /// </summary>
        private static IOperableTrigger _trigger1, _trigger2;

        /// <summary>
        /// call before each tests runs.
        /// </summary>
        [TestInitialize]
        public void Initialize()
        {
            _job = CreateJob();
            _trigger1 = CreateTrigger("trigger1", "triggerGroup1", _job.Key);
            _trigger2 = CreateTrigger("trigger2", "triggerGroup1", _job.Key);
            JobStore.StoreJob(_job, false);
            JobStore.StoreTrigger(_trigger1, false);
            JobStore.StoreTrigger(_trigger2, false);
        }

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
        /// call TriggeredJobComplete, set Instruction to delete, trigger is deleted
        /// </summary>
        [TestMethod]
        public void TrigggeredJobComplete_SetInstructionToDeleted_TriggerIsDeletedSuccesfully()
        {
            //act
            JobStore.TriggeredJobComplete(_trigger1, _job, SchedulerInstruction.DeleteTrigger);

            //assert
            Assert.IsNull(JobStore.RetrieveTrigger(_trigger1.Key));

            MockedSignaler.Verify(x => x.SignalSchedulingChange(null));
        }

        /// <summary>
        /// call triggeredJobCompelete, set Instruction to error, trigger is moved to the error state. 
        /// </summary>
        [TestMethod]
        public void TrigggeredJobComplete_SetInstructionToError_TriggerSetToErrorState()
        {
            //act
            JobStore.TriggeredJobComplete(_trigger1, _job, SchedulerInstruction.SetTriggerError);

            //assert
            Assert.AreEqual(TriggerState.Error, JobStore.GetTriggerState(_trigger1.Key));
            MockedSignaler.Verify(x => x.SignalSchedulingChange(null));
        }

        /// <summary>
        /// call triggeredJobCompelete, set Instruction to allJobTriggersError, triggers are moved to the error state. 
        /// </summary>
        [TestMethod]
        public void TrigggeredJobComplete_SetInstructionToAllJobTriggersError_TriggersSetToErrorState()
        {
            //act
            JobStore.TriggeredJobComplete(_trigger1, _job, SchedulerInstruction.SetAllJobTriggersError);

            //assert
            Assert.AreEqual(TriggerState.Error, JobStore.GetTriggerState(_trigger1.Key));
            Assert.AreEqual(TriggerState.Error, JobStore.GetTriggerState(_trigger2.Key));
            MockedSignaler.Verify(x => x.SignalSchedulingChange(null));
        }

        /// <summary>
        /// call triggeredJobCompelete, set Instruction to AllJobTriggersComplete, triggers are moved to the complete state. 
        /// </summary>
        [TestMethod]
        public void TrigggeredJobComplete_SetInstructionToAllJobTriggersComplete_TriggersSetoCompleteState()
        {
            //act
            JobStore.TriggeredJobComplete(_trigger1, _job, SchedulerInstruction.SetAllJobTriggersComplete);

            //assert
            Assert.AreEqual(TriggerState.Complete, JobStore.GetTriggerState(_trigger1.Key));
            Assert.AreEqual(TriggerState.Complete, JobStore.GetTriggerState(_trigger2.Key));
            MockedSignaler.Verify(x => x.SignalSchedulingChange(null));
        }

        /// <summary>
        /// check jobdatamap is persisted into store after the trigger completes.
        /// </summary>
        [TestMethod]
        public void TriggeredJobComplete_JobIsPersist_JobDataMapIsResavedSuccessfully()
        {
            //arrange
            var job =
                JobBuilder.Create<PersistJob>()
                          .WithIdentity("testPersistJob", "persistJobGroup")
                          .UsingJobData("data", "data")
                          .WithDescription("I am a persist job")
                          .Build();

            var trigger1 = CreateTrigger("trigger1", "triggerGroup1", _job.Key);
            JobStore.StoreJob(job, false);
            JobStore.StoreTrigger(trigger1, false);
            var jobDetails = (JobDetailImpl)job;
            IDictionary<string, object> dict = new Dictionary<string, object> { { "test", "test" } };
            jobDetails.JobDataMap = new JobDataMap(dict);

            //act
            JobStore.TriggeredJobComplete(trigger1, job, SchedulerInstruction.SetTriggerComplete);
            var retrievedJob = JobStore.RetrieveJob(job.Key);

            //assert    
            Assert.AreEqual(retrievedJob.JobDataMap["test"], "test");
        }

        /// <summary>
        /// when trigger completes, the disallow concurrent execution job should be removed from blockedJobsSet.
        /// </summary>
        [TestMethod]
        public void TriggeredJobComplete_ForDisallowConcurrentJob_JobIsNotInBlockedJobsSet()
        {
            //arrange
            var job =
                JobBuilder.Create<NonConcurrentJob>()
                          .WithIdentity("testDisallowConcurrentJob", "JobGroup")
                          .WithDescription("I am a DisallowConcurrent job")
                          .Build();

            var trigger1 = CreateTrigger("trigger1", "triggerGroup1", job.Key);
            JobStore.StoreJob(job, false);
            JobStore.StoreTrigger(trigger1, false);
            var jobHashKey = Schema.JobHashKey(job.Key);

            //act
            JobStore.TriggeredJobComplete(trigger1, job, SchedulerInstruction.SetTriggerComplete);


            //assert
            Assert.IsFalse(Db.SetContains(Schema.BlockedJobsSet(), jobHashKey));
        }
    }
}
