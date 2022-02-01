using System.Collections.Generic;
using System.Threading.Tasks;
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
            JobStore.StoreJob(_job, false).Wait();
            JobStore.StoreTrigger(_trigger1, false).Wait();
            JobStore.StoreTrigger(_trigger2, false).Wait();
        }

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
        /// call TriggeredJobComplete, set Instruction to delete, trigger is deleted
        /// </summary>
        [TestMethod]
        public async Task TrigggeredJobComplete_SetInstructionToDeleted_TriggerIsDeletedSuccesfully()
        {
            //act
            await JobStore.TriggeredJobComplete(_trigger1, _job, SchedulerInstruction.DeleteTrigger);

            //assert
            Assert.IsNull(await JobStore.RetrieveTrigger(_trigger1.Key));

            MockedSignaler.Verify(x => x.SignalSchedulingChange(null, default));
        }

        /// <summary>
        /// call triggeredJobCompelete, set Instruction to error, trigger is moved to the error state. 
        /// </summary>
        [TestMethod]
        public async Task TrigggeredJobComplete_SetInstructionToError_TriggerSetToErrorState()
        {
            //act
            await JobStore.TriggeredJobComplete(_trigger1, _job, SchedulerInstruction.SetTriggerError);

            //assert
            Assert.AreEqual(TriggerState.Error, await JobStore.GetTriggerState(_trigger1.Key));
            MockedSignaler.Verify(x => x.SignalSchedulingChange(null, default));
        }

        /// <summary>
        /// call triggeredJobCompelete, set Instruction to allJobTriggersError, triggers are moved to the error state. 
        /// </summary>
        [TestMethod]
        public async Task TrigggeredJobComplete_SetInstructionToAllJobTriggersError_TriggersSetToErrorState()
        {
            //act
            await JobStore.TriggeredJobComplete(_trigger1, _job, SchedulerInstruction.SetAllJobTriggersError);

            //assert
            Assert.AreEqual(TriggerState.Error, await JobStore.GetTriggerState(_trigger1.Key));
            Assert.AreEqual(TriggerState.Error, await JobStore.GetTriggerState(_trigger2.Key));
            MockedSignaler.Verify(x => x.SignalSchedulingChange(null, default));
        }

        /// <summary>
        /// call triggeredJobCompelete, set Instruction to AllJobTriggersComplete, triggers are moved to the complete state. 
        /// </summary>
        [TestMethod]
        public async Task TrigggeredJobComplete_SetInstructionToAllJobTriggersComplete_TriggersSetoCompleteState()
        {
            //act
            await JobStore.TriggeredJobComplete(_trigger1, _job, SchedulerInstruction.SetAllJobTriggersComplete);

            //assert
            Assert.AreEqual(TriggerState.Complete, await JobStore.GetTriggerState(_trigger1.Key));
            Assert.AreEqual(TriggerState.Complete, await JobStore.GetTriggerState(_trigger2.Key));
            MockedSignaler.Verify(x => x.SignalSchedulingChange(null, default));
        }

        /// <summary>
        /// check jobdatamap is persisted into store after the trigger completes.
        /// </summary>
        [TestMethod]
        public async Task TriggeredJobComplete_JobIsPersist_JobDataMapIsResavedSuccessfully()
        {
            //arrange
            var job =
                JobBuilder.Create<PersistJob>()
                          .WithIdentity("testPersistJob", "persistJobGroup")
                          .UsingJobData("data", "data")
                          .WithDescription("I am a persist job")
                          .Build();

            var trigger1 = CreateTrigger("trigger11", "triggerGroup11", _job.Key);
            await JobStore.StoreJob(job, false);
            await JobStore.StoreTrigger(trigger1, false);
            var jobDetails = (JobDetailImpl)job;
            IDictionary<string, object> dict = new Dictionary<string, object> { { "test", "test" } };
            jobDetails.JobDataMap = new JobDataMap(dict);

            //act
            await JobStore.TriggeredJobComplete(trigger1, job, SchedulerInstruction.SetTriggerComplete);
            var retrievedJob = await JobStore.RetrieveJob(job.Key);

            //assert    
            Assert.AreEqual("test", retrievedJob.JobDataMap["test"]);
        }

        /// <summary>
        /// when trigger completes, the disallow concurrent execution job should be removed from blockedJobsSet.
        /// </summary>
        [TestMethod]
        public async Task TriggeredJobComplete_ForDisallowConcurrentJob_JobIsNotInBlockedJobsSet()
        {
            //arrange
            var job =
                JobBuilder.Create<NonConcurrentJob>()
                          .WithIdentity("testDisallowConcurrentJob", "JobGroup")
                          .WithDescription("I am a DisallowConcurrent job")
                          .Build();

            var trigger1 = CreateTrigger("trigger11", "triggerGroup11", job.Key);
            await JobStore.StoreJob(job, false);
            await JobStore.StoreTrigger(trigger1, false);
            var jobHashKey = Schema.JobHashKey(job.Key);

            //act
            await JobStore.TriggeredJobComplete(trigger1, job, SchedulerInstruction.SetTriggerComplete);

            //assert
            Assert.IsFalse(Db.SetContains(Schema.BlockedJobsSet(), jobHashKey));
        }
    }
}
