using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Quartz;
using Quartz.Impl.Matchers;
using QuartzRedisJobStore.JobStore;

namespace QuartzRedisJobStore.UnitTest
{
    /// <summary>
    /// Job related tests.
    /// </summary>
    [TestClass]
    public class JobFixture : BaseFixture
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
        /// store a job
        /// </summary>
        [TestMethod]
        public async Task StoreJobSuccessfully()
        {
            //arrange
            var job = CreateJob();
            await JobStore.StoreJob(job, false);

            //act
            var jobData = await Db.HashGetAllAsync(Schema.JobHashKey(job.Key));

            //assert
            Assert.IsNotNull(jobData);
            var description = (string)(from j in jobData.ToList()
                               where j.Name == RedisJobStoreSchema.Description
                               select j.Value).FirstOrDefault();

            Assert.AreEqual("JobTesting", description);
        }

        /// <summary>
        /// store jobdatamap. 
        /// </summary>
        [TestMethod]
        public async Task StoreJobDataMapSuccessfully()
        {
            //arrange
            var job = CreateJob();
            await JobStore.StoreJob(job, false);

            //act
            var jobData = await Db.HashGetAllAsync(Schema.JobDataMapHashKey(job.Key));

            //assert
            Assert.IsNotNull(jobData);
            var data = (string)(from j in jobData.ToList()
                        where j.Name == "testJob"
                        select j.Value).FirstOrDefault();

            Assert.AreEqual("testJob", data);
        }

        /// <summary>
        /// try to store another job with the same name, set replacing to false, then 
        /// the original one will not be overriden.
        /// </summary>
        [TestMethod]
        public async Task StoreJob_WithoutReplacingExisting_NoOverride()
        {
            //arrange
            var job = CreateJob();
            await JobStore.StoreJob(CreateJob(), false);
            try
            {
                await JobStore.StoreJob(CreateJob(description: "anotherDescription"), false);
            }
            catch { }

            //act
            var jobData = await Db.HashGetAllAsync(Schema.JobHashKey(job.Key));

            //assert
            Assert.IsNotNull(jobData);
            var description = (string)(from j in jobData.ToList()
                               where j.Name == RedisJobStoreSchema.Description
                               select j.Value).FirstOrDefault();

            Assert.AreEqual("JobTesting", description);
        }

        /// <summary>
        /// try to store another job with the same name, set replacing to true, then 
        /// the original one will be overriden.
        /// </summary>
        [TestMethod]
        public async Task StoreJob_WithReplacingExisting_OverrideSuccessfully()
        {
            //arrange
            var job = CreateJob();
            await JobStore.StoreJob(CreateJob(), true);
            await JobStore.StoreJob(CreateJob(description: "anotherDescription"), true);

            //act
            var jobData = await Db.HashGetAllAsync(Schema.JobHashKey(job.Key));

            //assert
            Assert.IsNotNull(jobData);
            var description = (string)(from j in jobData.ToList()
                               where j.Name == RedisJobStoreSchema.Description
                               select j.Value).FirstOrDefault();

            Assert.AreEqual("anotherDescription", description);
        }

        /// <summary>
        /// retrieve a job
        /// </summary>
        [TestMethod]
        public async Task RetreiveJobSuccessfully()
        {
            //arrange
            var originalJob = CreateJob();
            await JobStore.StoreJob(originalJob, true);

            //act
            var retrievedJob = await JobStore.RetrieveJob(originalJob.Key);

            //assert
            Assert.IsNotNull(retrievedJob);

            Assert.IsTrue(retrievedJob.Equals(originalJob));
        }

        /// <summary>
        /// remove a job
        /// </summary>
        [TestMethod]
        public async Task RemoveJobSuccessfully()
        {
            //arrange 
            var job = CreateJob("job1", "group1");
            var trigger1 = CreateTrigger("trigger1", "triggerGroup1", job.Key);
            var trigger2 = CreateTrigger("trigger2", "triggerGroup2", job.Key);
            var triggerSet = new HashSet<ITrigger> { trigger1, trigger2 };
            await this.StoreJobAndTriggers(job, triggerSet);

            //act
            var result = await JobStore.RemoveJob(job.Key);

            //assert
            Assert.IsTrue(result);
            Assert.IsNull(await JobStore.RetrieveJob(job.Key));
            Assert.IsNull(await JobStore.RetrieveTrigger(trigger1.Key));
            Assert.IsNull(await JobStore.RetrieveTrigger(trigger2.Key));
        }

        /// <summary>
        /// remove jobs
        /// </summary>
        [TestMethod]
        public async Task RemoveJobsSuccessfully()
        {
            //arrange
            var jobsAndTriggers = CreateJobsAndTriggers(2, 1, 1, 1);
            await JobStore.StoreJobsAndTriggers(jobsAndTriggers, true);

            //act
            var result = await JobStore.RemoveJobs((from job in jobsAndTriggers.Keys select job.Key).ToList());

            //assert
            Assert.IsTrue(result);
        }

        /// <summary>
        /// Get total number of jobs in the store
        /// </summary>
        [TestMethod]
        public async Task GetNumberOfJobsSuccessfully()
        {
            //arrange
            await JobStore.StoreJob(CreateJob("job1", "group1"), true);
            await JobStore.StoreJob(CreateJob("job2", "group2"), true);
            await JobStore.StoreJob(CreateJob("job3", "group3"), true);

            //act
            var numberOfJobs = await JobStore.GetNumberOfJobs();

            //assert
            Assert.IsTrue(numberOfJobs == 3);
        }

        /// <summary>
        /// get the jobs in which its group is group1
        /// </summary>
        [TestMethod]
        public async Task GetJobKeys_UseEqualOperator_Successfully()
        {
            //arrange
            await JobStore.StoreJob(CreateJob("job1", "group1"), true);
            await JobStore.StoreJob(CreateJob("job2", "group1"), true);
            await JobStore.StoreJob(CreateJob("job3", "group3"), true);

            //act
            var jobKeys = await JobStore.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("group1"));

            //assert
            Assert.IsTrue(jobKeys.Count == 2);
        }

        /// <summary>
        /// get the jobs in which its group contains group0
        /// </summary>
        [TestMethod]
        public async Task GetJobKeys_UseContainOperator_Successfully()
        {
            //arrange
            await JobStore.StoreJob(CreateJob("job1", "group01"), true);
            await JobStore.StoreJob(CreateJob("job2", "group01"), true);
            await JobStore.StoreJob(CreateJob("job3", "group03"), true);

            //act
            var jobKeys = await JobStore.GetJobKeys(GroupMatcher<JobKey>.GroupContains("group0"));

            //assert
            Assert.IsTrue(jobKeys.Count == 3);
        }

        /// <summary>
        /// get the jobs in which its group ends with s
        /// </summary>
        [TestMethod]
        public async Task GetJobKeys_UseEndsWithOperator_Successfully()
        {
            //arrange
            await JobStore.StoreJob(CreateJob("job1", "group01s"), true);
            await JobStore.StoreJob(CreateJob("job2", "group01s"), true);
            await JobStore.StoreJob(CreateJob("job3", "group03s"), true);

            //act
            var jobKeys = await JobStore.GetJobKeys(GroupMatcher<JobKey>.GroupEndsWith("s"));

            //assert
            Assert.IsTrue(jobKeys.Count == 3);
        }

        /// <summary>
        /// get the jobs in which its group starts with groups
        /// </summary>
        [TestMethod]
        public async Task GetJobKeys_UseStartsWithOperator_Successfully()
        {
            //arrange
            await JobStore.StoreJob(CreateJob("job1", "groups1"), true);
            await JobStore.StoreJob(CreateJob("job2", "groups2"), true);
            await JobStore.StoreJob(CreateJob("job3", "groups3"), true);

            //act
            var jobKeys = await JobStore.GetJobKeys(GroupMatcher<JobKey>.GroupStartsWith("groups"));

            //assert
            Assert.IsTrue(jobKeys.Count == 3);
        }

        /// <summary>
        /// get all the group name in the store
        /// </summary>
        [TestMethod]
        public async Task GetJobGroupNamesSuccessfully()
        {

            //arrange
            await JobStore.StoreJob(CreateJob("job1", "groups1"), true);
            await JobStore.StoreJob(CreateJob("job2", "groups2"), true);

            //act
            var groups = await JobStore.GetJobGroupNames();

            //assert
            Assert.IsTrue(groups.Count == 2);
        }

        /// <summary>
        /// pause a job
        /// </summary>
        [TestMethod]
        public async Task PauseJobSuccessfully()
        {
            //arrange
            var job = CreateJob("pausedJob", "pausedGroup");
            var trigger1 = CreateTrigger("trigger1", "triggerGroup1", job.Key);
            var trigger2 = CreateTrigger("trigger2", "triggerGroup2", job.Key);
            var triggerSet = new HashSet<ITrigger> { trigger1, trigger2 };
            await this.StoreJobAndTriggers(job, triggerSet);

            //act
            await JobStore.PauseJob(job.Key);

            //assert
            Assert.AreEqual(TriggerState.Paused, await JobStore.GetTriggerState(trigger1.Key));
            Assert.AreEqual(TriggerState.Paused, await JobStore.GetTriggerState(trigger2.Key));
        }

        /// <summary>
        /// Pause all the job which their group equals jobGroup_1
        /// </summary>
        [TestMethod]
        public async Task PauseJobs_UseEqualOperator_Successfully()
        {
            //arrange
            var jobsAndTriggers = CreateJobsAndTriggers(2, 1, 1, 2);
            await JobStore.StoreJobsAndTriggers(jobsAndTriggers, false);
            var pausedGroup = jobsAndTriggers.First().Key.Key.Group;

            //act
            await JobStore.PauseJobs(GroupMatcher<JobKey>.GroupEquals(pausedGroup));

            //assert
            foreach (var job in jobsAndTriggers.Keys)
            {
                var triggers = jobsAndTriggers[job];

                if (job.Key.Group == pausedGroup)
                {
                    foreach (var trigger in triggers)
                        Assert.AreEqual(TriggerState.Paused, await JobStore.GetTriggerState(trigger.Key));
                }
                else
                {
                    foreach (var trigger in triggers)
                        Assert.AreEqual(TriggerState.Normal, await JobStore.GetTriggerState(trigger.Key));
                }
            }
        }

        /// <summary>
        /// Pause all the job which their group starts with start
        /// </summary>
        [TestMethod]
        public async Task PauseJobs_UseStartsWithOperator_Successfully()
        {
            //arrange
            var job = CreateJob("job1", "startGroup");
            var trigger1 = CreateTrigger("trigger1", "triggerGroup1", job.Key);
            var trigger2 = CreateTrigger("trigger2", "triggerGroup2", job.Key);
            var triggerSet = new HashSet<ITrigger> { trigger1, trigger2 };
            await this.StoreJobAndTriggers(job, triggerSet);

            //act
            var pausedJobs = await JobStore.PauseJobs(GroupMatcher<JobKey>.GroupStartsWith("start"));

            //assert
            Assert.IsTrue(pausedJobs.Count == 1);
            Assert.AreEqual(TriggerState.Paused, await JobStore.GetTriggerState(trigger1.Key));
            Assert.AreEqual(TriggerState.Paused, await JobStore.GetTriggerState(trigger2.Key));
        }

        /// <summary>
        /// Pause all the job which their group ends with Ends
        /// </summary>
        [TestMethod]
        public async Task PauseJobs_UseEndssWithOperator_Successfully()
        {
            //arrange
            var job = CreateJob("job1", "GroupEnds");
            var trigger1 = CreateTrigger("trigger1", "triggerGroup1", job.Key);
            var trigger2 = CreateTrigger("trigger2", "triggerGroup2", job.Key);
            var triggerSet = new HashSet<ITrigger> { trigger1, trigger2 };
            await this.StoreJobAndTriggers(job, triggerSet);

            //act
            var pausedJobs = await JobStore.PauseJobs(GroupMatcher<JobKey>.GroupEndsWith("Ends"));

            //assert
            Assert.IsTrue(pausedJobs.Count == 1);
            Assert.AreEqual(TriggerState.Paused, await JobStore.GetTriggerState(trigger1.Key));
            Assert.AreEqual(TriggerState.Paused, await JobStore.GetTriggerState(trigger2.Key));
        }

        /// <summary>
        /// Pause all the job which their group contains foobar
        /// </summary>
        [TestMethod]
        public async Task PauseJobs_UseContainWithsOperator_Successfully()
        {
            //arrange
            var job = CreateJob("job1", "GroupContainsfoobar");
            var trigger1 = CreateTrigger("trigger1", "triggerGroup1", job.Key);
            var trigger2 = CreateTrigger("trigger2", "triggerGroup2", job.Key);
            var triggerSet = new HashSet<ITrigger> { trigger1, trigger2 };
            await this.StoreJobAndTriggers(job, triggerSet);

            //act
            var pausedJobs = await JobStore.PauseJobs(GroupMatcher<JobKey>.GroupContains("foobar"));

            //assert
            Assert.IsTrue(pausedJobs.Count == 1);
            Assert.AreEqual(TriggerState.Paused, await JobStore.GetTriggerState(trigger1.Key));
            Assert.AreEqual(TriggerState.Paused, await JobStore.GetTriggerState(trigger2.Key));
        }

        /// <summary>
        /// resume a job
        /// </summary>
        [TestMethod]
        public async Task ResumeJobSuccessfully()
        {
            //arrange
            var job = CreateJob("job1", "jobGroup1");
            var trigger1 = CreateTrigger("trigger1", "triggerGroup1", job.Key);
            var trigger2 = CreateTrigger("trigger2", "triggerGroup2", job.Key);
            var triggerSet = new HashSet<ITrigger> { trigger1, trigger2 };
            await this.StoreJobAndTriggers(job, triggerSet);
            await JobStore.PauseJob(job.Key);

            //act
            await JobStore.ResumeJob(job.Key);

            //assert
            Assert.AreEqual(TriggerState.Normal, await JobStore.GetTriggerState(trigger1.Key));
            Assert.AreEqual(TriggerState.Normal, await JobStore.GetTriggerState(trigger2.Key));
        }


        /// <summary>
        /// resume all the job which their group equals jobGroup_1
        /// </summary>
        [TestMethod]
        public async Task ResumeJobs_UseEqualOperator_Successfully()
        {
            //arrange
            var jobsAndTriggers = CreateJobsAndTriggers(2, 2, 2, 2);
            await JobStore.StoreJobsAndTriggers(jobsAndTriggers, false);
            var pausedGroup = jobsAndTriggers.Keys.First().Key.Group;
            await JobStore.PauseJobs(GroupMatcher<JobKey>.GroupEquals(pausedGroup));
            IReadOnlyCollection<ITrigger> triggers = new HashSet<ITrigger>();
            jobsAndTriggers.TryGetValue(jobsAndTriggers.Keys.First(), out triggers);

            //act
            var resumedJobGroups = await JobStore.ResumeJobs(GroupMatcher<JobKey>.GroupEquals(pausedGroup));

            //assert
            Assert.IsTrue(resumedJobGroups.Count == 1);
            Assert.AreEqual(resumedJobGroups.First(), pausedGroup);

            //all its triggers are back to the Normal state
            foreach (var trigger in triggers)
            {
                Assert.AreEqual(TriggerState.Normal, await JobStore.GetTriggerState(trigger.Key));
            }
        }

        /// <summary>
        /// resume all the job which their group ends with _1
        /// </summary>
        [TestMethod]
        public async Task ResumeJobs_UseEndsWithOperator_Successfully()
        {
            //arrange
            var jobsAndTriggers = CreateJobsAndTriggers(2, 2, 2, 2);
            await JobStore.StoreJobsAndTriggers(jobsAndTriggers, false);

            await JobStore.PauseJobs(GroupMatcher<JobKey>.GroupEndsWith("_1"));
            IReadOnlyCollection<ITrigger> triggers = new HashSet<ITrigger>();
            jobsAndTriggers.TryGetValue(jobsAndTriggers.Keys.First(), out triggers);

            //act
            var resumedJobGroups = await JobStore.ResumeJobs(GroupMatcher<JobKey>.GroupEndsWith("_1"));

            //assert
            Assert.IsTrue(resumedJobGroups.Count == 1);

            //all its triggers are back to the Normal state
            foreach (var trigger in triggers)
            {
                Assert.AreEqual(TriggerState.Normal, await JobStore.GetTriggerState(trigger.Key));
            }
        }

        /// <summary>
        /// resume all the job which their group starts with jobGroup_
        /// </summary>
        [TestMethod]
        public async Task ResumeJobs_UseStartsWithOperator_Successfully()
        {
            //arrange
            var jobsAndTriggers = CreateJobsAndTriggers(2, 1, 1, 1);
            await JobStore.StoreJobsAndTriggers(jobsAndTriggers, false);
            await JobStore.PauseJobs(GroupMatcher<JobKey>.GroupStartsWith("jobGroup_"));

            //act
            var resumedJobGroups = await JobStore.ResumeJobs(GroupMatcher<JobKey>.GroupStartsWith("jobGroup_"));

            //assert
            Assert.IsTrue(resumedJobGroups.Count == 2);
        }

        /// <summary>
        /// resume all the job which their group contains _
        /// </summary>
        [TestMethod]
        public async Task ResumeJobs_UseContainsWithOperator_Successfully()
        {
            //arrange
            var jobsAndTriggers = CreateJobsAndTriggers(2, 2, 2, 2);
            await JobStore.StoreJobsAndTriggers(jobsAndTriggers, false);
            await JobStore.PauseJobs(GroupMatcher<JobKey>.GroupContains("_"));

            //act
            var resumedJobGroups = await JobStore.ResumeJobs(GroupMatcher<JobKey>.GroupContains("_"));

            //assert
            Assert.IsTrue(resumedJobGroups.Count == 2);
        }
    }
}
