using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Quartz;
using Quartz.Impl.Matchers;
using Quartz.Impl.Triggers;
using Quartz.Spi;
using StackExchange.Redis;
using System.Threading;
using System.Threading.Tasks;
using System.Globalization;
using Microsoft.Extensions.Logging;
using System.Timers;

namespace QuartzRedisJobStore.JobStore
{
    /// <summary>
    /// base job storage which could be used by master/slave redis or clustered redis.
    /// </summary>
    public abstract class BaseJobStorage
    {
        /// <summary>
        /// Logger 
        /// </summary>
        readonly ILogger logger;

        /// <summary>
        /// Utc datetime of Epoch.
        /// </summary>
        static readonly DateTime unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// RedisJobStoreSchema
        /// </summary>
        protected readonly RedisJobStoreSchema redisJobStoreSchema;

        /// <summary>
        /// ISchedulerSignaler
        /// </summary>
        protected readonly ISchedulerSignaler signaler;

        /// <summary>
        /// threshold for the misfire (measured in milliseconds)
        /// </summary>
        protected int misfireThreshold = 60000;

        /// <summary>
        /// redis db.
        /// </summary>
        protected IDatabase db;

        /// <summary>
        /// Triggerlock time out here we need to make sure the longest job should not exceed this amount of time, otherwise we need to increase it. 
        /// </summary>
        protected int triggerLockTimeout;

        /// <summary>
        /// redis lock time out in milliseconds.
        /// </summary>
        protected int redisLockTimeout;

        /// <summary>
        /// scheduler instance id
        /// </summary>
        protected readonly string schedulerInstanceId;

        /// <summary>
        /// JsonSerializerSettings
        /// </summary>
        readonly JsonSerializerSettings serializerSettings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.All, DateTimeZoneHandling = DateTimeZoneHandling.Utc, NullValueHandling = NullValueHandling.Ignore, ContractResolver = new CamelCasePropertyNamesContractResolver() };

        /// <summary>
        /// constructor
        /// </summary>
        /// <param name="redisJobStoreSchema">RedisJobStoreSchema</param>
        /// <param name="db">IDatabase</param>
        /// <param name="signaler">ISchedulerSignaler</param>
        /// <param name="schedulerInstanceId">schedulerInstanceId</param>
        /// <param name="triggerLockTimeout">Trigger lock timeout(number in miliseconds) used in releasing the orphan triggers.</param>
        /// <param name="redisLockTimeout">Redis Lock timeout (number in miliseconds)</param>
        protected BaseJobStorage(RedisJobStoreSchema redisJobStoreSchema, IDatabase db, ISchedulerSignaler signaler, string schedulerInstanceId, int triggerLockTimeout, int redisLockTimeout, ILogger logger)
        {
            this.redisJobStoreSchema = redisJobStoreSchema;
            this.db = db;
            this.signaler = signaler;
            this.schedulerInstanceId = schedulerInstanceId;
            this.logger = logger;
            this.triggerLockTimeout = triggerLockTimeout;
            this.redisLockTimeout = redisLockTimeout;
        }


        /// <summary>
        /// Store the given <see cref="T:Quartz.IJobDetail"/>.
        /// </summary>
        /// <param name="jobDetail">The <see cref="T:Quartz.IJobDetail"/> to be stored.</param><param name="replaceExisting">If <see langword="true"/>, any <see cref="T:Quartz.IJob"/> existing in the
        ///             <see cref="T:Quartz.Spi.IJobStore"/> with the same name and group should be
        ///             over-written.
        ///             </param>
        public abstract Task StoreJobAsync(IJobDetail jobDetail, bool replaceExisting);

        /// <summary>
        /// Retrieve the <see cref="T:Quartz.IJobDetail"/> for the given
        ///             <see cref="T:Quartz.IJob"/>.
        /// </summary>
        /// <returns>
        /// The desired <see cref="T:Quartz.IJob"/>, or null if there is no match.
        /// </returns>
        public async Task<IJobDetail> RetrieveJobAsync(JobKey jobKey)
        {
            var jobHashKey = redisJobStoreSchema.JobHashKey(jobKey);
            var jobDetails = await db.HashGetAllAsync(jobHashKey);

            if (jobDetails == null || !jobDetails.Any())
                return null;

            var jobDataMapHashKey = redisJobStoreSchema.JobDataMapHashKey(jobKey);
            var jobDataMap = await db.HashGetAllAsync(jobDataMapHashKey);
            var jobProperties = ConvertToDictionaryString(jobDetails);

            var jobType = Type.GetType(jobProperties[RedisJobStoreSchema.JobClass]);

            if (jobType == null)
            {
                logger.LogWarning("Could not find job class {0} for job {1}", jobProperties[RedisJobStoreSchema.JobClass], jobKey);
                return null;
            }

            var jobBuilder =
                JobBuilder.Create(jobType)
                          .WithIdentity(jobKey)
                          .WithDescription(jobProperties[RedisJobStoreSchema.Description])
                          .RequestRecovery(Convert.ToBoolean(Convert.ToInt16(jobProperties[RedisJobStoreSchema.RequestRecovery])))
                          .StoreDurably(Convert.ToBoolean(Convert.ToInt16(jobProperties[RedisJobStoreSchema.IsDurable])));

            if (jobDataMap != null && jobDataMap.Any())
            {
                var dataMap = new JobDataMap(ConvertToDictionaryString(jobDataMap) as IDictionary);
                jobBuilder.SetJobData(dataMap);
            }
            return jobBuilder.Build();
        }

        /// <summary>
        /// Store the given <see cref="T:Quartz.ITrigger"/>.
        /// </summary>
        /// <param name="trigger">The <see cref="T:Quartz.ITrigger"/> to be stored.</param><param name="replaceExisting">If <see langword="true"/>, any <see cref="T:Quartz.ITrigger"/> existing in
        ///             the <see cref="T:Quartz.Spi.IJobStore"/> with the same name and group should
        ///             be over-written.</param><throws>ObjectAlreadyExistsException </throws>
        public abstract Task StoreTriggerAsync(ITrigger trigger, bool replaceExisting);

        /// <summary>
        /// remove the trigger state from all the possible sorted set.
        /// </summary>
        /// <param name="triggerHashKey">TriggerHashKey</param>
        /// <returns>succeeds or not</returns>
        public abstract Task<bool> UnsetTriggerStateAsync(string triggerHashKey);


        /// <summary>
        /// Store the given <see cref="T:Quartz.ICalendar"/>.
        /// </summary>
        /// <param name="name">The name.</param><param name="calendar">The <see cref="T:Quartz.ICalendar"/> to be stored.</param><param name="replaceExisting">If <see langword="true"/>, any <see cref="T:Quartz.ICalendar"/> existing
        ///             in the <see cref="T:Quartz.Spi.IJobStore"/> with the same name and group
        ///             should be over-written.</param><param name="updateTriggers">If <see langword="true"/>, any <see cref="T:Quartz.ITrigger"/>s existing
        ///             in the <see cref="T:Quartz.Spi.IJobStore"/> that reference an existing
        ///             Calendar with the same name with have their next fire time
        ///             re-computed with the new <see cref="T:Quartz.ICalendar"/>.</param><throws>ObjectAlreadyExistsException </throws>
        public abstract Task StoreCalendarAsync(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers);

        /// <summary>
        /// Remove (delete) the <see cref="T:Quartz.ICalendar"/> with the
        ///             given name.
        /// </summary>
        /// <remarks>
        /// If removal of the <see cref="T:Quartz.ICalendar"/> would result in
        ///             <see cref="T:Quartz.ITrigger"/>s pointing to non-existent calendars, then a
        ///             <see cref="T:Quartz.JobPersistenceException"/> will be thrown.
        /// </remarks>
        /// <param name="calendarName">The name of the <see cref="T:Quartz.ICalendar"/> to be removed.</param>
        /// <returns>
        /// <see langword="true"/> if a <see cref="T:Quartz.ICalendar"/> with the given name
        ///             was found and removed from the store.
        /// </returns>
        public abstract Task<bool> RemoveCalendarAsync(string calendarName);

        /// <summary>
        /// Remove (delete) the <see cref="T:Quartz.IJob"/> with the given
        ///             key, and any <see cref="T:Quartz.ITrigger"/> s that reference
        ///             it.
        /// </summary>
        /// <remarks>
        /// If removal of the <see cref="T:Quartz.IJob"/> results in an empty group, the
        ///             group should be removed from the <see cref="T:Quartz.Spi.IJobStore"/>'s list of
        ///             known group names.
        /// </remarks>
        /// <returns>
        /// <see langword="true"/> if a <see cref="T:Quartz.IJob"/> with the given name and
        ///             group was found and removed from the store.
        /// </returns>
        public abstract Task<bool> RemoveJobAsync(JobKey jobKey);

        /// <summary>
        /// Pause all of the <see cref="T:Quartz.IJob"/>s in the given
        ///             group - by pausing all of their <see cref="T:Quartz.ITrigger"/>s.
        /// <para>
        /// The JobStore should "remember" that the group is paused, and impose the
        ///             pause on any new jobs that are added to the group while the group is
        ///             paused.
        /// </para>
        /// </summary>
        /// <seealso cref="T:System.String"/>
        public abstract Task<IReadOnlyCollection<string>> PauseJobsAsync(GroupMatcher<JobKey> matcher);

        /// <summary>
        /// Resume (un-pause) the <see cref="T:Quartz.IJob"/> with the
        ///             given key.
        /// <para>
        /// If any of the <see cref="T:Quartz.IJob"/>'s<see cref="T:Quartz.ITrigger"/> s missed one
        ///             or more fire-times, then the <see cref="T:Quartz.ITrigger"/>'s misfire
        ///             instruction will be applied.
        /// </para>
        /// </summary>
        public async Task ResumeJobAsync(JobKey jobKey)
        {
            foreach (var trigger in await GetTriggersForJobAsync(jobKey))
                await ResumeTriggerAsync(trigger.Key);
        }

        /// <summary>
        /// Pause the <see cref="T:Quartz.IJob"/> with the given key - by
        ///             pausing all of its current <see cref="T:Quartz.ITrigger"/>s.
        /// </summary>
        public async Task PauseJobAsync(JobKey jobKey)
        {
            foreach (var trigger in await GetTriggersForJobAsync(jobKey))
                await PauseTriggerAsync(trigger.Key);
        }


        /// <summary>
        /// Resume (un-pause) all of the <see cref="T:Quartz.IJob"/>s in
        ///             the given group.
        /// <para>
        /// If any of the <see cref="T:Quartz.IJob"/> s had <see cref="T:Quartz.ITrigger"/> s that
        ///             missed one or more fire-times, then the <see cref="T:Quartz.ITrigger"/>'s
        ///             misfire instruction will be applied.
        /// </para>
        /// </summary>
        public abstract Task<IReadOnlyCollection<string>> ResumeJobsAsync(GroupMatcher<JobKey> matcher);

        /// <summary>
        /// Resume (un-pause) the <see cref="T:Quartz.ITrigger"/> with the
        ///             given key.
        /// <para>
        /// If the <see cref="T:Quartz.ITrigger"/> missed one or more fire-times, then the
        ///             <see cref="T:Quartz.ITrigger"/>'s misfire instruction will be applied.
        /// </para>
        /// </summary>
        /// <seealso cref="T:System.String"/>
        public abstract Task ResumeTriggerAsync(TriggerKey triggerKey);

        /// <summary>
        /// Remove (delete) the <see cref="T:Quartz.ITrigger"/> with the given key.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If removal of the <see cref="T:Quartz.ITrigger"/> results in an empty group, the
        ///             group should be removed from the <see cref="T:Quartz.Spi.IJobStore"/>'s list of
        ///             known group names.
        /// </para>
        /// <para>
        /// If removal of the <see cref="T:Quartz.ITrigger"/> results in an 'orphaned' <see cref="T:Quartz.IJob"/>
        ///             that is not 'durable', then the <see cref="T:Quartz.IJob"/> should be deleted
        ///             also.
        /// </para>
        /// </remarks>
        /// <returns>
        /// <see langword="true"/> if a <see cref="T:Quartz.ITrigger"/> with the given
        ///             name and group was found and removed from the store.
        /// </returns>
        public abstract Task<bool> RemoveTriggerAsync(TriggerKey triggerKey, bool removeNonDurableJob = true);

        /// <summary>
        /// Resume (un-pause) all of the <see cref="T:Quartz.ITrigger"/>s
        ///             in the given group.
        /// <para>
        /// If any <see cref="T:Quartz.ITrigger"/> missed one or more fire-times, then the
        ///             <see cref="T:Quartz.ITrigger"/>'s misfire instruction will be applied.
        /// </para>
        /// </summary>
        public abstract Task<IReadOnlyCollection<string>> ResumeTriggersAsync(GroupMatcher<TriggerKey> matcher);

        /// <summary>
        /// Pause the <see cref="T:Quartz.ITrigger"/> with the given key.
        /// </summary>
        public abstract Task PauseTriggerAsync(TriggerKey triggerKey);


        /// <summary>
        /// Pause all triggers - equivalent of calling <see cref="M:Quartz.Spi.IJobStore.PauseTriggers(Quartz.Impl.Matchers.GroupMatcher{Quartz.TriggerKey})"/>
        ///             on every group.
        /// <para>
        /// When <see cref="M:Quartz.Spi.IJobStore.ResumeAll"/> is called (to un-pause), trigger misfire
        ///             instructions WILL be applied.
        /// </para>
        /// </summary>
        /// <seealso cref="M:Quartz.Spi.IJobStore.ResumeAll"/>
        public async Task PauseAllTriggersAsync()
        {
            var triggerGroups = await db.SetMembersAsync(redisJobStoreSchema.TriggerGroupsSetKey());
            foreach (var group in triggerGroups)
                await PauseTriggersAsync(GroupMatcher<TriggerKey>.GroupEquals(redisJobStoreSchema.TriggerGroup(group)));
        }

        /// <summary>
        /// Resume (un-pause) all triggers - equivalent of calling <see cref="M:Quartz.Spi.IJobStore.ResumeTriggers(Quartz.Impl.Matchers.GroupMatcher{Quartz.TriggerKey})"/>
        ///             on every group.
        /// <para>
        /// If any <see cref="T:Quartz.ITrigger"/> missed one or more fire-times, then the
        ///             <see cref="T:Quartz.ITrigger"/>'s misfire instruction will be applied.
        /// </para>
        /// </summary>
        /// <seealso cref="M:Quartz.Spi.IJobStore.PauseAll"/>
        public async Task ResumeAllTriggersAsync()
        {
            var triggerGroups = await db.SetMembersAsync(redisJobStoreSchema.TriggerGroupsSetKey());
            foreach (var group in triggerGroups)
                await ResumeTriggersAsync(GroupMatcher<TriggerKey>.GroupEquals(redisJobStoreSchema.TriggerGroup(group)));
        }

        /// <summary>
        ///  Release triggers from the given current state to the new state if its locking scheduler has not registered as alive in the last triggerlocktimeout 
        /// </summary>
        /// <param name="currentState"></param>
        /// <param name="newState"></param>
        protected async Task ReleaseOrphanedTriggersAsync(RedisTriggerState currentState, RedisTriggerState newState)
        {
            var triggers = await db.SortedSetRangeByScoreWithScoresAsync(redisJobStoreSchema.TriggerStateSetKey(currentState), 0, -1);

            foreach (var sortedSetEntry in triggers)
            {
                var lockedId =
                    await db.StringGetAsync(
                        redisJobStoreSchema.TriggerLockKey(
                            redisJobStoreSchema.TriggerKey(sortedSetEntry.Element.ToString())));
                // Lock key has expired. We can safely alter the trigger's state.
                if (string.IsNullOrEmpty(lockedId))
                    await SetTriggerStateAsync(newState, sortedSetEntry.Score, sortedSetEntry.Element);
            }
        }

        /// <summary>
        /// Remove (delete) the <see cref="T:Quartz.ITrigger"/> with the
        ///             given name, and store the new given one - which must be associated
        ///             with the same job.
        /// </summary>
        /// <param name="triggerKey">The <see cref="T:Quartz.ITrigger"/> to be replaced.</param><param name="newTrigger">The new <see cref="T:Quartz.ITrigger"/> to be stored.</param>
        /// <returns>
        /// <see langword="true"/> if a <see cref="T:Quartz.ITrigger"/> with the given
        ///             name and group was found and removed from the store.
        /// </returns>
        public async Task<bool> ReplaceTriggerAsync(TriggerKey triggerKey, IOperableTrigger newTrigger)
        {
            var oldTrigger = await RetrieveTriggerAsync(triggerKey);

            var found = oldTrigger != null;

            if (found)
            {
                if (!oldTrigger.JobKey.Equals(newTrigger.JobKey))
                    throw new JobPersistenceException("New Trigger is not linked to the same job as the old trigger");

                await RemoveTriggerAsync(triggerKey, false);
                await StoreTriggerAsync(newTrigger, false);
            }

            return found;
        }

        /// <summary>
        /// Retrieve the given <see cref="T:Quartz.ITrigger"/>.
        /// </summary>
        /// <returns>
        /// The desired <see cref="T:Quartz.ITrigger"/>, or null if there is no
        ///             match.
        /// </returns>
        public async Task<IOperableTrigger> RetrieveTriggerAsync(TriggerKey triggerKey)
        {
            var triggerHashKey = redisJobStoreSchema.TriggerHashkey(triggerKey);

            var properties = await db.HashGetAllAsync(triggerHashKey);

            if (properties != null && properties.Any())
                return CreateTriggerFromProperties(triggerKey, ConvertToDictionaryString(properties));

            logger.LogWarning("trigger does not exist - {0}", triggerHashKey);
            return null;

        }

        /// <summary>
        /// Release triggers currently held by schedulers which have ceased to function e.g. crashed
        /// </summary>
        protected async Task ReleaseTriggersAsync()
        {
            var misfireTime = DateTimeOffset.UtcNow.DateTime.ToUnixTimeMilliSeconds();
            if (misfireTime - await GetLastTriggersReleaseTimeAsync() > triggerLockTimeout)
            {
                // it has been more than triggerLockTimeout minutes since we last released orphaned triggers
                await ReleaseOrphanedTriggersAsync(RedisTriggerState.Acquired, RedisTriggerState.Waiting);
                await ReleaseOrphanedTriggersAsync(RedisTriggerState.Blocked, RedisTriggerState.Waiting);
                await ReleaseOrphanedTriggersAsync(RedisTriggerState.PausedBlocked, RedisTriggerState.Paused);
                await SetLastTriggerReleaseTimeAsync(DateTimeOffset.UtcNow.DateTime.ToUnixTimeMilliSeconds());
            }
        }

        /// <summary>
        /// Get a handle to the next trigger to be fired, and mark it as 'reserved'
        ///             by the calling scheduler.
        /// </summary>
        /// <param name="noLaterThan">If &gt; 0, the JobStore should only return a Trigger
        ///             that will fire no later than the time represented in this value as
        ///             milliseconds.</param><param name="maxCount"/><param name="timeWindow"/>
        /// <returns/>
        /// <seealso cref="T:Quartz.ITrigger"/>
        public async Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggersAsync(DateTimeOffset noLaterThan, int maxCount, TimeSpan timeWindow)
        {
            await ReleaseTriggersAsync();
            var triggers = new List<IOperableTrigger>();
            var retry = false;

            do
            {
                retry = false;
                var acquiredJobHashKeysForNoConcurrentExec = new HashSet<string>();
                var score = ToUnixTimeMilliseconds(noLaterThan.Add(timeWindow));

                var waitingStateTriggers = await
                    db.SortedSetRangeByScoreWithScoresAsync(redisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Waiting), 0, score, Exclude.None, Order.Ascending, 0, maxCount);

                foreach (var sortedSetEntry in waitingStateTriggers)
                {
                    var trigger = await RetrieveTriggerAsync(redisJobStoreSchema.TriggerKey(sortedSetEntry.Element));

                    if (await ApplyMisfireAsync(trigger))
                    {
                        retry = true;
                        break;
                    }

                    if (trigger.GetNextFireTimeUtc() == null)
                    {
                        await UnsetTriggerStateAsync(sortedSetEntry.Element);
                        continue;
                    }

                    var jobHashKey = redisJobStoreSchema.JobHashKey(trigger.JobKey);
                    var job = await RetrieveJobAsync(trigger.JobKey);

                    if (job == null)
                    {
                        Logger.LogWarning("Could not find implementation for job {0}", trigger.JobKey);
                        continue;
                    }

                    if (job?.ConcurrentExecutionDisallowed == true)
                    {
                        if (acquiredJobHashKeysForNoConcurrentExec.Contains(jobHashKey))
                            continue;

                        acquiredJobHashKeysForNoConcurrentExec.Add(jobHashKey);
                    }

                    await LockTriggerAsync(trigger.Key);
                    await SetTriggerStateAsync(RedisTriggerState.Acquired, sortedSetEntry.Score, sortedSetEntry.Element);
                    triggers.Add(trigger);
                }
            } while (retry);

            return triggers;
        }

        /// <summary>
        /// Inform the <see cref="T:Quartz.Spi.IJobStore"/> that the scheduler no longer plans to
        ///             fire the given <see cref="T:Quartz.ITrigger"/>, that it had previously acquired
        ///             (reserved).
        /// </summary>
        public async Task ReleaseAcquiredTriggerAsync(IOperableTrigger trigger)
        {
            var triggerHashKey = redisJobStoreSchema.TriggerHashkey(trigger.Key);

            var score = await db.SortedSetScoreAsync(redisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Acquired), triggerHashKey);

            if (score.HasValue)
            {
                if (trigger.GetNextFireTimeUtc().HasValue)
                    await SetTriggerStateAsync(RedisTriggerState.Waiting, trigger.GetNextFireTimeUtc().Value.DateTime.ToUnixTimeMilliSeconds(), triggerHashKey);
                else
                    await UnsetTriggerStateAsync(triggerHashKey);
            }
        }

        /// <summary>
        /// Inform the <see cref="T:Quartz.Spi.IJobStore"/> that the scheduler is now firing the
        ///             given <see cref="T:Quartz.ITrigger"/> (executing its associated <see cref="T:Quartz.IJob"/>),
        ///             that it had previously acquired (reserved).
        /// </summary>
        /// <returns>
        /// May return null if all the triggers or their calendars no longer exist, or
        ///             if the trigger was not successfully put into the 'executing'
        ///             state.  Preference is to return an empty list if none of the triggers
        ///             could be fired.
        /// </returns>
        public abstract Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFiredAsync(IReadOnlyCollection<IOperableTrigger> triggers);

        /// <summary>
        /// Inform the <see cref="T:Quartz.Spi.IJobStore"/> that the scheduler has completed the
        ///             firing of the given <see cref="T:Quartz.ITrigger"/> (and the execution its
        ///             associated <see cref="T:Quartz.IJob"/>), and that the <see cref="T:Quartz.JobDataMap"/>
        ///             in the given <see cref="T:Quartz.IJobDetail"/> should be updated if the <see cref="T:Quartz.IJob"/>
        ///             is stateful.
        /// </summary>
        public abstract Task TriggeredJobCompleteAsync(IOperableTrigger trigger, IJobDetail jobDetail, SchedulerInstruction triggerInstCode);

        /// <summary>
        /// Retrieve the given <see cref="T:Quartz.ITrigger"/>.
        /// </summary>
        /// <param name="calName">The name of the <see cref="T:Quartz.ICalendar"/> to be retrieved.</param>
        /// <returns>
        /// The desired <see cref="T:Quartz.ICalendar"/>, or null if there is no
        ///             match.
        /// </returns>
        public async Task<ICalendar> RetrieveCalendarAsync(string calName)
        {
            var calendarHashKey = redisJobStoreSchema.CalendarHashKey(calName);
            ICalendar calendar = null;

            var calendarPropertiesInRedis = await db.HashGetAllAsync(calendarHashKey);

            if (calendarPropertiesInRedis != null && calendarPropertiesInRedis.Any()) 
            {
                var calendarProperties = ConvertToDictionaryString(calendarPropertiesInRedis);
                calendar = JsonConvert.DeserializeObject(calendarProperties[RedisJobStoreSchema.CalendarSerialized], serializerSettings) as ICalendar;
            }

            return calendar;
        }


        /// <summary>
        /// Get all of the Triggers that are associated to the given Job.
        /// </summary>
        /// <remarks>
        /// If there are no matches, a zero-length array should be returned.
        /// </remarks>
        public async Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJobAsync(JobKey jobKey)
        {
            var jobTriggerSetKey = redisJobStoreSchema.JobTriggersSetKey(jobKey);
            var triggerHashKeys = await db.SetMembersAsync(jobTriggerSetKey);

            var result = new List<IOperableTrigger>();
            foreach(var triggerHashKey in triggerHashKeys)
                result.Add(await RetrieveTriggerAsync(redisJobStoreSchema.TriggerKey(triggerHashKey)));

            return result;
        }

        /// <summary>
        /// Gets the paused trigger groups.
        /// </summary>
        /// <returns/>
        public async Task<IReadOnlyCollection<string>> GetPausedTriggerGroupsAsync()
        {
            var triggerGroupSetKeys = await db.SetMembersAsync(redisJobStoreSchema.PausedTriggerGroupsSetKey());
            var groups = new List<string>();

            foreach (var triggerGroupSetKey in triggerGroupSetKeys)
                groups.Add(redisJobStoreSchema.TriggerGroup(triggerGroupSetKey));

            return groups;

        }

        /// <summary>
        /// returns true if the given JobGroup is paused
        /// </summary>
        /// <param name="groupName"/>
        /// <returns/>
        public Task<bool> IsJobGroupPausedAsync(string groupName)
        {
            return db.SetContainsAsync(redisJobStoreSchema.PausedJobGroupsSetKey(), redisJobStoreSchema.JobGroupSetKey(groupName));
        }

        /// <summary>
        /// returns true if the given TriggerGroup
        ///             is paused
        /// </summary>
        /// <param name="groupName"/>
        /// <returns/>
        public Task<bool> IsTriggerGroupPausedAsync(string groupName)
        {
            return db.SetContainsAsync(redisJobStoreSchema.PausedTriggerGroupsSetKey(), redisJobStoreSchema.TriggerGroupSetKey(groupName));
        }

        /// <summary>
        /// Get the number of <see cref="T:Quartz.IJob"/>s that are
        ///             stored in the <see cref="T:Quartz.Spi.IJobStore"/>.
        /// </summary>
        /// <returns/>
        public async Task<int> NumberOfJobsAsync()
        {
            return (int)await db.SetLengthAsync(redisJobStoreSchema.JobsSetKey());
        }

        /// <summary>
        /// Get the number of <see cref="T:Quartz.ITrigger"/>s that are
        ///             stored in the <see cref="T:Quartz.Spi.IJobStore"/>.
        /// </summary>
        /// <returns/>
        public async Task<int> NumberOfTriggersAsync()
        {
            return (int)await db.SetLengthAsync(redisJobStoreSchema.TriggersSetKey());
        }

        /// <summary>
        /// Get the number of <see cref="T:Quartz.ICalendar"/> s that are
        ///             stored in the <see cref="T:Quartz.Spi.IJobStore"/>.
        /// </summary>
        /// <returns/>
        public async Task<int> NumberOfCalendarsAsync()
        {
            return (int)await db.SetLengthAsync(redisJobStoreSchema.CalendarsSetKey());
        }

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.IJob"/> s that
        ///             have the given group name.
        /// <para>
        /// If there are no jobs in the given group name, the result should be a
        ///             zero-length array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        /// <param name="matcher"/>
        /// <returns/>
        public abstract Task<IReadOnlyCollection<JobKey>> JobKeysAsync(GroupMatcher<JobKey> matcher);

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.ITrigger"/>s
        ///             that have the given group name.
        /// <para>
        /// If there are no triggers in the given group name, the result should be a
        ///             zero-length array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        public abstract Task<IReadOnlyCollection<TriggerKey>> TriggerKeysAsync(GroupMatcher<TriggerKey> matcher);

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.IJob"/>
        ///             groups.
        /// <para>
        /// If there are no known group names, the result should be a zero-length
        ///             array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        public async Task<IReadOnlyCollection<string>> JobGroupNamesAsync()
        {
            var groupsSet = await db.SetMembersAsync(redisJobStoreSchema.JobGroupsSetKey());
            return groupsSet.Select(g => redisJobStoreSchema.JobGroup(g)).ToList();
        }

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.ITrigger"/>
        ///             groups.
        /// <para>
        /// If there are no known group names, the result should be a zero-length
        ///             array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        public async Task<IReadOnlyCollection<string>> TriggerGroupNamesAsync()
        {
            var groupsSet = await db.SetMembersAsync(redisJobStoreSchema.TriggerGroupsSetKey());
            return groupsSet.Select(g => redisJobStoreSchema.TriggerGroup(g)).ToList();
        }

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.ICalendar"/> s
        ///             in the <see cref="T:Quartz.Spi.IJobStore"/>.
        /// <para>
        /// If there are no Calendars in the given group name, the result should be
        ///             a zero-length array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        public async Task<IReadOnlyCollection<string>> CalendarNamesAsync()
        {
            var calendarsSet = await db.SetMembersAsync(redisJobStoreSchema.CalendarsSetKey());
            return calendarsSet.Select(g => redisJobStoreSchema.GetCalendarName(g)).ToList();
        }

        /// <summary>
        /// Get the current state of the identified <see cref="T:Quartz.ITrigger"/>.
        /// </summary>
        /// <seealso cref="T:Quartz.TriggerState"/>
        public abstract Task<TriggerState> GetTriggerStateAsync(TriggerKey triggerKey);

        /// <summary>
        /// Pause all of the <see cref="T:Quartz.ITrigger"/>s in the
        ///             given group.
        /// </summary>
        /// <remarks>
        /// The JobStore should "remember" that the group is paused, and impose the
        ///             pause on any new triggers that are added to the group while the group is
        ///             paused.
        /// </remarks>
        public abstract Task<IReadOnlyCollection<string>> PauseTriggersAsync(GroupMatcher<TriggerKey> matcher);


        /// <summary>
        ///  Determine whether or not the given trigger has misfired.If so, notify {SchedulerSignaler} and update the trigger.
        /// </summary>
        /// <param name="trigger">IOperableTrigger</param>
        /// <returns>applied or not</returns>
        protected async Task<bool> ApplyMisfireAsync(IOperableTrigger trigger)
        {
            var misfireTime = DateTimeOffset.UtcNow.DateTime.ToUnixTimeMilliSeconds();
            var score = misfireTime;

            if (misfireThreshold > 0)
                misfireTime = misfireTime - misfireThreshold;

            //if the trigger has no next fire time or exceeds the misfirethreshold or enable ignore misfirepolicy
            // then dont apply misfire.
            var nextFireTime = trigger.GetNextFireTimeUtc();

            if (!nextFireTime.HasValue ||
               (nextFireTime.HasValue && nextFireTime.Value.DateTime.ToUnixTimeMilliSeconds() > misfireTime) ||
               trigger.MisfireInstruction == -1)
                return false;

            ICalendar calendar = null;

            if (!string.IsNullOrEmpty(trigger.CalendarName))
                calendar = await RetrieveCalendarAsync(trigger.CalendarName);

            await signaler.NotifyTriggerListenersMisfired((IOperableTrigger)trigger.Clone());

            trigger.UpdateAfterMisfire(calendar);

            await StoreTriggerAsync(trigger, true);

            if (nextFireTime.HasValue == false)
            {
                await SetTriggerStateAsync(RedisTriggerState.Completed, score, redisJobStoreSchema.TriggerHashkey(trigger.Key));
                await signaler.NotifySchedulerListenersFinalized(trigger);
            }
            else if (nextFireTime.Equals(trigger.GetNextFireTimeUtc()))
                return false;

            return true;
        }

        /// <summary>
        /// Check if the job identified by the given key exists in storage
        /// </summary>
        /// <param name="jobKey">Jobkey</param>
        /// <returns>exists or not</returns>
        public Task<bool> CheckExistsAsync(JobKey jobKey)
        {
            return db.KeyExistsAsync(redisJobStoreSchema.JobHashKey(jobKey));
        }


        /// <summary>
        /// Check if the calendar identified by the given name exists
        /// </summary>
        /// <param name="calName">Calendar Name</param>
        /// <returns>exists or not</returns>
        public Task<bool> CheckExistsAsync(string calName)
        {
            return db.KeyExistsAsync(redisJobStoreSchema.CalendarHashKey(calName));
        }


        /// <summary>
        /// Check if the trigger identified by the given key exists
        /// </summary>
        /// <param name="triggerKey">TriggerKey</param>
        /// <returns>exists or not</returns>
        public Task<bool> CheckExistsAsync(TriggerKey triggerKey)
        {
            return db.KeyExistsAsync(redisJobStoreSchema.TriggerHashkey(triggerKey));
        }

        /// <summary>
        /// delete all scheduling data - all jobs, triggers and calendars. Scheduler.Clear()
        /// </summary>
        public async Task ClearAllSchedulingData()
        {
            // delete jobs
            foreach (string jobHashKey in await db.SetMembersAsync(redisJobStoreSchema.JobsSetKey()))
                await RemoveJobAsync(redisJobStoreSchema.JobKey(jobHashKey));

            // delete triggers
            foreach (var triggerHashKey in await db.SetMembersAsync(redisJobStoreSchema.TriggersSetKey()))
                await RemoveTriggerAsync(redisJobStoreSchema.TriggerKey(triggerHashKey));

            // delete calendars
            foreach (var calHashName in await db.SetMembersAsync(redisJobStoreSchema.CalendarsSetKey()))
            {
                await db.KeyDeleteAsync(redisJobStoreSchema.CalendarTriggersSetKey(redisJobStoreSchema.GetCalendarName(calHashName)));
                await RemoveCalendarAsync(redisJobStoreSchema.GetCalendarName(calHashName));
            }

            await db.KeyDeleteAsync(redisJobStoreSchema.PausedTriggerGroupsSetKey());
            await db.KeyDeleteAsync(redisJobStoreSchema.PausedJobGroupsSetKey());
            await db.KeyDeleteAsync(redisJobStoreSchema.LastTriggerReleaseTime());
        }


        /// <summary>
        /// Retrieve the last time (in milliseconds) that orphaned triggers were released
        /// </summary>
        /// <returns>time in milli seconds from epoch time</returns>
        protected async Task<double> GetLastTriggersReleaseTimeAsync()
        {
            var lastReleaseTime = await db.StringGetAsync(redisJobStoreSchema.LastTriggerReleaseTime());

            if (string.IsNullOrEmpty(lastReleaseTime))
                return 0;

            return double.Parse(lastReleaseTime, CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Set the last time at which orphaned triggers were released
        /// </summary>
        /// <param name="time">time in milli seconds from epoch time</param>
        protected Task SetLastTriggerReleaseTimeAsync(double time)
        {
            return db.StringSetAsync(redisJobStoreSchema.LastTriggerReleaseTime(), time);
        }

        /// <summary>
        /// Set a trigger state by adding the trigger to the relevant sorted set, using its next fire time as the score.
        /// </summary>
        /// <param name="state">RedisTriggerState</param>
        /// <param name="score">time in milli seconds from epoch time</param>
        /// <param name="triggerHashKey">TriggerHashKey</param>
        /// <returns>succeeds or not</returns>
        protected async Task<bool> SetTriggerStateAsync(RedisTriggerState state, double score, string triggerHashKey)
        {
            await UnsetTriggerStateAsync(triggerHashKey);
            return await db.SortedSetAddAsync(redisJobStoreSchema.TriggerStateSetKey(state), triggerHashKey, score);
        }

        /// <summary>
        /// convert JobDataMap to hashEntry array
        /// </summary>
        /// <param name="jobDataMap">JobDataMap</param>
        /// <returns>HashEntry[]</returns>
        protected HashEntry[] ConvertToHashEntries(JobDataMap jobDataMap)
        {
            var entries = new List<HashEntry>();
            if (jobDataMap != null)
                entries.AddRange(jobDataMap.Where(entry => entry.Value != null).Select(entry => new HashEntry(entry.Key, entry.Value.ToString())));

            return entries.ToArray();
        }

        /// <summary>
        /// convert hashEntry array to Dictionary 
        /// </summary>
        /// <param name="entries">HashEntry[]</param>
        /// <returns>IDictionary{string, string}</returns>
        protected IDictionary<string, string> ConvertToDictionaryString(HashEntry[] entries)
        {
            var stringMap = new Dictionary<string, string>();
            if (entries != null)
            {
                foreach (var entry in entries)
                    stringMap.Add(entry.Name, entry.Value.ToString());
            }
            return stringMap;
        }


        /// <summary>
        /// convert IJobDetail to HashEntry array
        /// </summary>
        /// <param name="jobDetail">JobDetail</param>
        /// <returns>Array of <see cref="HashEntry"/></returns>
        protected HashEntry[] ConvertToHashEntries(IJobDetail jobDetail)
        {
            var entries = new List<HashEntry>
                {
                    new HashEntry(RedisJobStoreSchema.JobClass, jobDetail.JobType.AssemblyQualifiedName),
                    new HashEntry(RedisJobStoreSchema.Description, jobDetail.Description ?? ""),
                    new HashEntry(RedisJobStoreSchema.IsDurable, jobDetail.Durable),
                    new HashEntry(RedisJobStoreSchema.RequestRecovery,jobDetail.RequestsRecovery),
                    new HashEntry(RedisJobStoreSchema.BlockedBy, ""),
                    new HashEntry(RedisJobStoreSchema.BlockTime, "")
                };

            return entries.ToArray();
        }

        /// <summary>
        /// convert trigger to HashEntry array
        /// </summary>
        /// <param name="trigger">Trigger</param>
        /// <returns>Array of <see cref="HashEntry"/></returns>
        protected HashEntry[] ConvertToHashEntries(ITrigger trigger)
        {
            var operableTrigger = trigger as IOperableTrigger;
            if (operableTrigger == null)
                throw new InvalidCastException("trigger needs to be IOperable");

            var entries = new List<HashEntry>
                {
                    new HashEntry(RedisJobStoreSchema.JobHash, redisJobStoreSchema.JobHashKey(operableTrigger.JobKey)),
                    new HashEntry(RedisJobStoreSchema.Description, operableTrigger.Description ?? ""),
                    new HashEntry(RedisJobStoreSchema.NextFireTime, operableTrigger.GetNextFireTimeUtc().HasValue? operableTrigger.GetNextFireTimeUtc().Value.DateTime.ToUnixTimeMilliSeconds().ToString():""),
                    new HashEntry(RedisJobStoreSchema.PrevFireTime, operableTrigger.GetPreviousFireTimeUtc().HasValue? operableTrigger.GetPreviousFireTimeUtc().Value.DateTime.ToUnixTimeMilliSeconds().ToString():""),
                    new HashEntry(RedisJobStoreSchema.Priority, operableTrigger.Priority),
                    new HashEntry(RedisJobStoreSchema.StartTime, operableTrigger.StartTimeUtc.DateTime.ToUnixTimeMilliSeconds().ToString()),
                    new HashEntry(RedisJobStoreSchema.EndTime, operableTrigger.EndTimeUtc.HasValue?operableTrigger.EndTimeUtc.Value.DateTime.ToUnixTimeMilliSeconds().ToString():""),
                    new HashEntry(RedisJobStoreSchema.FinalFireTime, operableTrigger.FinalFireTimeUtc.HasValue?operableTrigger.FinalFireTimeUtc.Value.DateTime.ToUnixTimeMilliSeconds().ToString():""),
                    new HashEntry(RedisJobStoreSchema.FireInstanceId, operableTrigger.FireInstanceId ?? string.Empty),
                    new HashEntry(RedisJobStoreSchema.MisfireInstruction, operableTrigger.MisfireInstruction),
                    new HashEntry(RedisJobStoreSchema.CalendarName, operableTrigger.CalendarName ?? string.Empty)
                };

            if (operableTrigger is ISimpleTrigger simpleTrigger)
            {
                entries.Add(new HashEntry(RedisJobStoreSchema.TriggerType, RedisJobStoreSchema.TriggerTypeSimple));
                entries.Add(new HashEntry(RedisJobStoreSchema.RepeatCount, simpleTrigger.RepeatCount));
                entries.Add(new HashEntry(RedisJobStoreSchema.RepeatInterval, simpleTrigger.RepeatInterval.ToString()));
                entries.Add(new HashEntry(RedisJobStoreSchema.TimesTriggered, simpleTrigger.TimesTriggered));
            }
            else if (operableTrigger is ICronTrigger cronTrigger)
            {
                entries.Add(new HashEntry(RedisJobStoreSchema.TriggerType, RedisJobStoreSchema.TriggerTypeCron));
                entries.Add(new HashEntry(RedisJobStoreSchema.CronExpression, cronTrigger.CronExpressionString));
                entries.Add(new HashEntry(RedisJobStoreSchema.TimeZoneId, cronTrigger.TimeZone.Id));
            }

            return entries.ToArray();
        }

        /// <summary>
        /// convert Calendar to HashEntry array
        /// </summary>
        /// <param name="calendar"></param>
        /// <returns></returns>
        protected HashEntry[] ConvertToHashEntries(ICalendar calendar)
        {
            var entries = new List<HashEntry>
                {
                    new HashEntry(RedisJobStoreSchema.CalendarSerialized, JsonConvert.SerializeObject(calendar, serializerSettings))
                };

            return entries.ToArray();
        }

        /// <summary>
        /// Lock the trigger with the given key to the current job scheduler
        /// </summary>
        /// <param name="triggerKey">TriggerKey</param>
        /// <returns>succeed or not</returns>
        protected Task<bool> LockTriggerAsync(TriggerKey triggerKey)
        {
            return db.StringSetAsync(redisJobStoreSchema.TriggerLockKey(triggerKey), schedulerInstanceId, TimeSpan.FromSeconds(triggerLockTimeout));
        }

        #region private methods

        /// <summary>
        /// convert trigger properties into different IOperableTrigger object (simple or cron trigger)
        /// </summary>
        /// <param name="triggerKey">TriggerKey</param>
        /// <param name="properties">trigger's properties</param>
        /// <returns>IOperableTrigger</returns>
        IOperableTrigger CreateTriggerFromProperties(TriggerKey triggerKey, IDictionary<string, string> properties)
        {
            var type = properties[RedisJobStoreSchema.TriggerType];

            if (string.IsNullOrEmpty(type))
                return null;

            if (type == RedisJobStoreSchema.TriggerTypeSimple)
            {
                var simpleTrigger = new SimpleTriggerImpl();

                if (!string.IsNullOrEmpty(properties[RedisJobStoreSchema.RepeatCount]))
                    simpleTrigger.RepeatCount = Convert.ToInt32(properties[RedisJobStoreSchema.RepeatCount]);

                if (!string.IsNullOrEmpty(properties[RedisJobStoreSchema.RepeatInterval]))
                    simpleTrigger.RepeatInterval = TimeSpan.Parse(properties[RedisJobStoreSchema.RepeatInterval]);

                if (!string.IsNullOrEmpty(properties[RedisJobStoreSchema.TimesTriggered]))
                    simpleTrigger.TimesTriggered = Convert.ToInt32(properties[RedisJobStoreSchema.TimesTriggered]);

                PopulateTrigger(triggerKey, properties, simpleTrigger);

                return simpleTrigger;
            }
            else
            {
                var cronTrigger = new CronTriggerImpl();

                if (!string.IsNullOrEmpty(properties[RedisJobStoreSchema.TimeZoneId]))
                    cronTrigger.TimeZone = TimeZoneInfo.FindSystemTimeZoneById(properties[RedisJobStoreSchema.TimeZoneId]);

                if (!string.IsNullOrEmpty(properties[RedisJobStoreSchema.CronExpression]))
                    cronTrigger.CronExpressionString = properties[RedisJobStoreSchema.CronExpression];

                PopulateTrigger(triggerKey, properties, cronTrigger);

                return cronTrigger;
            }
        }

        /// <summary>
        /// populate common properties of a trigger.
        /// </summary>
        /// <param name="triggerKey">triggerKey</param>
        /// <param name="properties">trigger's properties</param>
        /// <param name="trigger">IOperableTrigger</param>
        void PopulateTrigger(TriggerKey triggerKey, IDictionary<string, string> properties, IOperableTrigger trigger)
        {
            trigger.Key = triggerKey;
            var (name, group) = redisJobStoreSchema.Split(properties[RedisJobStoreSchema.JobHash]);
            trigger.JobKey = new JobKey(name, group);
            trigger.Description = properties[RedisJobStoreSchema.Description];
            trigger.FireInstanceId = properties[RedisJobStoreSchema.FireInstanceId];
            trigger.CalendarName = properties[RedisJobStoreSchema.CalendarName];
            trigger.Priority = int.Parse(properties[RedisJobStoreSchema.Priority]);
            trigger.MisfireInstruction = int.Parse(properties[RedisJobStoreSchema.MisfireInstruction]);
            trigger.StartTimeUtc = DateTimeFromUnixTimestampMillis(double.Parse(properties[RedisJobStoreSchema.StartTime]));

            trigger.EndTimeUtc = string.IsNullOrEmpty(properties[RedisJobStoreSchema.EndTime])
                                      ? default(DateTimeOffset?)
                                      : DateTimeFromUnixTimestampMillis(double.Parse(properties[RedisJobStoreSchema.EndTime]));

            if (trigger is AbstractTrigger)
            {
                trigger.SetNextFireTimeUtc(
                    string.IsNullOrEmpty(properties[RedisJobStoreSchema.NextFireTime])
                        ? default(DateTimeOffset?)
                        : DateTimeFromUnixTimestampMillis(double.Parse(properties[RedisJobStoreSchema.NextFireTime]))
                );

                trigger.SetPreviousFireTimeUtc(
                    string.IsNullOrEmpty(properties[RedisJobStoreSchema.PrevFireTime])
                        ? default(DateTimeOffset?)
                        : DateTimeFromUnixTimestampMillis(double.Parse(properties[RedisJobStoreSchema.PrevFireTime]))
                );
            }
        }

        /// <summary>
        /// convert to utc datetime
        /// </summary>
        /// <param name="millis">milliseconds</param>
        /// <returns>datetime in utc</returns>
        static DateTime DateTimeFromUnixTimestampMillis(double millis)
        {
            return unixEpoch.AddMilliseconds(millis);
        }

        /// <summary>
        /// get the total milli seconds from unix epoch time.
        /// </summary>
        /// <param name="dateTimeOffset">DateTimeOffset</param>
        /// <returns>total millisconds from epoch time</returns>
        public double ToUnixTimeMilliseconds(DateTimeOffset dateTimeOffset)
        {
            // Truncate sub-millisecond precision before offsetting by the Unix Epoch to avoid
            // the last digit being off by one for dates that result in negative Unix times
            return (dateTimeOffset - new DateTimeOffset(unixEpoch)).TotalMilliseconds;
        }

        /// <summary>
        /// try to acquire a redis lock.
        /// </summary>
        /// <returns>locked or not</returns>
        async Task<(bool locked, string lockValue)> TryLockAsync(CancellationToken cancellationToken)
        {
            var guid = Guid.NewGuid().ToString();
            var lockacquired = await db.LockTakeAsync(redisJobStoreSchema.LockKey, guid, TimeSpan.FromMilliseconds(redisLockTimeout));
            cancellationToken.ThrowIfCancellationRequested();
            return (lockacquired, lockacquired ? guid : null);
        }

        /// <summary>
        /// try to acquire a lock with retry
        /// if acquire fails, then retry till it succeeds.
        /// </summary>
        public async Task<string> LockWithWait(CancellationToken cancellationToken)
        {
            (bool locked, string lockValue) tuple;
            while (!(tuple = await TryLockAsync(cancellationToken)).locked)
            {
                try
                {
                    logger.LogInformation("waiting for redis lock");
                    await Task.Delay(RandomInt(75, 125));
                }
                catch (ThreadInterruptedException ex)
                {
                    logger.LogError(ex, "errored out on waiting for a lock");
                }
            }
            return tuple.lockValue;
        }

        /// <summary>
        /// get a random integer within the specified bounds.
        /// </summary>
        /// <param name="min">mininum number</param>
        /// <param name="max">maxinum number></param>
        /// <returns>random number</returns>
        protected int RandomInt(int min, int max)
        {
            return new Random().Next((max - min) + 1) + min;
        }

        /// <summary>
        /// release the redis lock. 
        /// </summary>
        /// <returns>unlock succeeds or not</returns>
        public Task<bool> UnlockAsync(string lockValue)
        {
            return db.LockReleaseAsync(redisJobStoreSchema.LockKey, lockValue);
        }

        /// <summary>
        /// return the logger for the current class
        /// </summary>
        protected ILogger Logger
        {
            get { return logger; }
        }
        #endregion
    }
}
