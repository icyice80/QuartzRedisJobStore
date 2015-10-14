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
using log4net;
using System.Threading;

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
        private readonly ILog _logger;

        /// <summary>
        /// Utc datetime of Epoch.
        /// </summary>
        private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// RedisJobStoreSchema
        /// </summary>
        protected readonly RedisJobStoreSchema RedisJobStoreSchema;

        /// <summary>
        /// ISchedulerSignaler
        /// </summary>
        protected readonly ISchedulerSignaler SchedulerSignaler;

        /// <summary>
        /// threshold for the misfire (measured in milliseconds)
        /// </summary>
        protected int MisfireThreshold = 60000;

        /// <summary>
        /// redis db.
        /// </summary>
        protected IDatabase Db;

        /// <summary>
        /// Triggerlock time out here we need to make sure the longest job should not exceed this amount of time, otherwise we need to increase it. 
        /// </summary>
        protected int TriggerLockTimeout;

        /// <summary>
        /// lockValue
        /// </summary>
        protected string LockValue;

        /// <summary>
        /// redis lock time out in milliseconds.
        /// </summary>
        protected int RedisLockTimeout;

        /// <summary>
        /// scheduler instance id
        /// </summary>
        protected readonly string SchedulerInstanceId;

        /// <summary>
        /// JsonSerializerSettings
        /// </summary>
        private readonly JsonSerializerSettings _serializerSettings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.All, DateTimeZoneHandling = DateTimeZoneHandling.Utc, NullValueHandling = NullValueHandling.Ignore, ContractResolver = new CamelCasePropertyNamesContractResolver() };

        /// <summary>
        /// constructor
        /// </summary>
        /// <param name="redisJobStoreSchema">RedisJobStoreSchema</param>
        /// <param name="db">IDatabase</param>
        /// <param name="signaler">ISchedulerSignaler</param>
        /// <param name="schedulerInstanceId">schedulerInstanceId</param>
        /// <param name="triggerLockTimeout">Trigger lock timeout(number in miliseconds) used in releasing the orphan triggers.</param>
        /// <param name="redisLockTimeout">Redis Lock timeout (number in miliseconds)</param>
        protected BaseJobStorage(RedisJobStoreSchema redisJobStoreSchema, IDatabase db, ISchedulerSignaler signaler, string schedulerInstanceId, int triggerLockTimeout, int redisLockTimeout)
        {
            RedisJobStoreSchema = redisJobStoreSchema;
            Db = db;
            SchedulerSignaler = signaler;
            SchedulerInstanceId = schedulerInstanceId;
            _logger = LogManager.GetLogger(GetType());
            TriggerLockTimeout = triggerLockTimeout;
            RedisLockTimeout = redisLockTimeout;
        }


        /// <summary>
        /// Store the given <see cref="T:Quartz.IJobDetail"/>.
        /// </summary>
        /// <param name="jobDetail">The <see cref="T:Quartz.IJobDetail"/> to be stored.</param><param name="replaceExisting">If <see langword="true"/>, any <see cref="T:Quartz.IJob"/> existing in the
        ///             <see cref="T:Quartz.Spi.IJobStore"/> with the same name and group should be
        ///             over-written.
        ///             </param>
        public abstract void StoreJob(IJobDetail jobDetail, Boolean replaceExisting);

        /// <summary>
        /// Retrieve the <see cref="T:Quartz.IJobDetail"/> for the given
        ///             <see cref="T:Quartz.IJob"/>.
        /// </summary>
        /// <returns>
        /// The desired <see cref="T:Quartz.IJob"/>, or null if there is no match.
        /// </returns>
        public IJobDetail RetrieveJob(JobKey jobKey)
        {
            var jobHashKey = RedisJobStoreSchema.JobHashKey(jobKey);

            HashEntry[] jobDetails = Db.HashGetAll(jobHashKey);

            if (jobDetails == null || jobDetails.Count() == 0)
            {
                return null;
            }

            var jobDataMapHashKey = RedisJobStoreSchema.JobDataMapHashKey(jobKey);

            HashEntry[] jobDataMap = Db.HashGetAll(jobDataMapHashKey);

            var jobProperties = ConvertToDictionaryString(jobDetails);

            var jobBuilder =
                JobBuilder.Create(Type.GetType(jobProperties[RedisJobStoreSchema.JobClass]))
                          .WithIdentity(jobKey)
                          .WithDescription(jobProperties[RedisJobStoreSchema.Description])
                          .RequestRecovery(Convert.ToBoolean(Convert.ToInt16(jobProperties[RedisJobStoreSchema.RequestRecovery])))
                          .StoreDurably(Convert.ToBoolean(Convert.ToInt16(jobProperties[RedisJobStoreSchema.IsDurable])));


            if (jobDataMap != null && jobDataMap.Count() > 0)
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
        public abstract void StoreTrigger(ITrigger trigger, bool replaceExisting);

        /// <summary>
        /// remove the trigger state from all the possible sorted set.
        /// </summary>
        /// <param name="triggerHashKey">TriggerHashKey</param>
        /// <returns>succeeds or not</returns>
        public abstract bool UnsetTriggerState(string triggerHashKey);


        /// <summary>
        /// Store the given <see cref="T:Quartz.ICalendar"/>.
        /// </summary>
        /// <param name="name">The name.</param><param name="calendar">The <see cref="T:Quartz.ICalendar"/> to be stored.</param><param name="replaceExisting">If <see langword="true"/>, any <see cref="T:Quartz.ICalendar"/> existing
        ///             in the <see cref="T:Quartz.Spi.IJobStore"/> with the same name and group
        ///             should be over-written.</param><param name="updateTriggers">If <see langword="true"/>, any <see cref="T:Quartz.ITrigger"/>s existing
        ///             in the <see cref="T:Quartz.Spi.IJobStore"/> that reference an existing
        ///             Calendar with the same name with have their next fire time
        ///             re-computed with the new <see cref="T:Quartz.ICalendar"/>.</param><throws>ObjectAlreadyExistsException </throws>
        public abstract void StoreCalendar(String name, ICalendar calendar, bool replaceExisting, bool updateTriggers);

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
        public abstract bool RemoveCalendar(String calendarName);

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
        public abstract bool RemoveJob(JobKey jobKey);

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
        public abstract IList<string> PauseJobs(GroupMatcher<JobKey> matcher);

        /// <summary>
        /// Resume (un-pause) the <see cref="T:Quartz.IJob"/> with the
        ///             given key.
        /// <para>
        /// If any of the <see cref="T:Quartz.IJob"/>'s<see cref="T:Quartz.ITrigger"/> s missed one
        ///             or more fire-times, then the <see cref="T:Quartz.ITrigger"/>'s misfire
        ///             instruction will be applied.
        /// </para>
        /// </summary>
        public void ResumeJob(JobKey jobKey)
        {
            foreach (var trigger in GetTriggersForJob(jobKey))
            {
                ResumeTrigger(trigger.Key);
            }
        }

        /// <summary>
        /// Pause the <see cref="T:Quartz.IJob"/> with the given key - by
        ///             pausing all of its current <see cref="T:Quartz.ITrigger"/>s.
        /// </summary>
        public void PauseJob(JobKey jobKey)
        {
            foreach (var trigger in GetTriggersForJob(jobKey))
            {
                PauseTrigger(trigger.Key);
            }
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
        public abstract global::Quartz.Collection.ISet<string> ResumeJobs(GroupMatcher<JobKey> matcher);


        /// <summary>
        /// Resume (un-pause) the <see cref="T:Quartz.ITrigger"/> with the
        ///             given key.
        /// <para>
        /// If the <see cref="T:Quartz.ITrigger"/> missed one or more fire-times, then the
        ///             <see cref="T:Quartz.ITrigger"/>'s misfire instruction will be applied.
        /// </para>
        /// </summary>
        /// <seealso cref="T:System.String"/>
        public abstract void ResumeTrigger(TriggerKey triggerKey);

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
        public abstract bool RemoveTrigger(TriggerKey triggerKey, bool removeNonDurableJob = true);

        /// <summary>
        /// Resume (un-pause) all of the <see cref="T:Quartz.ITrigger"/>s
        ///             in the given group.
        /// <para>
        /// If any <see cref="T:Quartz.ITrigger"/> missed one or more fire-times, then the
        ///             <see cref="T:Quartz.ITrigger"/>'s misfire instruction will be applied.
        /// </para>
        /// </summary>
        public abstract IList<string> ResumeTriggers(GroupMatcher<TriggerKey> matcher);

        /// <summary>
        /// Pause the <see cref="T:Quartz.ITrigger"/> with the given key.
        /// </summary>
        public abstract void PauseTrigger(TriggerKey triggerKey);


        /// <summary>
        /// Pause all triggers - equivalent of calling <see cref="M:Quartz.Spi.IJobStore.PauseTriggers(Quartz.Impl.Matchers.GroupMatcher{Quartz.TriggerKey})"/>
        ///             on every group.
        /// <para>
        /// When <see cref="M:Quartz.Spi.IJobStore.ResumeAll"/> is called (to un-pause), trigger misfire
        ///             instructions WILL be applied.
        /// </para>
        /// </summary>
        /// <seealso cref="M:Quartz.Spi.IJobStore.ResumeAll"/>
        public void PauseAllTriggers()
        {
            RedisValue[] triggerGroups = Db.SetMembers(RedisJobStoreSchema.TriggerGroupsSetKey());
            foreach (var group in triggerGroups)
            {
                PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals(RedisJobStoreSchema.TriggerGroup(group)));
            }
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
        public void ResumeAllTriggers()
        {
            RedisValue[] triggerGroups = Db.SetMembers(RedisJobStoreSchema.TriggerGroupsSetKey());
            foreach (var group in triggerGroups)
            {
                ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals(RedisJobStoreSchema.TriggerGroup(group)));
            }
        }

        /// <summary>
        ///  Release triggers from the given current state to the new state if its locking scheduler has not registered as alive in the last triggerlocktimeout 
        /// </summary>
        /// <param name="currentState"></param>
        /// <param name="newState"></param>
        protected void ReleaseOrphanedTriggers(RedisTriggerState currentState, RedisTriggerState newState)
        {
            SortedSetEntry[] triggers = Db.SortedSetRangeByScoreWithScores(RedisJobStoreSchema.TriggerStateSetKey(currentState), 0, -1);

            foreach (var sortedSetEntry in triggers)
            {
                string lockedId =
                    Db.StringGet(
                        RedisJobStoreSchema.TriggerLockKey(
                            RedisJobStoreSchema.TriggerKey(sortedSetEntry.Element.ToString())));
                // Lock key has expired. We can safely alter the trigger's state.
                if (string.IsNullOrEmpty(lockedId))
                {
                    SetTriggerState(newState, sortedSetEntry.Score, sortedSetEntry.Element);
                }
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
        public bool ReplaceTrigger(TriggerKey triggerKey, IOperableTrigger newTrigger)
        {
            var oldTrigger = RetrieveTrigger(triggerKey);

            bool found = oldTrigger != null;

            if (found)
            {
                if (!oldTrigger.JobKey.Equals(newTrigger.JobKey))
                {
                    throw new JobPersistenceException("New Trigger is not linked to the same job as the old trigger");
                }

                RemoveTrigger(triggerKey, false);
                StoreTrigger(newTrigger, false);
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
        public IOperableTrigger RetrieveTrigger(TriggerKey triggerKey)
        {
            var triggerHashKey = RedisJobStoreSchema.TriggerHashkey(triggerKey);

            var properties = Db.HashGetAll(triggerHashKey);

            if (properties != null && properties.Count() > 0)
            {
                return RetrieveTrigger(triggerKey, ConvertToDictionaryString(properties));
            }

            _logger.WarnFormat("trigger does not exist - {0}", triggerHashKey);
            return null;

        }

        /// <summary>
        /// Release triggers currently held by schedulers which have ceased to function e.g. crashed
        /// </summary>
        protected void ReleaseTriggers()
        {
            double misfireTime = DateTimeOffset.UtcNow.DateTime.ToUnixTimeMilliSeconds();
            if (misfireTime - GetLastTriggersReleaseTime() > TriggerLockTimeout)
            {
                // it has been more than triggerLockTimeout minutes since we last released orphaned triggers
                ReleaseOrphanedTriggers(RedisTriggerState.Acquired, RedisTriggerState.Waiting);
                ReleaseOrphanedTriggers(RedisTriggerState.Blocked, RedisTriggerState.Waiting);
                ReleaseOrphanedTriggers(RedisTriggerState.PausedBlocked, RedisTriggerState.Paused);
                SetLastTriggerReleaseTime(DateTimeOffset.UtcNow.DateTime.ToUnixTimeMilliSeconds());
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
        public IList<IOperableTrigger> AcquireNextTriggers(DateTimeOffset noLaterThan, int maxCount, TimeSpan timeWindow)
        {
            ReleaseTriggers();

            var triggers = new List<IOperableTrigger>();

            bool retry = false;

            do
            {
                var acquiredJobHashKeysForNoConcurrentExec = new global::Quartz.Collection.HashSet<string>();

                var score = ToUnixTimeMilliseconds(noLaterThan.Add(timeWindow));

                var waitingStateTriggers =
                    Db.SortedSetRangeByScoreWithScores(
                        RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Waiting), 0, score,
                        Exclude.None, Order.Ascending, 0, maxCount);
                foreach (var sortedSetEntry in waitingStateTriggers)
                {

                    var trigger = RetrieveTrigger(RedisJobStoreSchema.TriggerKey(sortedSetEntry.Element));

                    if (ApplyMisfire(trigger))
                    {
                        retry = true;
                        break;
                    }

                    if (trigger.GetNextFireTimeUtc() == null)
                    {
                        this.UnsetTriggerState(sortedSetEntry.Element);
                        continue;
                    }

                    var jobHashKey = RedisJobStoreSchema.JobHashKey(trigger.JobKey);

                    var job = RetrieveJob(trigger.JobKey);

                    if (job != null && job.ConcurrentExecutionDisallowed)
                    {
                        if (acquiredJobHashKeysForNoConcurrentExec.Contains(jobHashKey))
                        {
                            continue;
                        }
                        acquiredJobHashKeysForNoConcurrentExec.Add(jobHashKey);
                    }

                    LockTrigger(trigger.Key);
                    SetTriggerState(RedisTriggerState.Acquired,
                                         sortedSetEntry.Score, sortedSetEntry.Element);
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
        public void ReleaseAcquiredTrigger(IOperableTrigger trigger)
        {
            var triggerHashKey = RedisJobStoreSchema.TriggerHashkey(trigger.Key);

            var score =
                Db.SortedSetScore(RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Acquired),
                                        triggerHashKey);

            if (score.HasValue)
            {
                if (trigger.GetNextFireTimeUtc().HasValue)
                {
                    SetTriggerState(RedisTriggerState.Waiting,
                                         trigger.GetNextFireTimeUtc().Value.DateTime.ToUnixTimeMilliSeconds(), triggerHashKey);
                }
                else
                {
                    this.UnsetTriggerState(triggerHashKey);
                }
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
        public abstract IList<TriggerFiredResult> TriggersFired(IList<IOperableTrigger> triggers);

        /// <summary>
        /// Inform the <see cref="T:Quartz.Spi.IJobStore"/> that the scheduler has completed the
        ///             firing of the given <see cref="T:Quartz.ITrigger"/> (and the execution its
        ///             associated <see cref="T:Quartz.IJob"/>), and that the <see cref="T:Quartz.JobDataMap"/>
        ///             in the given <see cref="T:Quartz.IJobDetail"/> should be updated if the <see cref="T:Quartz.IJob"/>
        ///             is stateful.
        /// </summary>
        public abstract void TriggeredJobComplete(IOperableTrigger trigger, IJobDetail jobDetail,
                                                  SchedulerInstruction triggerInstCode);

        /// <summary>
        /// Retrieve the given <see cref="T:Quartz.ITrigger"/>.
        /// </summary>
        /// <param name="calName">The name of the <see cref="T:Quartz.ICalendar"/> to be retrieved.</param>
        /// <returns>
        /// The desired <see cref="T:Quartz.ICalendar"/>, or null if there is no
        ///             match.
        /// </returns>
        public ICalendar RetrieveCalendar(string calName)
        {
            var calendarHashKey = RedisJobStoreSchema.CalendarHashKey(calName);
            ICalendar calendar = null;

            HashEntry[] calendarPropertiesInRedis = Db.HashGetAll(calendarHashKey);

            if(calendarPropertiesInRedis != null && calendarPropertiesInRedis.Count()>0) {

                var calendarProperties = ConvertToDictionaryString(calendarPropertiesInRedis);

                calendar =
                    JsonConvert.DeserializeObject(calendarProperties[RedisJobStoreSchema.CalendarSerialized],
                                                  _serializerSettings) as ICalendar;
            }

            return calendar;
        }


        /// <summary>
        /// Get all of the Triggers that are associated to the given Job.
        /// </summary>
        /// <remarks>
        /// If there are no matches, a zero-length array should be returned.
        /// </remarks>
        public IList<IOperableTrigger> GetTriggersForJob(JobKey jobKey)
        {
            var jobTriggerSetKey = RedisJobStoreSchema.JobTriggersSetKey(jobKey);
            var triggerHashKeys = Db.SetMembers(jobTriggerSetKey);

            return triggerHashKeys.Select(triggerHashKey => RetrieveTrigger(RedisJobStoreSchema.TriggerKey(triggerHashKey))).ToList();
        }

        /// <summary>
        /// Gets the paused trigger groups.
        /// </summary>
        /// <returns/>
        public global::Quartz.Collection.ISet<string> GetPausedTriggerGroups()
        {
            RedisValue[] triggerGroupSetKeys =
                Db.SetMembers(RedisJobStoreSchema.PausedTriggerGroupsSetKey());

            var groups = new global::Quartz.Collection.HashSet<string>();

            foreach (var triggerGroupSetKey in triggerGroupSetKeys)
            {
                groups.Add(RedisJobStoreSchema.TriggerGroup(triggerGroupSetKey));
            }

            return groups;

        }

        /// <summary>
        /// returns true if the given JobGroup is paused
        /// </summary>
        /// <param name="groupName"/>
        /// <returns/>
        public bool IsJobGroupPaused(string groupName)
        {
            return
                Db.SetContains(RedisJobStoreSchema.PausedJobGroupsSetKey(),
                                     RedisJobStoreSchema.JobGroupSetKey(groupName));
        }

        /// <summary>
        /// returns true if the given TriggerGroup
        ///             is paused
        /// </summary>
        /// <param name="groupName"/>
        /// <returns/>
        public bool IsTriggerGroupPaused(string groupName)
        {
            return
                Db.SetContains(RedisJobStoreSchema.PausedTriggerGroupsSetKey(),
                                    RedisJobStoreSchema.TriggerGroupSetKey(groupName));
        }

        /// <summary>
        /// Get the number of <see cref="T:Quartz.IJob"/>s that are
        ///             stored in the <see cref="T:Quartz.Spi.IJobStore"/>.
        /// </summary>
        /// <returns/>
        public int NumberOfJobs()
        {
            return (int)Db.SetLength(RedisJobStoreSchema.JobsSetKey());
        }

        /// <summary>
        /// Get the number of <see cref="T:Quartz.ITrigger"/>s that are
        ///             stored in the <see cref="T:Quartz.Spi.IJobStore"/>.
        /// </summary>
        /// <returns/>
        public int NumberOfTriggers()
        {
            return (int)Db.SetLength(RedisJobStoreSchema.TriggersSetKey());
        }

        /// <summary>
        /// Get the number of <see cref="T:Quartz.ICalendar"/> s that are
        ///             stored in the <see cref="T:Quartz.Spi.IJobStore"/>.
        /// </summary>
        /// <returns/>
        public int NumberOfCalendars()
        {
            return (int)Db.SetLength(RedisJobStoreSchema.CalendarsSetKey());
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
        public abstract global::Quartz.Collection.ISet<JobKey> JobKeys(GroupMatcher<JobKey> matcher);

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.ITrigger"/>s
        ///             that have the given group name.
        /// <para>
        /// If there are no triggers in the given group name, the result should be a
        ///             zero-length array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        public abstract global::Quartz.Collection.ISet<TriggerKey> TriggerKeys(GroupMatcher<TriggerKey> matcher);

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.IJob"/>
        ///             groups.
        /// <para>
        /// If there are no known group names, the result should be a zero-length
        ///             array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        public IList<string> JobGroupNames()
        {
            RedisValue[] groupsSet = Db.SetMembers(RedisJobStoreSchema.JobGroupsSetKey());

            return groupsSet.Select(g => RedisJobStoreSchema.JobGroup(g)).ToList();
        }

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.ITrigger"/>
        ///             groups.
        /// <para>
        /// If there are no known group names, the result should be a zero-length
        ///             array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        public IList<string> TriggerGroupNames()
        {
            RedisValue[] groupsSet = Db.SetMembers(RedisJobStoreSchema.TriggerGroupsSetKey());

            return groupsSet.Select(g => RedisJobStoreSchema.TriggerGroup(g)).ToList();
        }

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.ICalendar"/> s
        ///             in the <see cref="T:Quartz.Spi.IJobStore"/>.
        /// <para>
        /// If there are no Calendars in the given group name, the result should be
        ///             a zero-length array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        public IList<string> CalendarNames()
        {
            RedisValue[] calendarsSet = Db.SetMembers(RedisJobStoreSchema.CalendarsSetKey());

            return calendarsSet.Select(g => RedisJobStoreSchema.GetCalendarName(g)).ToList();
        }

        /// <summary>
        /// Get the current state of the identified <see cref="T:Quartz.ITrigger"/>.
        /// </summary>
        /// <seealso cref="T:Quartz.TriggerState"/>
        public abstract TriggerState GetTriggerState(TriggerKey triggerKey);

        /// <summary>
        /// Pause all of the <see cref="T:Quartz.ITrigger"/>s in the
        ///             given group.
        /// </summary>
        /// <remarks>
        /// The JobStore should "remember" that the group is paused, and impose the
        ///             pause on any new triggers that are added to the group while the group is
        ///             paused.
        /// </remarks>
        public abstract IList<string> PauseTriggers(GroupMatcher<TriggerKey> matcher);


        /// <summary>
        ///  Determine whether or not the given trigger has misfired.If so, notify {SchedulerSignaler} and update the trigger.
        /// </summary>
        /// <param name="trigger">IOperableTrigger</param>
        /// <returns>applied or not</returns>
        protected bool ApplyMisfire(IOperableTrigger trigger)
        {
            double misfireTime = DateTimeOffset.UtcNow.DateTime.ToUnixTimeMilliSeconds();
            double score = misfireTime;

            if (MisfireThreshold > 0)
            {
                misfireTime = misfireTime - MisfireThreshold;
            }

            //if the trigger has no next fire time or exceeds the misfirethreshold or enable ignore misfirepolicy
            // then dont apply misfire.
            DateTimeOffset? nextFireTime = trigger.GetNextFireTimeUtc();

            if (nextFireTime.HasValue == false ||
               (nextFireTime.HasValue && nextFireTime.Value.DateTime.ToUnixTimeMilliSeconds() > misfireTime) ||
               trigger.MisfireInstruction == -1)
            {
                return false;
            }

            ICalendar calendar = null;

            if (!string.IsNullOrEmpty(trigger.CalendarName))
            {
                calendar = RetrieveCalendar(trigger.CalendarName);
            }

            SchedulerSignaler.NotifyTriggerListenersMisfired((IOperableTrigger)trigger.Clone());

            trigger.UpdateAfterMisfire(calendar);

            StoreTrigger(trigger, true);

            if (nextFireTime.HasValue == false)
            {
                SetTriggerState(RedisTriggerState.Completed,
                                     score, RedisJobStoreSchema.TriggerHashkey(trigger.Key));
                SchedulerSignaler.NotifySchedulerListenersFinalized(trigger);
            }
            else if (nextFireTime.Equals(trigger.GetNextFireTimeUtc()))
            {
                return false;
            }
            return true;
        }

        /// <summary>
        /// Check if the job identified by the given key exists in storage
        /// </summary>
        /// <param name="jobKey">Jobkey</param>
        /// <returns>exists or not</returns>
        public bool CheckExists(JobKey jobKey)
        {
            return Db.KeyExists(RedisJobStoreSchema.JobHashKey(jobKey));
        }


        /// <summary>
        /// Check if the calendar identified by the given name exists
        /// </summary>
        /// <param name="calName">Calendar Name</param>
        /// <returns>exists or not</returns>
        public bool CheckExists(string calName)
        {
            return Db.KeyExists(RedisJobStoreSchema.CalendarHashKey(calName));
        }


        /// <summary>
        /// Check if the trigger identified by the given key exists
        /// </summary>
        /// <param name="triggerKey">TriggerKey</param>
        /// <returns>exists or not</returns>
        public bool CheckExists(TriggerKey triggerKey)
        {
            return Db.KeyExists(RedisJobStoreSchema.TriggerHashkey(triggerKey));
        }

        /// <summary>
        /// delete all scheduling data - all jobs, triggers and calendars. Scheduler.Clear()
        /// </summary>
        public void ClearAllSchedulingData()
        {
            // delete triggers
            foreach (string jobHashKey in
                Db.SetMembers(RedisJobStoreSchema.JobsSetKey()))
            {
                RemoveJob(RedisJobStoreSchema.JobKey(jobHashKey));
            }

            foreach (var triggerHashKey in Db.SetMembers(RedisJobStoreSchema.TriggersSetKey()))
            {
                RemoveTrigger(RedisJobStoreSchema.TriggerKey(triggerHashKey));
            }

            foreach (var calHashName in Db.SetMembers(RedisJobStoreSchema.CalendarsSetKey()))
            {
                Db.KeyDelete(RedisJobStoreSchema.CalendarTriggersSetKey(RedisJobStoreSchema.GetCalendarName(calHashName)));
                RemoveCalendar(RedisJobStoreSchema.GetCalendarName(calHashName));
            }

            Db.KeyDelete(RedisJobStoreSchema.PausedTriggerGroupsSetKey());
            Db.KeyDelete(RedisJobStoreSchema.PausedJobGroupsSetKey());
        }


        /// <summary>
        /// Retrieve the last time (in milliseconds) that orphaned triggers were released
        /// </summary>
        /// <returns>time in milli seconds from epoch time</returns>
        protected double GetLastTriggersReleaseTime()
        {
            var lastReleaseTime = Db.StringGet(RedisJobStoreSchema.LastTriggerReleaseTime());

            if (string.IsNullOrEmpty(lastReleaseTime))
            {
                return 0;
            }
            return double.Parse(lastReleaseTime);
        }

        /// <summary>
        /// Set the last time at which orphaned triggers were released
        /// </summary>
        /// <param name="time">time in milli seconds from epoch time</param>
        protected void SetLastTriggerReleaseTime(double time)
        {
            Db.StringSet(RedisJobStoreSchema.LastTriggerReleaseTime(), time);
        }

        /// <summary>
        /// Set a trigger state by adding the trigger to the relevant sorted set, using its next fire time as the score.
        /// </summary>
        /// <param name="state">RedisTriggerState</param>
        /// <param name="score">time in milli seconds from epoch time</param>
        /// <param name="triggerHashKey">TriggerHashKey</param>
        /// <returns>succeeds or not</returns>
        protected bool SetTriggerState(RedisTriggerState state, double score, string triggerHashKey)
        {
            this.UnsetTriggerState(triggerHashKey);
            return Db.SortedSetAdd(RedisJobStoreSchema.TriggerStateSetKey(state), triggerHashKey, score);
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
            {
                entries.AddRange(jobDataMap.Select(entry => new HashEntry(entry.Key, entry.Value.ToString())));
            }
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
                {
                    stringMap.Add(entry.Name, entry.Value.ToString());
                }
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
            {
                throw new InvalidCastException("trigger needs to be IOperable");
            }

            var entries = new List<HashEntry>
                {
                    new HashEntry(RedisJobStoreSchema.JobHash, this.RedisJobStoreSchema.JobHashKey(operableTrigger.JobKey)),
                    new HashEntry(RedisJobStoreSchema.Description, operableTrigger.Description ?? ""),
                    new HashEntry(RedisJobStoreSchema.NextFireTime, operableTrigger.GetNextFireTimeUtc().HasValue? operableTrigger.GetNextFireTimeUtc().Value.DateTime.ToUnixTimeMilliSeconds().ToString():""),
                    new HashEntry(RedisJobStoreSchema.PrevFireTime, operableTrigger.GetPreviousFireTimeUtc().HasValue? operableTrigger.GetPreviousFireTimeUtc().Value.DateTime.ToUnixTimeMilliSeconds().ToString():""),
                    new HashEntry(RedisJobStoreSchema.Priority, operableTrigger.Priority),
                    new HashEntry(RedisJobStoreSchema.StartTime,operableTrigger.StartTimeUtc.DateTime.ToUnixTimeMilliSeconds().ToString()),
                    new HashEntry(RedisJobStoreSchema.EndTime, operableTrigger.EndTimeUtc.HasValue?operableTrigger.EndTimeUtc.Value.DateTime.ToUnixTimeMilliSeconds().ToString():""),
                    new HashEntry(RedisJobStoreSchema.FinalFireTime,operableTrigger.FinalFireTimeUtc.HasValue?operableTrigger.FinalFireTimeUtc.Value.DateTime.ToUnixTimeMilliSeconds().ToString():""),
                    new HashEntry(RedisJobStoreSchema.FireInstanceId,operableTrigger.FireInstanceId??string.Empty),
                    new HashEntry(RedisJobStoreSchema.MisfireInstruction,operableTrigger.MisfireInstruction),
                    new HashEntry(RedisJobStoreSchema.CalendarName,operableTrigger.CalendarName??string.Empty)
                };

            if (operableTrigger is ISimpleTrigger)
            {
                entries.Add(new HashEntry(RedisJobStoreSchema.TriggerType, RedisJobStoreSchema.TriggerTypeSimple));
                entries.Add(new HashEntry(RedisJobStoreSchema.RepeatCount, ((ISimpleTrigger)operableTrigger).RepeatCount));
                entries.Add(new HashEntry(RedisJobStoreSchema.RepeatInterval, ((ISimpleTrigger)operableTrigger).RepeatInterval.ToString()));
                entries.Add(new HashEntry(RedisJobStoreSchema.TimesTriggered, ((ISimpleTrigger)operableTrigger).TimesTriggered));
            }
            else if (operableTrigger is ICronTrigger)
            {
                entries.Add(new HashEntry(RedisJobStoreSchema.TriggerType, RedisJobStoreSchema.TriggerTypeCron));
                entries.Add(new HashEntry(RedisJobStoreSchema.CronExpression, ((ICronTrigger)operableTrigger).CronExpressionString));
                entries.Add(new HashEntry(RedisJobStoreSchema.TimeZoneId, ((ICronTrigger)operableTrigger).TimeZone.Id));
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
                    new HashEntry(RedisJobStoreSchema.CalendarSerialized, JsonConvert.SerializeObject(calendar,_serializerSettings))
                };

            return entries.ToArray();
        }

        /// <summary>
        /// Lock the trigger with the given key to the current job scheduler
        /// </summary>
        /// <param name="triggerKey">TriggerKey</param>
        /// <returns>succeed or not</returns>
        protected bool LockTrigger(TriggerKey triggerKey)
        {
            return Db.StringSet(RedisJobStoreSchema.TriggerLockKey(triggerKey), SchedulerInstanceId, TimeSpan.FromSeconds(TriggerLockTimeout));
        }


        #region private methods

        /// <summary>
        /// convert trigger properties into different IOperableTrigger object (simple or cron trigger)
        /// </summary>
        /// <param name="triggerKey">TriggerKey</param>
        /// <param name="properties">trigger's properties</param>
        /// <returns>IOperableTrigger</returns>
        private IOperableTrigger RetrieveTrigger(TriggerKey triggerKey, IDictionary<string, string> properties)
        {
            var type = properties[RedisJobStoreSchema.TriggerType];

            if (string.IsNullOrEmpty(type))
            {
                return null;
            }


            if (type == RedisJobStoreSchema.TriggerTypeSimple)
            {
                var simpleTrigger = new SimpleTriggerImpl();

                if (!string.IsNullOrEmpty(properties[RedisJobStoreSchema.RepeatCount]))
                {
                    simpleTrigger.RepeatCount = Convert.ToInt32(properties[RedisJobStoreSchema.RepeatCount]);

                }

                if (!string.IsNullOrEmpty(properties[RedisJobStoreSchema.RepeatInterval]))
                {
                    simpleTrigger.RepeatInterval = TimeSpan.Parse(properties[RedisJobStoreSchema.RepeatInterval]);
                }

                if (!string.IsNullOrEmpty(properties[RedisJobStoreSchema.TimesTriggered]))
                {
                    simpleTrigger.TimesTriggered = Convert.ToInt32(properties[RedisJobStoreSchema.TimesTriggered]);
                }

                PopulateTrigger(triggerKey, properties, simpleTrigger);

                return simpleTrigger;

            }
            else
            {

                var cronTrigger = new CronTriggerImpl();

                if (!string.IsNullOrEmpty(properties[RedisJobStoreSchema.TimeZoneId]))
                {
                    cronTrigger.TimeZone =
                        TimeZoneInfo.FindSystemTimeZoneById(properties[RedisJobStoreSchema.TimeZoneId]);
                }
                if (!string.IsNullOrEmpty(properties[RedisJobStoreSchema.CronExpression]))
                {
                    cronTrigger.CronExpressionString = properties[RedisJobStoreSchema.CronExpression];
                }


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
        private void PopulateTrigger(TriggerKey triggerKey, IDictionary<string, string> properties, IOperableTrigger trigger)
        {
            trigger.Key = triggerKey;
            var jobGroupName = RedisJobStoreSchema.Split(properties[RedisJobStoreSchema.JobHash]);
            trigger.JobKey = new JobKey(jobGroupName[2], jobGroupName[1]);
            trigger.Description = properties[RedisJobStoreSchema.Description];
            trigger.FireInstanceId = properties[RedisJobStoreSchema.FireInstanceId];
            trigger.CalendarName = properties[RedisJobStoreSchema.CalendarName];
            trigger.Priority = int.Parse(properties[RedisJobStoreSchema.Priority]);
            trigger.MisfireInstruction = int.Parse(properties[RedisJobStoreSchema.MisfireInstruction]);
            trigger.StartTimeUtc = DateTimeFromUnixTimestampMillis(
                                           double.Parse(properties[RedisJobStoreSchema.StartTime]));

            trigger.EndTimeUtc = string.IsNullOrEmpty(properties[RedisJobStoreSchema.EndTime])
                                      ? default(DateTimeOffset?)
                                      : DateTimeFromUnixTimestampMillis(
                                          double.Parse(properties[RedisJobStoreSchema.EndTime]));

            var baseTrigger = trigger as AbstractTrigger;

            if (baseTrigger != null)
            {
                trigger.SetNextFireTimeUtc(string.IsNullOrEmpty(properties[RedisJobStoreSchema.NextFireTime])
                                      ? default(DateTimeOffset?)
                                      : DateTimeFromUnixTimestampMillis(
                                          double.Parse(properties[RedisJobStoreSchema.NextFireTime])));

                trigger.SetPreviousFireTimeUtc(string.IsNullOrEmpty(properties[RedisJobStoreSchema.PrevFireTime])
                                      ? default(DateTimeOffset?)
                                      : DateTimeFromUnixTimestampMillis(
                                          double.Parse(properties[RedisJobStoreSchema.PrevFireTime])));
            }


        }

        /// <summary>
        /// convert to utc datetime
        /// </summary>
        /// <param name="millis">milliseconds</param>
        /// <returns>datetime in utc</returns>
        private static DateTime DateTimeFromUnixTimestampMillis(double millis)
        {
            return UnixEpoch.AddMilliseconds(millis);
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
            return (dateTimeOffset - new DateTimeOffset(UnixEpoch)).TotalMilliseconds;
        }

        /// <summary>
        /// try to acquire a redis lock.
        /// </summary>
        /// <returns>locked or not</returns>
        private bool Lock()
        {
            var guid = Guid.NewGuid().ToString();

            var lockacquired = Db.LockTake(RedisJobStoreSchema.LockKey, guid, TimeSpan.FromMilliseconds(RedisLockTimeout));
            if (lockacquired)
            {
                LockValue = guid;
            }
            return lockacquired;
        }

        /// <summary>
        /// try to acquire a lock with retry
        /// if acquire fails, then retry till it succeeds.
        /// </summary>
        public void LockWithWait()
        {
            while (!Lock())
            {
                try
                {
                    _logger.Info("waiting for redis lock");
                    Thread.Sleep(RandomInt(75, 125));
                }
                catch (ThreadInterruptedException ex)
                {
                    _logger.ErrorFormat("errored out on waiting for a lock", ex);
                }
            }

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
        public bool Unlock()
        {
            var key = RedisJobStoreSchema.LockKey;

            return Db.LockRelease(key, LockValue);
        }

        /// <summary>
        /// return the logger for the current class
        /// </summary>
        protected ILog Logger
        {
            get { return _logger; }
        }

        #endregion

    }
}
