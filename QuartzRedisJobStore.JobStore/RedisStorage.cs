using System;
using System.Collections.Generic;
using System.Linq;
using Quartz;
using Quartz.Impl.Matchers;
using Quartz.Spi;
using StackExchange.Redis;

namespace QuartzRedisJobStore.JobStore
{
    /// <summary>
    /// Master/slave redis storage for job, trigger, calendar, scheduler related operations.
    /// </summary>
    public class RedisStorage : BaseJobStorage
    {

        #region constructor
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="redisJobStoreSchema">RedisJobStoreSchema</param>
        /// <param name="db">IDatabase</param>
        /// <param name="signaler">ISchedulerSignaler</param>
        /// <param name="schedulerInstanceId">SchedulerInstanceId</param>
        /// <param name="triggerLockTimeout">triggerLockTimeout</param>
        /// <param name="redisLockTimeout">redisLockTimeout</param>
        public RedisStorage(RedisJobStoreSchema redisJobStoreSchema, IDatabase db, ISchedulerSignaler signaler, string schedulerInstanceId, int triggerLockTimeout, int redisLockTimeout) : base(redisJobStoreSchema, db, signaler, schedulerInstanceId, triggerLockTimeout, redisLockTimeout) { }

        #endregion

        #region Overrides of BaseJobStorage
        /// <summary>
        /// Store the given <see cref="T:Quartz.IJobDetail"/>.
        /// </summary>
        /// <param name="jobDetail">The <see cref="T:Quartz.IJobDetail"/> to be stored.</param><param name="replaceExisting">If <see langword="true"/>, any <see cref="T:Quartz.IJob"/> existing in the
        ///             <see cref="T:Quartz.Spi.IJobStore"/> with the same name and group should be
        ///             over-written.
        ///             </param>
        public override void StoreJob(IJobDetail jobDetail, bool replaceExisting)
        {
            var jobHashKey = RedisJobStoreSchema.JobHashKey(jobDetail.Key);
            var jobDataMapHashKey = RedisJobStoreSchema.JobDataMapHashKey(jobDetail.Key);
            var jobGroupSetKey = RedisJobStoreSchema.JobGroupSetKey(jobDetail.Key.Group);

            if (Db.KeyExists(jobHashKey) && !replaceExisting)
            {
                throw new ObjectAlreadyExistsException(jobDetail);
            }

            Db.HashSet(jobHashKey, ConvertToHashEntries(jobDetail));

            Db.HashSet(jobDataMapHashKey, ConvertToHashEntries(jobDetail.JobDataMap));

            Db.SetAdd(RedisJobStoreSchema.JobsSetKey(), jobHashKey);

            Db.SetAdd(RedisJobStoreSchema.JobGroupsSetKey(), jobGroupSetKey);

            Db.SetAdd(jobGroupSetKey, jobHashKey);

        }

        /// <summary>
        /// Store the given <see cref="T:Quartz.ITrigger"/>.
        /// </summary>
        /// <param name="trigger">The <see cref="T:Quartz.ITrigger"/> to be stored.</param><param name="replaceExisting">If <see langword="true"/>, any <see cref="T:Quartz.ITrigger"/> existing in
        ///             the <see cref="T:Quartz.Spi.IJobStore"/> with the same name and group should
        ///             be over-written.</param><throws>ObjectAlreadyExistsException </throws>
        public override void StoreTrigger(ITrigger trigger, bool replaceExisting)
        {
            var triggerHashKey = RedisJobStoreSchema.TriggerHashkey(trigger.Key);
            var triggerGroupSetKey = RedisJobStoreSchema.TriggerGroupSetKey(trigger.Key.Group);
            var jobTriggerSetKey = RedisJobStoreSchema.JobTriggersSetKey(trigger.JobKey);

            if (((trigger is ISimpleTrigger) == false) && ((trigger is ICronTrigger) == false))
            {
                throw new NotImplementedException("Unknown trigger, only SimpleTrigger and CronTrigger are supported");
            }

            var triggerExists = Db.KeyExists(triggerHashKey);

            if (triggerExists && replaceExisting == false)
            {
                throw new ObjectAlreadyExistsException(trigger);
            }


            Db.HashSet(triggerHashKey, ConvertToHashEntries(trigger));
            Db.SetAdd(RedisJobStoreSchema.TriggersSetKey(), triggerHashKey);
            Db.SetAdd(RedisJobStoreSchema.TriggerGroupsSetKey(), triggerGroupSetKey);
            Db.SetAdd(triggerGroupSetKey, triggerHashKey);
            Db.SetAdd(jobTriggerSetKey, triggerHashKey);

            if(!string.IsNullOrEmpty(trigger.CalendarName)) {
                var calendarTriggersSetKey = RedisJobStoreSchema.CalendarTriggersSetKey(trigger.CalendarName);
                Db.SetAdd(calendarTriggersSetKey, triggerHashKey);
            }

            //if trigger already exists, remove it from all the possible states. 
            if (triggerExists)
            {
                this.UnsetTriggerState(triggerHashKey);
            }

            //then update it with the new state in the its respectvie sorted set. 
            UpdateTriggerState(trigger);
        }

        /// <summary>
        /// remove the trigger from all the possible state in the its respective sorted set. 
        /// </summary>
        /// <param name="triggerHashKey">trigger hash key</param>
        /// <returns>succeeds or not</returns>
        public override bool UnsetTriggerState(string triggerHashKey)
        {


            var removedList = (from RedisTriggerState state in Enum.GetValues(typeof(RedisTriggerState)) select Db.SortedSetRemove(RedisJobStoreSchema.TriggerStateSetKey(state), triggerHashKey)).ToList();

            if (removedList.Any(x => x))
            {
                return Db.KeyDelete(
                    RedisJobStoreSchema.TriggerLockKey(RedisJobStoreSchema.TriggerKey(triggerHashKey)));
            }

            return false;
        }


        /// <summary>
        /// Store the given <see cref="T:Quartz.ICalendar"/>.
        /// </summary>
        /// <param name="name">The name.</param><param name="calendar">The <see cref="T:Quartz.ICalendar"/> to be stored.</param><param name="replaceExisting">If <see langword="true"/>, any <see cref="T:Quartz.ICalendar"/> existing
        ///             in the <see cref="T:Quartz.Spi.IJobStore"/> with the same name and group
        ///             should be over-written.</param><param name="updateTriggers">If <see langword="true"/>, any <see cref="T:Quartz.ITrigger"/>s existing
        ///             in the <see cref="T:Quartz.Spi.IJobStore"/> that reference an existing
        ///             Calendar with the same name with have their next fire time
        ///             re-computed with the new <see cref="T:Quartz.ICalendar"/>.</param><throws>ObjectAlreadyExistsException </throws>
        public override void StoreCalendar(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers)
        {
            string calendarHashKey = RedisJobStoreSchema.CalendarHashKey(name);

            if (replaceExisting == false && Db.KeyExists(calendarHashKey))
            {
                throw new ObjectAlreadyExistsException(string.Format("Calendar with key {0} already exists", calendarHashKey));
            }

            Db.HashSet(calendarHashKey, ConvertToHashEntries(calendar));
            Db.SetAdd(RedisJobStoreSchema.CalendarsSetKey(), calendarHashKey);

            if (updateTriggers)
            {
                var calendarTriggersSetkey = RedisJobStoreSchema.CalendarTriggersSetKey(name);

                var triggerHashKeys = Db.SetMembers(calendarTriggersSetkey);

                foreach (var triggerHashKey in triggerHashKeys)
                {
                    var trigger = RetrieveTrigger(RedisJobStoreSchema.TriggerKey(triggerHashKey));

                    trigger.UpdateWithNewCalendar(calendar, TimeSpan.FromSeconds(MisfireThreshold));

                    StoreTrigger(trigger, true);
                }
            }

        }

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
        public override bool RemoveCalendar(string calendarName)
        {
            var calendarTriggersSetKey = RedisJobStoreSchema.CalendarTriggersSetKey(calendarName);

            if (Db.SetLength(calendarTriggersSetKey) > 0)
            {
                throw new JobPersistenceException(string.Format("There are triggers are using calendar {0}",
                                                                calendarName));
            }

            var calendarHashKey = RedisJobStoreSchema.CalendarHashKey(calendarName);

            return Db.KeyDelete(calendarHashKey) && Db.SetRemove(RedisJobStoreSchema.CalendarsSetKey(), calendarHashKey);
        }

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
        public override bool RemoveJob(JobKey jobKey)
        {
            var jobHashKey = RedisJobStoreSchema.JobHashKey(jobKey);
            var jobDataMapHashKey = RedisJobStoreSchema.JobDataMapHashKey(jobKey);
            var jobGroupSetKey = RedisJobStoreSchema.JobGroupSetKey(jobKey.Group);
            var jobTriggerSetKey = RedisJobStoreSchema.JobTriggersSetKey(jobKey);

            var delJobHashKeyResult = Db.KeyDelete(jobHashKey);

            Db.KeyDelete(jobDataMapHashKey);

            Db.SetRemove(RedisJobStoreSchema.JobsSetKey(), jobHashKey);

            Db.SetRemove(jobGroupSetKey, jobHashKey);

            var jobTriggerSetResult = Db.SetMembers(jobTriggerSetKey);

            Db.KeyDelete(jobTriggerSetKey);

            var jobGroupSetLengthResult = Db.SetLength(jobGroupSetKey);

            if (jobGroupSetLengthResult == 0)
            {
                Db.SetRemoveAsync(RedisJobStoreSchema.JobGroupsSetKey(), jobGroupSetKey);
            }

            // remove all triggers associated with this job
            foreach (var triggerHashKey in jobTriggerSetResult)
            {
                var triggerkey = RedisJobStoreSchema.TriggerKey(triggerHashKey);
                var triggerGroupKey = RedisJobStoreSchema.TriggerGroupSetKey(triggerkey.Group);

                this.UnsetTriggerState(triggerHashKey);

                Db.SetRemove(RedisJobStoreSchema.TriggersSetKey(), triggerHashKey);

                Db.SetRemove(RedisJobStoreSchema.TriggerGroupsSetKey(), triggerGroupKey);

                Db.SetRemove(RedisJobStoreSchema.TriggerGroupSetKey(triggerkey.Group), triggerHashKey);

                Db.KeyDelete(triggerHashKey.ToString());
            }

            return delJobHashKeyResult;
        }


        /// <summary>
        /// Pause the <see cref="T:Quartz.IJob"/> with the given key - by
        ///             pausing all of its current <see cref="T:Quartz.ITrigger"/>s.
        /// </summary>
        public override IList<string> PauseJobs(GroupMatcher<JobKey> matcher)
        {
            var pausedJobGroups = new List<string>();

            if (matcher.CompareWithOperator.Equals(StringOperator.Equality))
            {
                var jobGroupSetKey = RedisJobStoreSchema.JobGroupSetKey(matcher.CompareToValue);

                if (Db.SetAdd(RedisJobStoreSchema.PausedJobGroupsSetKey(), jobGroupSetKey))
                {
                    pausedJobGroups.Add(RedisJobStoreSchema.JobGroup(jobGroupSetKey));

                    foreach (RedisValue val in Db.SetMembers(jobGroupSetKey))
                    {
                        PauseJob(RedisJobStoreSchema.JobKey(val));
                    }
                }
            }
            else
            {
                var jobGroupSets = Db.SetMembers(RedisJobStoreSchema.JobGroupsSetKey());

                var jobGroups = jobGroupSets.Where(jobGroupSet => matcher.CompareWithOperator.Evaluate(RedisJobStoreSchema.JobGroup(jobGroupSet), matcher.CompareToValue)).ToDictionary<RedisValue, string, RedisValue[]>(jobGroupSet => jobGroupSet, jobGroupSet => Db.SetMembers(jobGroupSet.ToString()));

                foreach (var jobGroup in jobGroups)
                {
                    if (Db.SetAdd(RedisJobStoreSchema.PausedJobGroupsSetKey(), jobGroup.Key))
                    {
                        pausedJobGroups.Add(RedisJobStoreSchema.JobGroup(jobGroup.Key));

                        foreach (var jobHashKey in jobGroup.Value)
                        {
                            PauseJob(RedisJobStoreSchema.JobKey(jobHashKey));
                        }
                    }
                }
            }

            return pausedJobGroups;
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
        public override global::Quartz.Collection.ISet<string> ResumeJobs(GroupMatcher<JobKey> matcher)
        {
            var resumedJobGroups = new List<string>();

            if (matcher.CompareWithOperator.Equals(StringOperator.Equality))
            {
                var jobGroupSetKey = RedisJobStoreSchema.JobGroupSetKey(matcher.CompareToValue);

                var removedPausedResult = Db.SetRemove(RedisJobStoreSchema.PausedJobGroupsSetKey(), jobGroupSetKey);
                var jobsResult = Db.SetMembers(jobGroupSetKey);


                if (removedPausedResult)
                {
                    resumedJobGroups.Add(RedisJobStoreSchema.JobGroup(jobGroupSetKey));
                }

                foreach (var job in jobsResult)
                {
                    ResumeJob(RedisJobStoreSchema.JobKey(job));
                }
            }
            else
            {
                foreach (var jobGroupSetKey in Db.SetMembers(RedisJobStoreSchema.JobGroupsSetKey()))
                {
                    if (matcher.CompareWithOperator.Evaluate(RedisJobStoreSchema.JobGroup(jobGroupSetKey),
                                                            matcher.CompareToValue))
                    {

                        resumedJobGroups.AddRange(ResumeJobs(
                                GroupMatcher<JobKey>.GroupEquals(RedisJobStoreSchema.JobGroup(jobGroupSetKey))));
                    }
                }
            }

            return new global::Quartz.Collection.HashSet<string>(resumedJobGroups);
        }

        /// <summary>
        /// Resume (un-pause) the <see cref="T:Quartz.ITrigger"/> with the
        ///             given key.
        /// <para>
        /// If the <see cref="T:Quartz.ITrigger"/> missed one or more fire-times, then the
        ///             <see cref="T:Quartz.ITrigger"/>'s misfire instruction will be applied.
        /// </para>
        /// </summary>
        /// <seealso cref="T:System.String"/>
        public override void ResumeTrigger(TriggerKey triggerKey)
        {
            var triggerHashKey = RedisJobStoreSchema.TriggerHashkey(triggerKey);

            var triggerExists = Db.SetContains(RedisJobStoreSchema.TriggersSetKey(), triggerHashKey);

            var isPausedTrigger =
                Db.SortedSetScore(RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Paused),
                                         triggerHashKey);

            var isPausedBlockedTrigger =
                Db.SortedSetScore(RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.PausedBlocked),
                                         triggerHashKey);

            if (triggerExists == false)
            {
                return;
            }

            //Trigger is not paused, cant be resumed then.
            if (!isPausedTrigger.HasValue && !isPausedBlockedTrigger.HasValue)
            {
                return;
            }

            var trigger = RetrieveTrigger(triggerKey);

            var jobHashKey = RedisJobStoreSchema.JobHashKey(trigger.JobKey);

            var nextFireTime = trigger.GetNextFireTimeUtc();

            if (nextFireTime.HasValue)
            {

                if (Db.SetContains(RedisJobStoreSchema.BlockedJobsSet(), jobHashKey))
                {
                    SetTriggerState(RedisTriggerState.Blocked, nextFireTime.Value.DateTime.ToUnixTimeMilliSeconds(), triggerHashKey);
                }
                else
                {
                    SetTriggerState(RedisTriggerState.Waiting, nextFireTime.Value.DateTime.ToUnixTimeMilliSeconds(), triggerHashKey);
                }
            }
            ApplyMisfire(trigger);
        }

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
        public override bool RemoveTrigger(TriggerKey triggerKey, bool removeNonDurableJob = true)
        {
            var triggerHashKey = RedisJobStoreSchema.TriggerHashkey(triggerKey);

            if (!Db.KeyExists(triggerHashKey))
            {
                return false;
            }

            IOperableTrigger trigger = RetrieveTrigger(triggerKey);

            var triggerGroupSetKey = RedisJobStoreSchema.TriggerGroupSetKey(triggerKey.Group);
            var jobHashKey = RedisJobStoreSchema.JobHashKey(trigger.JobKey);
            var jobTriggerSetkey = RedisJobStoreSchema.JobTriggersSetKey(trigger.JobKey);

            Db.SetRemove(RedisJobStoreSchema.TriggersSetKey(), triggerHashKey);

            Db.SetRemove(triggerGroupSetKey, triggerHashKey);

            Db.SetRemove(jobTriggerSetkey, triggerHashKey);

            if (Db.SetLength(triggerGroupSetKey) == 0)
            {
                Db.SetRemove(RedisJobStoreSchema.TriggerGroupsSetKey(), triggerGroupSetKey);
            }

            if (removeNonDurableJob)
            {

                var jobTriggerSetKeyLengthResult = Db.SetLength(jobTriggerSetkey);

                var jobExistsResult = Db.KeyExists(jobHashKey);

                if (jobTriggerSetKeyLengthResult == 0 && jobExistsResult)
                {
                    var job = RetrieveJob(trigger.JobKey);

                    if (job.Durable == false)
                    {
                        RemoveJob(job.Key);
                        SchedulerSignaler.NotifySchedulerListenersJobDeleted(job.Key);
                    }
                }
            }

            if (!string.IsNullOrEmpty(trigger.CalendarName))
            {
                Db.SetRemove(RedisJobStoreSchema.CalendarTriggersSetKey(trigger.CalendarName), triggerHashKey);
            }

            this.UnsetTriggerState(triggerHashKey);
            return Db.KeyDelete(triggerHashKey);
        }

        /// <summary>
        /// Resume (un-pause) all of the <see cref="T:Quartz.ITrigger"/>s
        ///             in the given group.
        /// <para>
        /// If any <see cref="T:Quartz.ITrigger"/> missed one or more fire-times, then the
        ///             <see cref="T:Quartz.ITrigger"/>'s misfire instruction will be applied.
        /// </para>
        /// </summary>
        public override IList<string> ResumeTriggers(GroupMatcher<TriggerKey> matcher)
        {
            var resumedTriggerGroups = new List<string>();

            if (matcher.CompareWithOperator.Equals(StringOperator.Equality))
            {
                var triggerGroupSetKey =
                    RedisJobStoreSchema.TriggerGroupSetKey(matcher.CompareToValue);

                Db.SetRemove(RedisJobStoreSchema.PausedTriggerGroupsSetKey(), triggerGroupSetKey);

                var triggerHashKeysResult = Db.SetMembers(triggerGroupSetKey);

                foreach (var triggerHashKey in triggerHashKeysResult)
                {
                    var trigger = RetrieveTrigger(RedisJobStoreSchema.TriggerKey(triggerHashKey));

                    ResumeTrigger(trigger.Key);

                    if(!resumedTriggerGroups.Contains(trigger.Key.Group)) {
                        resumedTriggerGroups.Add(trigger.Key.Group);
                    }
                }
            }
            else
            {
                foreach (var triggerGroupSetKy in Db.SetMembersAsync(RedisJobStoreSchema.TriggerGroupsSetKey()).Result)
                {
                    if (matcher.CompareWithOperator.Evaluate(RedisJobStoreSchema.TriggerGroup(triggerGroupSetKy),
                                                            matcher.CompareToValue))
                    {
                        resumedTriggerGroups.AddRange(ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals(RedisJobStoreSchema.TriggerGroup(triggerGroupSetKy))));
                    }
                }
            }


            return resumedTriggerGroups;
        }

        /// <summary>
        /// Pause the <see cref="T:Quartz.ITrigger"/> with the given key.
        /// </summary>
        public override void PauseTrigger(TriggerKey triggerKey)
        {
            var triggerHashKey = RedisJobStoreSchema.TriggerHashkey(triggerKey);

            var triggerExistsResult = Db.KeyExists(triggerHashKey);

            var completedScoreResult =
                Db.SortedSetScore(RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Completed),
                                        triggerHashKey);

            var nextFireTimeResult = Db.HashGet(triggerHashKey, RedisJobStoreSchema.NextFireTime);

            var blockedScoreResult =
                Db.SortedSetScore(RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Blocked),
                                        triggerHashKey);


            if (!triggerExistsResult)
            {
                return;
            }

            if (completedScoreResult.HasValue)
            {
                return;
            }

            var nextFireTime = double.Parse(string.IsNullOrEmpty(nextFireTimeResult) ? "-1" : nextFireTimeResult.ToString());

            if (blockedScoreResult.HasValue)
            {
                SetTriggerState(RedisTriggerState.PausedBlocked, nextFireTime, triggerHashKey);
            }
            else
            {
                SetTriggerState(RedisTriggerState.Paused, nextFireTime, triggerHashKey);
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
        public override IList<TriggerFiredResult> TriggersFired(IList<IOperableTrigger> triggers)
        {
            var result = new List<TriggerFiredResult>();

            foreach (var trigger in triggers)
            {
                var triggerHashKey = RedisJobStoreSchema.TriggerHashkey(trigger.Key);

                var triggerExistResult = Db.KeyExists(triggerHashKey);
                var triggerAcquiredResult =
                    Db.SortedSetScore(RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Acquired),
                                             triggerHashKey);

                if (triggerExistResult == false)
                {
                    Logger.WarnFormat("Trigger {0} does not exist", triggerHashKey);
                    continue;
                }

                if (!triggerAcquiredResult.HasValue)
                {
                    Logger.WarnFormat("Trigger {0} was not acquired", triggerHashKey);
                    continue;
                }

                ICalendar calendar = null;

                string calendarname = trigger.CalendarName;
                if (!string.IsNullOrEmpty(calendarname))
                {
                    calendar = this.RetrieveCalendar(calendarname);

                    if (calendar == null)
                    {
                        continue;
                    }
                }

                var previousFireTime = trigger.GetPreviousFireTimeUtc();

                trigger.Triggered(calendar);

                var job = this.RetrieveJob(trigger.JobKey);

                var triggerFireBundle = new TriggerFiredBundle(job, trigger, calendar, false, DateTimeOffset.UtcNow,
                                                               previousFireTime, previousFireTime, trigger.GetNextFireTimeUtc());

                if (job.ConcurrentExecutionDisallowed)
                {
                    var jobHasKey = this.RedisJobStoreSchema.JobHashKey(trigger.JobKey);
                    var jobTriggerSetKey = this.RedisJobStoreSchema.JobTriggersSetKey(job.Key);

                    foreach (var nonConcurrentTriggerHashKey in this.Db.SetMembers(jobTriggerSetKey))
                    {
                        var score =
                            this.Db.SortedSetScore(this.RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Waiting),
                                                    nonConcurrentTriggerHashKey);

                        if (score.HasValue)
                        {
                            this.SetTriggerState(RedisTriggerState.Blocked, score.Value, nonConcurrentTriggerHashKey);
                        }
                        else
                        {
                            score = this.Db.SortedSetScore(this.RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Paused),
                                                    nonConcurrentTriggerHashKey);
                            if (score.HasValue)
                            {
                                this.SetTriggerState(RedisTriggerState.PausedBlocked, score.Value, nonConcurrentTriggerHashKey);
                            }
                        }
                    }

                 
                    Db.SetAdd(this.RedisJobStoreSchema.JobBlockedKey(job.Key), this.SchedulerInstanceId);

                    Db.SetAdd(this.RedisJobStoreSchema.BlockedJobsSet(), jobHasKey);
                }

                //release the fired triggers
                var nextFireTimeUtc = trigger.GetNextFireTimeUtc();
                    if (nextFireTimeUtc != null)
                    {
                        var nextFireTime = nextFireTimeUtc.Value;
                        this.Db.HashSet(triggerHashKey, RedisJobStoreSchema.NextFireTime, nextFireTime.DateTime.ToUnixTimeMilliSeconds());
                        this.SetTriggerState(RedisTriggerState.Waiting, nextFireTime.DateTime.ToUnixTimeMilliSeconds(), triggerHashKey);
                    }
                
                else
                {
                    this.Db.HashSet(triggerHashKey, RedisJobStoreSchema.NextFireTime, "");
                    this.UnsetTriggerState(triggerHashKey);
                }

                result.Add(new TriggerFiredResult(triggerFireBundle));

            }

            return result;
        }

        /// <summary>
        /// Inform the <see cref="T:Quartz.Spi.IJobStore"/> that the scheduler has completed the
        ///             firing of the given <see cref="T:Quartz.ITrigger"/> (and the execution its
        ///             associated <see cref="T:Quartz.IJob"/>), and that the <see cref="T:Quartz.JobDataMap"/>
        ///             in the given <see cref="T:Quartz.IJobDetail"/> should be updated if the <see cref="T:Quartz.IJob"/>
        ///             is stateful.
        /// </summary>
        public override void TriggeredJobComplete(IOperableTrigger trigger, IJobDetail jobDetail, SchedulerInstruction triggerInstCode)
        {
            var jobHashKey = this.RedisJobStoreSchema.JobHashKey(jobDetail.Key);

            var jobDataMapHashKey = this.RedisJobStoreSchema.JobDataMapHashKey(jobDetail.Key);

            var triggerHashKey = this.RedisJobStoreSchema.TriggerHashkey(trigger.Key);

            if (this.Db.KeyExists(jobHashKey))
            {
                Logger.InfoFormat("{0} - Job has completed", jobHashKey);

                if (jobDetail.PersistJobDataAfterExecution)
                {
                    var jobDataMap = jobDetail.JobDataMap;

                    Db.KeyDelete(jobDataMapHashKey);
                    if (jobDataMap != null && !jobDataMap.IsEmpty)
                    {
                        Db.HashSet(jobDataMapHashKey, ConvertToHashEntries(jobDataMap));
                    }

                }

                if (jobDetail.ConcurrentExecutionDisallowed)
                {

                    Db.SetRemove(this.RedisJobStoreSchema.BlockedJobsSet(), jobHashKey);
                   
                    Db.KeyDelete(this.RedisJobStoreSchema.JobBlockedKey(jobDetail.Key));

                    var jobTriggersSetKey = this.RedisJobStoreSchema.JobTriggersSetKey(jobDetail.Key);

                    foreach (var nonConcurrentTriggerHashKey in this.Db.SetMembers(jobTriggersSetKey))
                    {
                        var score =
                            this.Db.SortedSetScore(this.RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Blocked),
                                                    nonConcurrentTriggerHashKey);
                        if (score.HasValue)
                        {
                            this.SetTriggerState(RedisTriggerState.Paused, score.Value, nonConcurrentTriggerHashKey);
                        }
                        else
                        {
                            score =
                                this.Db.SortedSetScoreAsync(
                                    this.RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.PausedBlocked),
                                    nonConcurrentTriggerHashKey).Result;

                            if (score.HasValue)
                            {
                                this.SetTriggerState(RedisTriggerState.Paused, score.Value, nonConcurrentTriggerHashKey);
                            }
                        }
                    }
                    this.SchedulerSignaler.SignalSchedulingChange(null);
                }

            }
            else
            {
                this.Db.SetRemove(this.RedisJobStoreSchema.BlockedJobsSet(), jobHashKey);
            }

            if (this.Db.KeyExists(triggerHashKey))
            {
                if (triggerInstCode == SchedulerInstruction.DeleteTrigger)
                {
                    if (trigger.GetNextFireTimeUtc().HasValue == false)
                    {
                        if (string.IsNullOrEmpty(this.Db.HashGet(triggerHashKey, RedisJobStoreSchema.NextFireTime)))
                        {
                            RemoveTrigger(trigger.Key);
                        }
                    }
                    else
                    {
                        this.RemoveTrigger(trigger.Key);
                        this.SchedulerSignaler.SignalSchedulingChange(null);
                    }
                }
                else if (triggerInstCode == SchedulerInstruction.SetTriggerComplete)
                {
                    this.SetTriggerState(RedisTriggerState.Completed, DateTimeOffset.UtcNow.DateTime.ToUnixTimeMilliSeconds(), triggerHashKey);
                    this.SchedulerSignaler.SignalSchedulingChange(null);
                }
                else if (triggerInstCode == SchedulerInstruction.SetTriggerError)
                {
                    double score = trigger.GetNextFireTimeUtc().HasValue
                                       ? trigger.GetNextFireTimeUtc().Value.DateTime.ToUnixTimeMilliSeconds() : 0;
                    this.SetTriggerState(RedisTriggerState.Error, score, triggerHashKey);
                    this.SchedulerSignaler.SignalSchedulingChange(null);
                }
                else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersError)
                {
                    var jobTriggersSetKey = this.RedisJobStoreSchema.JobTriggersSetKey(jobDetail.Key);

                    foreach (var errorTriggerHashKey in this.Db.SetMembersAsync(jobTriggersSetKey).Result)
                    {
                        var nextFireTime = this.Db.HashGetAsync(errorTriggerHashKey.ToString(), RedisJobStoreSchema.NextFireTime).Result;
                        var score = string.IsNullOrEmpty(nextFireTime) ? 0 : double.Parse(nextFireTime);
                        this.SetTriggerState(RedisTriggerState.Error, score, errorTriggerHashKey);
                    }
                    this.SchedulerSignaler.SignalSchedulingChange(null);
                }
                else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersComplete)
                {
                    var jobTriggerSetKey = this.RedisJobStoreSchema.JobTriggersSetKey(jobDetail.Key);

                    foreach (var completedTriggerHashKey in this.Db.SetMembersAsync(jobTriggerSetKey).Result)
                    {
                        this.SetTriggerState(RedisTriggerState.Completed, DateTimeOffset.UtcNow.DateTime.ToUnixTimeMilliSeconds(),
                                             completedTriggerHashKey);
                    }

                    this.SchedulerSignaler.SignalSchedulingChange(null);
                }
            }
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
        public override global::Quartz.Collection.ISet<JobKey> JobKeys(GroupMatcher<JobKey> matcher)
        {
            var jobKeys = new global::Quartz.Collection.HashSet<JobKey>();

            if (matcher.CompareWithOperator.Equals(StringOperator.Equality))
            {
                var jobGroupSetKey = this.RedisJobStoreSchema.JobGroupSetKey(matcher.CompareToValue);
                var jobHashKeys = this.Db.SetMembers(jobGroupSetKey);
                if (jobHashKeys != null)
                {
                    foreach (var jobHashKey in jobHashKeys)
                    {
                        jobKeys.Add(this.RedisJobStoreSchema.JobKey(jobHashKey));
                    }
                }
            }
            else
            {
                var jobGroupSets = this.Db.SetMembers(this.RedisJobStoreSchema.JobGroupsSetKey());

                var jobGroupsResult = (from groupSet in jobGroupSets where matcher.CompareWithOperator.Evaluate(this.RedisJobStoreSchema.JobGroup(groupSet), matcher.CompareToValue) select Db.SetMembers(groupSet.ToString())).ToList();

                foreach (var jobHashKey in jobGroupsResult.Where(jobHashKeys => jobHashKeys != null).SelectMany(jobHashKeys => jobHashKeys))
                {
                    jobKeys.Add(this.RedisJobStoreSchema.JobKey(jobHashKey));
                }
            }

            return jobKeys;
        }

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.ITrigger"/>s
        ///             that have the given group name.
        /// <para>
        /// If there are no triggers in the given group name, the result should be a
        ///             zero-length array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        public override global::Quartz.Collection.ISet<TriggerKey> TriggerKeys(GroupMatcher<TriggerKey> matcher)
        {
            var triggerKeys = new global::Quartz.Collection.HashSet<TriggerKey>();

            if (matcher.CompareWithOperator.Equals(StringOperator.Equality))
            {
                var triggerGroupSetKey =
                    this.RedisJobStoreSchema.TriggerGroupSetKey(matcher.CompareToValue);

                var triggers = this.Db.SetMembers(triggerGroupSetKey);


                foreach (var trigger in triggers)
                {
                    triggerKeys.Add(this.RedisJobStoreSchema.TriggerKey(trigger));
                }
            }
            else
            {
                var triggerGroupSets = this.Db.SetMembersAsync(this.RedisJobStoreSchema.TriggerGroupsSetKey()).Result;
                var triggerGroupsResult = (from groupSet in triggerGroupSets where matcher.CompareWithOperator.Evaluate(this.RedisJobStoreSchema.TriggerGroup(groupSet), matcher.CompareToValue) select Db.SetMembers(groupSet.ToString())).ToList();

                foreach (var triggerHashKeys in triggerGroupsResult)
                {
                    if (triggerHashKeys != null)
                    {
                        foreach (var triggerHashKey in triggerHashKeys)
                        {
                            triggerKeys.Add(this.RedisJobStoreSchema.TriggerKey(triggerHashKey));
                        }
                    }
                }
            }

            return triggerKeys;
        }

        /// <summary>
        /// Get the current state of the identified <see cref="T:Quartz.ITrigger"/>.
        /// </summary>
        /// <seealso cref="T:Quartz.TriggerState"/>
        public override TriggerState GetTriggerState(TriggerKey triggerKey)
        {
            var triggerHashKey = this.RedisJobStoreSchema.TriggerHashkey(triggerKey);

            if (
                this.Db.SortedSetScore(this.RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Paused),
                                        triggerHashKey) != null ||
                this.Db.SortedSetScore(this.RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.PausedBlocked),
                                        triggerHashKey) != null)
            {
                return TriggerState.Paused;
            }
            if (
                this.Db.SortedSetScore(this.RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Blocked), triggerHashKey) != null)
            {
                return TriggerState.Blocked;
            }
            if (
                this.Db.SortedSetScore(this.RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Waiting),
                                       triggerHashKey) != null || this.Db.SortedSetScore(this.RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Acquired),triggerHashKey) !=null)
            {
                return TriggerState.Normal;
            }
            if (
                this.Db.SortedSetScore(this.RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Completed),
                                       triggerHashKey) != null)
            {
                return TriggerState.Complete;
            }
            if (
                this.Db.SortedSetScore(this.RedisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Error), triggerHashKey)
                != null)
            {
                return TriggerState.Error;
            }

            return TriggerState.None;

        }

        /// <summary>
        /// Pause all of the <see cref="T:Quartz.ITrigger"/>s in the
        ///             given group.
        /// </summary>
        /// <remarks>
        /// The JobStore should "remember" that the group is paused, and impose the
        ///             pause on any new triggers that are added to the group while the group is
        ///             paused.
        /// </remarks>
        public override IList<string> PauseTriggers(GroupMatcher<TriggerKey> matcher)
        {
            var pausedTriggerGroups = new List<string>();
            if (matcher.CompareWithOperator.Equals(StringOperator.Equality))
            {
                var triggerGroupSetKey = this.RedisJobStoreSchema.TriggerGroupSetKey(matcher.CompareToValue);
                var addResult = this.Db.SetAdd(this.RedisJobStoreSchema.PausedTriggerGroupsSetKey(), triggerGroupSetKey);

                if (addResult)
                {
                    foreach (var triggerHashKey in this.Db.SetMembers(triggerGroupSetKey))
                    {
                        this.PauseTrigger(this.RedisJobStoreSchema.TriggerKey(triggerHashKey));
                    }

                    pausedTriggerGroups.Add(this.RedisJobStoreSchema.TriggerGroup(triggerGroupSetKey));
                }
            }
            else
            {
                var allTriggerGroups = this.Db.SetMembers(this.RedisJobStoreSchema.TriggerGroupsSetKey());

                var triggerGroupsResult = allTriggerGroups.Where(groupHashKey => matcher.CompareWithOperator.Evaluate(this.RedisJobStoreSchema.TriggerGroup(groupHashKey), matcher.CompareToValue)).ToDictionary<RedisValue, string, RedisValue[]>(groupHashKey => groupHashKey, groupHashKey => Db.SetMembers(groupHashKey.ToString()));

                foreach (var triggerGroup in triggerGroupsResult)
                {
                    var addResult = this.Db.SetAdd(this.RedisJobStoreSchema.PausedTriggerGroupsSetKey(), triggerGroup.Key);

                    if (addResult)
                    {
                        foreach (var triggerHashKey in triggerGroup.Value)
                        {
                            this.PauseTrigger(this.RedisJobStoreSchema.TriggerKey(triggerHashKey));
                        }
                        pausedTriggerGroups.Add(this.RedisJobStoreSchema.TriggerGroup(triggerGroup.Key));
                    }
                }

            }

            return pausedTriggerGroups;
        }

        /// <summary>
        /// update trigger state
        /// if the group the trigger is in or the group the trigger's job is in are paused, then we need to check whether its job is in the blocked group, if it's then set its state to PausedBlocked, else set it to blocked.  
        /// else set the trigger to the normal state (waiting), conver the nextfiretime into UTC milliseconds, so it could be stored as score in the sorted set. 
        /// </summary>
        /// <param name="trigger">ITrigger</param>
        private void UpdateTriggerState(ITrigger trigger)
        {
            var triggerPausedResult =
                this.Db.SetContains(this.RedisJobStoreSchema.PausedTriggerGroupsSetKey(),
                                     this.RedisJobStoreSchema.TriggerGroupSetKey(trigger.Key.Group));
            var jobPausedResult = this.Db.SetContains(this.RedisJobStoreSchema.PausedJobGroupsSetKey(),
                                                         this.RedisJobStoreSchema.JobGroupSetKey(trigger.JobKey.Group));

            if (triggerPausedResult || jobPausedResult)
            {
                double nextFireTime = trigger.GetNextFireTimeUtc().HasValue
                                                   ? trigger.GetNextFireTimeUtc().Value.DateTime.ToUnixTimeMilliSeconds() : -1;

                var jobHashKey = this.RedisJobStoreSchema.JobHashKey(trigger.JobKey);

                if (this.Db.SetContains(this.RedisJobStoreSchema.BlockedJobsSet(), jobHashKey))
                {
                    this.SetTriggerState(RedisTriggerState.PausedBlocked,
                        nextFireTime, this.RedisJobStoreSchema.TriggerHashkey(trigger.Key));
                }
                else
                {
                    this.SetTriggerState(RedisTriggerState.Paused,
                        nextFireTime, this.RedisJobStoreSchema.TriggerHashkey(trigger.Key));
                }
            }
            else if (trigger.GetNextFireTimeUtc().HasValue)
            {
                this.SetTriggerState(RedisTriggerState.Waiting,
                       trigger.GetNextFireTimeUtc().Value.DateTime.ToUnixTimeMilliSeconds(), this.RedisJobStoreSchema.TriggerHashkey(trigger.Key));
            }
        }
        #endregion
    }
}
