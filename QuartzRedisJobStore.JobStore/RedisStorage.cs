using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using Microsoft.Extensions.Logging;
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
        public RedisStorage(RedisJobStoreSchema redisJobStoreSchema, IDatabase db, ISchedulerSignaler signaler, string schedulerInstanceId, int triggerLockTimeout, int redisLockTimeout, ILogger logger) : 
            base(redisJobStoreSchema, db, signaler, schedulerInstanceId, triggerLockTimeout, redisLockTimeout, logger) 
        { 
        }

        #endregion

        #region Overrides of BaseJobStorage
        /// <summary>
        /// Store the given <see cref="T:Quartz.IJobDetail"/>.
        /// </summary>
        /// <param name="jobDetail">The <see cref="T:Quartz.IJobDetail"/> to be stored.</param><param name="replaceExisting">If <see langword="true"/>, any <see cref="T:Quartz.IJob"/> existing in the
        ///             <see cref="T:Quartz.Spi.IJobStore"/> with the same name and group should be over-written.
        ///             </param>
        /// <throws>ObjectAlreadyExistsException </throws>
        public override async Task StoreJobAsync(IJobDetail jobDetail, bool replaceExisting)
        {
            var jobHashKey = redisJobStoreSchema.JobHashKey(jobDetail.Key);
            var jobDataMapHashKey = redisJobStoreSchema.JobDataMapHashKey(jobDetail.Key);
            var jobGroupSetKey = redisJobStoreSchema.JobGroupSetKey(jobDetail.Key.Group);

            if (await db.KeyExistsAsync(jobHashKey) && !replaceExisting)
                throw new ObjectAlreadyExistsException(jobDetail);

            await db.HashSetAsync(jobHashKey, ConvertToHashEntries(jobDetail));
            await db.HashSetAsync(jobDataMapHashKey, ConvertToHashEntries(jobDetail.JobDataMap));
            await db.SetAddAsync(redisJobStoreSchema.JobsSetKey(), jobHashKey);
            await db.SetAddAsync(redisJobStoreSchema.JobGroupsSetKey(), jobGroupSetKey);
            await db.SetAddAsync(jobGroupSetKey, jobHashKey);
        }

        /// <summary>
        /// Store the given <see cref="T:Quartz.ITrigger"/>.
        /// </summary>
        /// <param name="trigger">The <see cref="T:Quartz.ITrigger"/> to be stored.</param>
        /// <param name="replaceExisting">If <see langword="true"/>, any <see cref="T:Quartz.ITrigger"/> existing in the <see cref="T:Quartz.Spi.IJobStore"/> 
        /// with the same name and group should be over-written.</param>
        /// <throws>ObjectAlreadyExistsException </throws>
        public override async Task StoreTriggerAsync(ITrigger trigger, bool replaceExisting)
        {
            var triggerHashKey = redisJobStoreSchema.TriggerHashkey(trigger.Key);
            var triggerGroupSetKey = redisJobStoreSchema.TriggerGroupSetKey(trigger.Key.Group);
            var jobTriggerSetKey = redisJobStoreSchema.JobTriggersSetKey(trigger.JobKey);
            var jobHashKey = redisJobStoreSchema.JobHashKey(trigger.JobKey);

            if (!(trigger is ISimpleTrigger) && !(trigger is ICronTrigger))
                throw new NotImplementedException("Unknown trigger, only SimpleTrigger and CronTrigger are supported");


            var triggerExists = await db.KeyExistsAsync(triggerHashKey);

            if (triggerExists && replaceExisting == false)
                throw new ObjectAlreadyExistsException(trigger);

            if (!await db.KeyExistsAsync(jobHashKey))
                throw new JobPersistenceException(string.Format("Job with key {0} does not exist", jobHashKey));

            await db.HashSetAsync(triggerHashKey, ConvertToHashEntries(trigger));
            await db.SetAddAsync(redisJobStoreSchema.TriggersSetKey(), triggerHashKey);
            await db.SetAddAsync(redisJobStoreSchema.TriggerGroupsSetKey(), triggerGroupSetKey);
            await db.SetAddAsync(triggerGroupSetKey, triggerHashKey);
            await db.SetAddAsync(jobTriggerSetKey, triggerHashKey);

            if(!string.IsNullOrEmpty(trigger.CalendarName))
            {
                var calendarTriggersSetKey = redisJobStoreSchema.CalendarTriggersSetKey(trigger.CalendarName);
                await db.SetAddAsync(calendarTriggersSetKey, triggerHashKey);
            }

            //if trigger already exists, remove it from all the possible states. 
            if (triggerExists)
                await UnsetTriggerStateAsync(triggerHashKey);

            //then update it with the new state in the its respectvie sorted set. 
            await UpdateTriggerStateAsync(trigger);
        }

        /// <summary>
        /// remove the trigger from all the possible state in the its respective sorted set. 
        /// </summary>
        /// <param name="triggerHashKey">trigger hash key</param>
        /// <returns>succeeds or not</returns>
        public override async Task<bool> UnsetTriggerStateAsync(string triggerHashKey)
        {
            var wasRemoved = false;
            var result = false;

            foreach(var state in Enum.GetValues(typeof(RedisTriggerState)).OfType<RedisTriggerState>())
                wasRemoved |= await db.SortedSetRemoveAsync(redisJobStoreSchema.TriggerStateSetKey(state), triggerHashKey);

            if (wasRemoved)
                result = await db.KeyDeleteAsync(redisJobStoreSchema.TriggerLockKey(redisJobStoreSchema.TriggerKey(triggerHashKey)));

            return result;
        }


        /// <summary>
        /// Store the given <see cref="T:Quartz.ICalendar"/>.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="calendar">The <see cref="T:Quartz.ICalendar"/> to be stored.</param>
        /// <param name="replaceExisting">If <see langword="true"/>, any <see cref="T:Quartz.ICalendar"/> existing in the <see cref="T:Quartz.Spi.IJobStore"/> 
        /// with the same name and group should be over-written.</param>
        /// <param name="updateTriggers">If <see langword="true"/>, any <see cref="T:Quartz.ITrigger"/>s existing in the <see cref="T:Quartz.Spi.IJobStore"/> 
        /// that reference an existing Calendar with the same name with have their next fire time re-computed with the new <see cref="T:Quartz.ICalendar"/>.</param>
        /// <throws>ObjectAlreadyExistsException </throws>
        public override async Task StoreCalendarAsync(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers)
        {
            var calendarHashKey = redisJobStoreSchema.CalendarHashKey(name);

            if (replaceExisting == false && await db.KeyExistsAsync(calendarHashKey))
                throw new ObjectAlreadyExistsException(string.Format("Calendar with key {0} already exists", calendarHashKey));

            await db.HashSetAsync(calendarHashKey, ConvertToHashEntries(calendar));
            await db.SetAddAsync(redisJobStoreSchema.CalendarsSetKey(), calendarHashKey);

            if (updateTriggers)
            {
                var calendarTriggersSetkey = redisJobStoreSchema.CalendarTriggersSetKey(name);

                var triggerHashKeys = db.SetMembers(calendarTriggersSetkey);

                foreach (var triggerHashKey in triggerHashKeys)
                {
                    var trigger = await RetrieveTriggerAsync(redisJobStoreSchema.TriggerKey(triggerHashKey));
                    trigger.UpdateWithNewCalendar(calendar, TimeSpan.FromSeconds(misfireThreshold));
                    await StoreTriggerAsync(trigger, true);
                }
            }
        }

        /// <summary>
        /// Remove (delete) the <see cref="T:Quartz.ICalendar"/> with the given name.
        /// </summary>
        /// <remarks>
        /// If removal of the <see cref="T:Quartz.ICalendar"/> would result in <see cref="T:Quartz.ITrigger"/>s pointing to non-existent calendars, then a
        /// <see cref="T:Quartz.JobPersistenceException"/> will be thrown.
        /// </remarks>
        /// <param name="calendarName">The name of the <see cref="T:Quartz.ICalendar"/> to be removed.</param>
        /// <returns>
        /// <see langword="true"/> if a <see cref="T:Quartz.ICalendar"/> with the given name was found and removed from the store.
        /// </returns>
        public override async Task<bool> RemoveCalendarAsync(string calendarName)
        {
            var calendarTriggersSetKey = redisJobStoreSchema.CalendarTriggersSetKey(calendarName);

            if (await db.SetLengthAsync(calendarTriggersSetKey) > 0)
                throw new JobPersistenceException(string.Format("There are triggers are using calendar {0}", calendarName));

            var calendarHashKey = redisJobStoreSchema.CalendarHashKey(calendarName);
            return await db.KeyDeleteAsync(calendarHashKey) && await db.SetRemoveAsync(redisJobStoreSchema.CalendarsSetKey(), calendarHashKey);
        }

        /// <summary>
        /// Remove (delete) the <see cref="T:Quartz.IJob"/> with the given key, and any <see cref="T:Quartz.ITrigger"/> s that reference it.
        /// </summary>
        /// <remarks>
        /// If removal of the <see cref="T:Quartz.IJob"/> results in an empty group, the group should be removed from the 
        /// <see cref="T:Quartz.Spi.IJobStore"/>'s list of known group names.
        /// </remarks>
        /// <returns>
        /// <see langword="true"/> if a <see cref="T:Quartz.IJob"/> with the given name and group was found and removed from the store.
        /// </returns>
        public override async Task<bool> RemoveJobAsync(JobKey jobKey)
        {
            var jobHashKey = redisJobStoreSchema.JobHashKey(jobKey);
            var jobDataMapHashKey = redisJobStoreSchema.JobDataMapHashKey(jobKey);
            var jobGroupSetKey = redisJobStoreSchema.JobGroupSetKey(jobKey.Group);
            var jobTriggerSetKey = redisJobStoreSchema.JobTriggersSetKey(jobKey);

            var delJobHashKeyResult = await db.KeyDeleteAsync(jobHashKey);

            await db.KeyDeleteAsync(jobDataMapHashKey);
            await db.SetRemoveAsync(redisJobStoreSchema.JobsSetKey(), jobHashKey);
            await db.SetRemoveAsync(jobGroupSetKey, jobHashKey);

            var jobTriggerSetResult = db.SetMembers(jobTriggerSetKey);

            await db.KeyDeleteAsync(jobTriggerSetKey);

            var jobGroupSetLengthResult = await db.SetLengthAsync(jobGroupSetKey);

            if (jobGroupSetLengthResult == 0)
                await db.SetRemoveAsync(redisJobStoreSchema.JobGroupsSetKey(), jobGroupSetKey);

            // remove all triggers associated with this job
            foreach (var triggerHashKey in jobTriggerSetResult)
            {
                var triggerkey = redisJobStoreSchema.TriggerKey(triggerHashKey);
                var triggerGroupKey = redisJobStoreSchema.TriggerGroupSetKey(triggerkey.Group);

                await UnsetTriggerStateAsync(triggerHashKey);

                await db.SetRemoveAsync(redisJobStoreSchema.TriggersSetKey(), triggerHashKey);
                await db.SetRemoveAsync(redisJobStoreSchema.TriggerGroupsSetKey(), triggerGroupKey);
                await db.SetRemoveAsync(redisJobStoreSchema.TriggerGroupSetKey(triggerkey.Group), triggerHashKey);
                await db.KeyDeleteAsync(triggerHashKey.ToString());
            }

            return delJobHashKeyResult;
        }


        /// <summary>
        /// Pause the <see cref="T:Quartz.IJob"/> with the given key - by
        ///             pausing all of its current <see cref="T:Quartz.ITrigger"/>s.
        /// </summary>
        public override async Task<IReadOnlyCollection<string>> PauseJobsAsync(GroupMatcher<JobKey> matcher)
        {
            var pausedJobGroups = new List<string>();

            if (matcher.CompareWithOperator.Equals(StringOperator.Equality))
            {
                var jobGroupSetKey = redisJobStoreSchema.JobGroupSetKey(matcher.CompareToValue);

                if (await db.SetAddAsync(redisJobStoreSchema.PausedJobGroupsSetKey(), jobGroupSetKey))
                {
                    pausedJobGroups.Add(redisJobStoreSchema.JobGroup(jobGroupSetKey));

                    foreach (var val in await db.SetMembersAsync(jobGroupSetKey))
                        await PauseJobAsync(redisJobStoreSchema.JobKey(val));
                }
            }
            else
            {
                var allJobGroups = await db.SetMembersAsync(redisJobStoreSchema.JobGroupsSetKey());
                var filteredJobGroups = allJobGroups.Where(jobGroupSet => matcher.CompareWithOperator.Evaluate(redisJobStoreSchema.JobGroup(jobGroupSet), matcher.CompareToValue));

                foreach (var jobGroup in filteredJobGroups)
                {
                    var jobs = await db.SetMembersAsync(jobGroup.ToString());

                    if (await db.SetAddAsync(redisJobStoreSchema.PausedJobGroupsSetKey(), jobGroup))
                    {
                        pausedJobGroups.Add(redisJobStoreSchema.JobGroup(jobGroup));
                        if (jobs != null)
                        {
                            foreach (var jobHashKey in jobs)
                                await PauseJobAsync(redisJobStoreSchema.JobKey(jobHashKey));
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
        public override async Task<IReadOnlyCollection<string>> ResumeJobsAsync(GroupMatcher<JobKey> matcher)
        {
            var resumedJobGroups = new List<string>();

            if (matcher.CompareWithOperator.Equals(StringOperator.Equality))
            {
                var jobGroupSetKey = redisJobStoreSchema.JobGroupSetKey(matcher.CompareToValue);

                var removedPausedResult = await db.SetRemoveAsync(redisJobStoreSchema.PausedJobGroupsSetKey(), jobGroupSetKey);
                var jobsResult = await db.SetMembersAsync(jobGroupSetKey);


                if (removedPausedResult)
                    resumedJobGroups.Add(redisJobStoreSchema.JobGroup(jobGroupSetKey));

                foreach (var job in jobsResult)
                    await ResumeJobAsync(redisJobStoreSchema.JobKey(job));
            }
            else
            {
                foreach (var jobGroupSetKey in await db.SetMembersAsync(redisJobStoreSchema.JobGroupsSetKey()))
                {
                    if (matcher.CompareWithOperator.Evaluate(redisJobStoreSchema.JobGroup(jobGroupSetKey), matcher.CompareToValue))
                        resumedJobGroups.AddRange(await ResumeJobsAsync(GroupMatcher<JobKey>.GroupEquals(redisJobStoreSchema.JobGroup(jobGroupSetKey))));
                }
            }

            return new HashSet<string>(resumedJobGroups);
        }

        /// <summary>
        /// Resume (un-pause) the <see cref="T:Quartz.ITrigger"/> with the given key.
        /// <para>
        /// If the <see cref="T:Quartz.ITrigger"/> missed one or more fire-times, then the
        ///             <see cref="T:Quartz.ITrigger"/>'s misfire instruction will be applied.
        /// </para>
        /// </summary>
        /// <seealso cref="T:System.String"/>
        public override async Task ResumeTriggerAsync(TriggerKey triggerKey)
        {
            var triggerHashKey = redisJobStoreSchema.TriggerHashkey(triggerKey);
            var triggerExists = await db.SetContainsAsync(redisJobStoreSchema.TriggersSetKey(), triggerHashKey);
            var isPausedTrigger = await db.SortedSetScoreAsync(redisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Paused), triggerHashKey);
            var isPausedBlockedTrigger = await db.SortedSetScoreAsync(redisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.PausedBlocked), triggerHashKey);

            if (!triggerExists)
                return;

            //Trigger is not paused, cant be resumed then.
            if (!isPausedTrigger.HasValue && !isPausedBlockedTrigger.HasValue)
                return;

            var trigger = await RetrieveTriggerAsync(triggerKey);

            var jobHashKey = redisJobStoreSchema.JobHashKey(trigger.JobKey);

            var nextFireTime = trigger.GetNextFireTimeUtc();

            if (nextFireTime.HasValue)
            {
                var newTriggerState = await db.SetContainsAsync(redisJobStoreSchema.BlockedJobsSet(), jobHashKey)
                    ? RedisTriggerState.Blocked
                    : RedisTriggerState.Waiting;

                await SetTriggerStateAsync(newTriggerState, nextFireTime.Value.DateTime.ToUnixTimeMilliSeconds(), triggerHashKey);
            }
            
            await ApplyMisfireAsync(trigger);
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
        /// <see langword="true"/> if a <see cref="T:Quartz.ITrigger"/> with the given name and group was found and removed from the store.
        /// </returns>
        public override async Task<bool> RemoveTriggerAsync(TriggerKey triggerKey, bool removeNonDurableJob = true)
        {
            var triggerHashKey = redisJobStoreSchema.TriggerHashkey(triggerKey);

            if (!await db.KeyExistsAsync(triggerHashKey))
                return false;

            var trigger = await RetrieveTriggerAsync(triggerKey);

            var triggerGroupSetKey = redisJobStoreSchema.TriggerGroupSetKey(triggerKey.Group);
            var jobHashKey = redisJobStoreSchema.JobHashKey(trigger.JobKey);
            var jobTriggerSetkey = redisJobStoreSchema.JobTriggersSetKey(trigger.JobKey);

            await db.SetRemoveAsync(redisJobStoreSchema.TriggersSetKey(), triggerHashKey);
            await db.SetRemoveAsync(triggerGroupSetKey, triggerHashKey);
            await db.SetRemoveAsync(jobTriggerSetkey, triggerHashKey);

            if (await db.SetLengthAsync(triggerGroupSetKey) == 0)
                await db.SetRemoveAsync(redisJobStoreSchema.TriggerGroupsSetKey(), triggerGroupSetKey);

            if (removeNonDurableJob)
            {
                var jobTriggerSetKeyLengthResult = await db.SetLengthAsync(jobTriggerSetkey);

                var jobExistsResult = await db.KeyExistsAsync(jobHashKey);

                if (jobTriggerSetKeyLengthResult == 0 && jobExistsResult)
                {
                    var job = await RetrieveJobAsync(trigger.JobKey);

                    if (!job.Durable)
                    {
                        await RemoveJobAsync(job.Key);
                        await signaler.NotifySchedulerListenersJobDeleted(job.Key);
                    }
                }
            }

            if (!string.IsNullOrEmpty(trigger.CalendarName))
                await db.SetRemoveAsync(redisJobStoreSchema.CalendarTriggersSetKey(trigger.CalendarName), triggerHashKey);

            await UnsetTriggerStateAsync(triggerHashKey);
            return await db.KeyDeleteAsync(triggerHashKey);
        }

        /// <summary>
        /// Resume (un-pause) all of the <see cref="T:Quartz.ITrigger"/>s
        ///             in the given group.
        /// <para>
        /// If any <see cref="T:Quartz.ITrigger"/> missed one or more fire-times, then the
        ///             <see cref="T:Quartz.ITrigger"/>'s misfire instruction will be applied.
        /// </para>
        /// </summary>
        public override async Task<IReadOnlyCollection<string>> ResumeTriggersAsync(GroupMatcher<TriggerKey> matcher)
        {
            var resumedTriggerGroups = new List<string>();

            if (matcher.CompareWithOperator.Equals(StringOperator.Equality))
            {
                var triggerGroupSetKey = redisJobStoreSchema.TriggerGroupSetKey(matcher.CompareToValue);

                await db.SetRemoveAsync(redisJobStoreSchema.PausedTriggerGroupsSetKey(), triggerGroupSetKey);

                var triggerHashKeysResult = await db.SetMembersAsync(triggerGroupSetKey);

                foreach (var triggerHashKey in triggerHashKeysResult)
                {
                    var trigger = await RetrieveTriggerAsync(redisJobStoreSchema.TriggerKey(triggerHashKey));

                    await ResumeTriggerAsync(trigger.Key);

                    if(!resumedTriggerGroups.Contains(trigger.Key.Group)) 
                        resumedTriggerGroups.Add(trigger.Key.Group);
                }
            }
            else
            {
                foreach (var triggerGroupSetKy in await db.SetMembersAsync(redisJobStoreSchema.TriggerGroupsSetKey()))
                {
                    if (matcher.CompareWithOperator.Evaluate(redisJobStoreSchema.TriggerGroup(triggerGroupSetKy), matcher.CompareToValue))
                        resumedTriggerGroups.AddRange(await ResumeTriggersAsync(GroupMatcher<TriggerKey>.GroupEquals(redisJobStoreSchema.TriggerGroup(triggerGroupSetKy))));
                }
            }

            return resumedTriggerGroups;
        }

        /// <summary>
        /// Pause the <see cref="T:Quartz.ITrigger"/> with the given key.
        /// </summary>
        public override async Task PauseTriggerAsync(TriggerKey triggerKey)
        {
            var triggerHashKey = redisJobStoreSchema.TriggerHashkey(triggerKey);

            var triggerExistsResult = await db.KeyExistsAsync(triggerHashKey);
            var completedScoreResult = await db.SortedSetScoreAsync(redisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Completed), triggerHashKey);
            var nextFireTimeResult = await db.HashGetAsync(triggerHashKey, RedisJobStoreSchema.NextFireTime);
            var blockedScoreResult = await db.SortedSetScoreAsync(redisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Blocked), triggerHashKey);


            if (!triggerExistsResult || completedScoreResult.HasValue)
                return;

            var nextFireTime = double.Parse(string.IsNullOrEmpty(nextFireTimeResult) ? "-1" : nextFireTimeResult.ToString());

            var newTriggerState = blockedScoreResult.HasValue
                ? RedisTriggerState.PausedBlocked
                : RedisTriggerState.Paused;

            await SetTriggerStateAsync(newTriggerState, nextFireTime, triggerHashKey);
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
        public override async Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFiredAsync(IReadOnlyCollection<IOperableTrigger> triggers)
        {
            var result = new List<TriggerFiredResult>();

            foreach (var trigger in triggers)
            {
                var triggerHashKey = redisJobStoreSchema.TriggerHashkey(trigger.Key);

                var triggerExistResult = await db.KeyExistsAsync(triggerHashKey);
                var triggerAcquiredResult = await db.SortedSetScoreAsync(redisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Acquired), triggerHashKey);

                if (triggerExistResult == false)
                {
                    Logger.LogWarning("Trigger {0} does not exist", triggerHashKey);
                    continue;
                }

                if (!triggerAcquiredResult.HasValue)
                {
                    Logger.LogWarning("Trigger {0} was not acquired", triggerHashKey);
                    continue;
                }

                ICalendar calendar = null;

                var calendarname = trigger.CalendarName;
                if (!string.IsNullOrEmpty(calendarname))
                {
                    calendar = await RetrieveCalendarAsync(calendarname);

                    if (calendar == null)
                        continue;
                }

                var previousFireTime = trigger.GetPreviousFireTimeUtc();

                trigger.Triggered(calendar);

                var job = await RetrieveJobAsync(trigger.JobKey);

                if (job == null)
                {
                    Logger.LogWarning("Could not find implementation for job {0}", trigger.JobKey);
                    continue;
                }

                var triggerFireBundle = new TriggerFiredBundle(job, trigger, calendar, false, DateTimeOffset.UtcNow, previousFireTime, previousFireTime, trigger.GetNextFireTimeUtc());

                if (job.ConcurrentExecutionDisallowed)
                {
                    var jobHasKey = redisJobStoreSchema.JobHashKey(trigger.JobKey);
                    var jobTriggerSetKey = redisJobStoreSchema.JobTriggersSetKey(job.Key);

                    foreach (var nonConcurrentTriggerHashKey in await db.SetMembersAsync(jobTriggerSetKey))
                    {
                        var score = await db.SortedSetScoreAsync(redisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Waiting), nonConcurrentTriggerHashKey);

                        if (score.HasValue)
                            await SetTriggerStateAsync(RedisTriggerState.Blocked, score.Value, nonConcurrentTriggerHashKey);
                        else
                        {
                            score = await db.SortedSetScoreAsync(redisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Paused), nonConcurrentTriggerHashKey);
                            if (score.HasValue)
                                await SetTriggerStateAsync(RedisTriggerState.PausedBlocked, score.Value, nonConcurrentTriggerHashKey);
                        }
                    }

                    await db.SetAddAsync(redisJobStoreSchema.JobBlockedKey(job.Key), schedulerInstanceId);
                    await db.SetAddAsync(redisJobStoreSchema.BlockedJobsSet(), jobHasKey);
                }

                //release the fired triggers
                var nextFireTimeUtc = trigger.GetNextFireTimeUtc();
                if (nextFireTimeUtc != null)
                {
                    var nextFireTime = nextFireTimeUtc.Value;
                    await db.HashSetAsync(triggerHashKey, RedisJobStoreSchema.NextFireTime, nextFireTime.DateTime.ToUnixTimeMilliSeconds());
                    await SetTriggerStateAsync(RedisTriggerState.Waiting, nextFireTime.DateTime.ToUnixTimeMilliSeconds(), triggerHashKey);
                }
                else
                {
                    await db.HashSetAsync(triggerHashKey, RedisJobStoreSchema.NextFireTime, "");
                    await UnsetTriggerStateAsync(triggerHashKey);
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
        public override async Task TriggeredJobCompleteAsync(IOperableTrigger trigger, IJobDetail jobDetail, SchedulerInstruction triggerInstCode)
        {
            var jobHashKey = redisJobStoreSchema.JobHashKey(jobDetail.Key);
            var jobDataMapHashKey = redisJobStoreSchema.JobDataMapHashKey(jobDetail.Key);
            var triggerHashKey = redisJobStoreSchema.TriggerHashkey(trigger.Key);

            if (await db.KeyExistsAsync(jobHashKey))
            {
                Logger.LogInformation("{0} - Job has completed", jobHashKey);

                if (jobDetail.PersistJobDataAfterExecution)
                {
                    var jobDataMap = jobDetail.JobDataMap;

                    await db.KeyDeleteAsync(jobDataMapHashKey);
                    if (jobDataMap != null && !jobDataMap.IsEmpty)
                        await db.HashSetAsync(jobDataMapHashKey, ConvertToHashEntries(jobDataMap));
                }

                if (jobDetail.ConcurrentExecutionDisallowed)
                {
                    await db.SetRemoveAsync(redisJobStoreSchema.BlockedJobsSet(), jobHashKey);
                    await db.KeyDeleteAsync(redisJobStoreSchema.JobBlockedKey(jobDetail.Key));

                    var jobTriggersSetKey = redisJobStoreSchema.JobTriggersSetKey(jobDetail.Key);

                    foreach (var nonConcurrentTriggerHashKey in await db.SetMembersAsync(jobTriggersSetKey))
                    {
                        var score = await db.SortedSetScoreAsync(redisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Blocked), nonConcurrentTriggerHashKey);
                        if (score.HasValue)
                            await SetTriggerStateAsync(RedisTriggerState.Paused, score.Value, nonConcurrentTriggerHashKey);
                        else
                        {
                            score = await db.SortedSetScoreAsync(redisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.PausedBlocked), nonConcurrentTriggerHashKey);

                            if (score.HasValue)
                                await SetTriggerStateAsync(RedisTriggerState.Paused, score.Value, nonConcurrentTriggerHashKey);
                        }
                    }
                    signaler.SignalSchedulingChange(null);
                }
            }
            else
                await db.SetRemoveAsync(redisJobStoreSchema.BlockedJobsSet(), jobHashKey);

            if (await db.KeyExistsAsync(triggerHashKey))
            {
                if (triggerInstCode == SchedulerInstruction.DeleteTrigger)
                {
                    if (trigger.GetNextFireTimeUtc().HasValue == false)
                    {
                        if (string.IsNullOrEmpty(await db.HashGetAsync(triggerHashKey, RedisJobStoreSchema.NextFireTime)))
                            await RemoveTriggerAsync(trigger.Key);
                    }
                    else
                    {
                        await RemoveTriggerAsync(trigger.Key);
                        signaler.SignalSchedulingChange(null);
                    }
                }
                else if (triggerInstCode == SchedulerInstruction.SetTriggerComplete)
                {
                    await SetTriggerStateAsync(RedisTriggerState.Completed, DateTimeOffset.UtcNow.DateTime.ToUnixTimeMilliSeconds(), triggerHashKey);
                    signaler.SignalSchedulingChange(null);
                }
                else if (triggerInstCode == SchedulerInstruction.SetTriggerError)
                {
                    var score = trigger.GetNextFireTimeUtc()?.DateTime.ToUnixTimeMilliSeconds() ?? 0;
                    await SetTriggerStateAsync(RedisTriggerState.Error, score, triggerHashKey);
                    signaler.SignalSchedulingChange(null);
                }
                else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersError)
                {
                    var jobTriggersSetKey = redisJobStoreSchema.JobTriggersSetKey(jobDetail.Key);

                    foreach (var errorTriggerHashKey in await db.SetMembersAsync(jobTriggersSetKey))
                    {
                        var nextFireTime = await db.HashGetAsync(errorTriggerHashKey.ToString(), RedisJobStoreSchema.NextFireTime);
                        var score = string.IsNullOrEmpty(nextFireTime) ? 0 : double.Parse(nextFireTime);
                        await SetTriggerStateAsync(RedisTriggerState.Error, score, errorTriggerHashKey);
                    }
                    signaler.SignalSchedulingChange(null);
                }
                else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersComplete)
                {
                    var jobTriggerSetKey = redisJobStoreSchema.JobTriggersSetKey(jobDetail.Key);

                    foreach (var completedTriggerHashKey in await db.SetMembersAsync(jobTriggerSetKey))
                        await SetTriggerStateAsync(RedisTriggerState.Completed, DateTimeOffset.UtcNow.DateTime.ToUnixTimeMilliSeconds(), completedTriggerHashKey);

                    signaler.SignalSchedulingChange(null);
                }
            }
        }

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.IJob"/> s that have the given group name.
        /// <para>
        /// If there are no jobs in the given group name, the result should be a zero-length array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        /// <param name="matcher"/>
        /// <returns/>
        public override async Task<IReadOnlyCollection<JobKey>> JobKeysAsync(GroupMatcher<JobKey> matcher)
        {
            var jobKeys = new HashSet<JobKey>();

            if (matcher.CompareWithOperator.Equals(StringOperator.Equality))
            {
                var jobGroupSetKey = redisJobStoreSchema.JobGroupSetKey(matcher.CompareToValue);
                var jobHashKeys = await db.SetMembersAsync(jobGroupSetKey);

                if (jobHashKeys != null)
                {
                    foreach (var jobHashKey in jobHashKeys)
                        jobKeys.Add(redisJobStoreSchema.JobKey(jobHashKey));
                }
            }
            else
            {
                var allJobGroups = await db.SetMembersAsync(redisJobStoreSchema.JobGroupsSetKey());
                var filteredJobGroups = allJobGroups.Where(jobGroupSet => matcher.CompareWithOperator.Evaluate(redisJobStoreSchema.JobGroup(jobGroupSet), matcher.CompareToValue));

                foreach (var jobGroup in filteredJobGroups)
                {
                    var jobs = await db.SetMembersAsync(jobGroup.ToString());
                    if (jobs != null)
                    {
                        foreach (var jobHashKey in jobs)
                            jobKeys.Add(redisJobStoreSchema.JobKey(jobHashKey));
                    }
                }
            }

            return jobKeys;
        }

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.ITrigger"/>s that have the given group name.
        /// <para>
        /// If there are no triggers in the given group name, the result should be a zero-length array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        public override async Task<IReadOnlyCollection<TriggerKey>> TriggerKeysAsync(GroupMatcher<TriggerKey> matcher)
        {
            var triggerKeys = new HashSet<TriggerKey>();

            if (matcher.CompareWithOperator.Equals(StringOperator.Equality))
            {
                var triggerGroupSetKey = redisJobStoreSchema.TriggerGroupSetKey(matcher.CompareToValue);

                var triggers = await db.SetMembersAsync(triggerGroupSetKey);

                foreach (var trigger in triggers)
                    triggerKeys.Add(redisJobStoreSchema.TriggerKey(trigger));
            }
            else
            {
                var allTriggerGroups = await db.SetMembersAsync(redisJobStoreSchema.TriggerGroupsSetKey());
                var filteredGroups = allTriggerGroups.Where(groupSet => matcher.CompareWithOperator.Evaluate(redisJobStoreSchema.JobGroup(groupSet), matcher.CompareToValue));

                foreach (var triggerGroup in filteredGroups)
                {
                    var triggers = await db.SetMembersAsync(triggerGroup.ToString());
                    if (triggers != null)
                    {
                        foreach (var triggerHashKey in triggers)
                            triggerKeys.Add(redisJobStoreSchema.TriggerKey(triggerHashKey));
                    }
                }
            }

            return triggerKeys;
        }

        /// <summary>
        /// Get the current state of the identified <see cref="T:Quartz.ITrigger"/>.
        /// </summary>
        /// <seealso cref="T:Quartz.TriggerState"/>
        public override async Task<TriggerState> GetTriggerStateAsync(TriggerKey triggerKey)
        {
            var triggerHashKey = redisJobStoreSchema.TriggerHashkey(triggerKey);

            if (await db.SortedSetScoreAsync(redisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Paused), triggerHashKey) != null ||
                await db.SortedSetScoreAsync(redisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.PausedBlocked), triggerHashKey) != null)
                return TriggerState.Paused;

            if (await db.SortedSetScoreAsync(redisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Blocked), triggerHashKey) != null)
                return TriggerState.Blocked;

            if (await db.SortedSetScoreAsync(redisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Waiting), triggerHashKey) != null ||
                await db.SortedSetScoreAsync(redisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Acquired),triggerHashKey) !=null)
                return TriggerState.Normal;

            if (await db.SortedSetScoreAsync(redisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Completed), triggerHashKey) != null)
                return TriggerState.Complete;

            if (await db.SortedSetScoreAsync(redisJobStoreSchema.TriggerStateSetKey(RedisTriggerState.Error), triggerHashKey) != null)
                return TriggerState.Error;

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
        public override async Task<IReadOnlyCollection<string>> PauseTriggersAsync(GroupMatcher<TriggerKey> matcher)
        {
            var pausedTriggerGroups = new List<string>();
            if (matcher.CompareWithOperator.Equals(StringOperator.Equality))
            {
                var triggerGroupSetKey = redisJobStoreSchema.TriggerGroupSetKey(matcher.CompareToValue);
                var addResult = await db.SetAddAsync(redisJobStoreSchema.PausedTriggerGroupsSetKey(), triggerGroupSetKey);

                if (addResult)
                {
                    foreach (var triggerHashKey in await db.SetMembersAsync(triggerGroupSetKey))
                        await PauseTriggerAsync(redisJobStoreSchema.TriggerKey(triggerHashKey));

                    pausedTriggerGroups.Add(redisJobStoreSchema.TriggerGroup(triggerGroupSetKey));
                }
            }
            else
            {
                var allTriggerGroups = await db.SetMembersAsync(redisJobStoreSchema.TriggerGroupsSetKey());
                var filteredTriggerGroups = allTriggerGroups.Where(groupHashKey => matcher.CompareWithOperator.Evaluate(redisJobStoreSchema.TriggerGroup(groupHashKey), matcher.CompareToValue));

                foreach (var triggerGroup in filteredTriggerGroups)
                {
                    var triggers = await db.SetMembersAsync(triggerGroup.ToString());

                    var addResult = await db.SetAddAsync(redisJobStoreSchema.PausedTriggerGroupsSetKey(), triggerGroup);

                    if (addResult)
                    {
                        if (triggers != null)
                        {
                            foreach (var triggerHashKey in triggers)
                                await PauseTriggerAsync(redisJobStoreSchema.TriggerKey(triggerHashKey));
                        }
                        pausedTriggerGroups.Add(redisJobStoreSchema.TriggerGroup(triggerGroup));
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
        async Task UpdateTriggerStateAsync(ITrigger trigger)
        {
            var triggerPausedResult = await db.SetContainsAsync(redisJobStoreSchema.PausedTriggerGroupsSetKey(), redisJobStoreSchema.TriggerGroupSetKey(trigger.Key.Group));
            var jobPausedResult = await db.SetContainsAsync(redisJobStoreSchema.PausedJobGroupsSetKey(), redisJobStoreSchema.JobGroupSetKey(trigger.JobKey.Group));

            if (triggerPausedResult || jobPausedResult)
            {
                var nextFireTime = trigger.GetNextFireTimeUtc()?.DateTime.ToUnixTimeMilliSeconds() ?? -1;

                var jobHashKey = redisJobStoreSchema.JobHashKey(trigger.JobKey);

                var newTriggerState = await db.SetContainsAsync(redisJobStoreSchema.BlockedJobsSet(), jobHashKey)
                    ? RedisTriggerState.PausedBlocked
                    : RedisTriggerState.Paused;

                await SetTriggerStateAsync(newTriggerState, nextFireTime, redisJobStoreSchema.TriggerHashkey(trigger.Key));
            }
            else if (trigger.GetNextFireTimeUtc().HasValue)
                await SetTriggerStateAsync(RedisTriggerState.Waiting, trigger.GetNextFireTimeUtc().Value.DateTime.ToUnixTimeMilliSeconds(), redisJobStoreSchema.TriggerHashkey(trigger.Key));
        }
        #endregion
    }
}
