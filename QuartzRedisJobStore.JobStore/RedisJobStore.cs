using System;
using System.Collections.Generic;
using Quartz;
using Quartz.Impl.Matchers;
using Quartz.Spi;
using StackExchange.Redis;
using log4net;

namespace QuartzRedisJobStore.JobStore
{
    /// <summary>
    /// Redis Job Store 
    /// </summary>
    public class RedisJobStore : IJobStore
    {

        #region private fields
        /// <summary>
        /// logger
        /// </summary>
        private readonly ILog _logger = LogManager.GetLogger(typeof(RedisJobStore));
        /// <summary>
        /// redis job store schema
        /// </summary>
        private RedisJobStoreSchema _storeSchema;
        /// <summary>
        /// redis db.
        /// </summary>
        private IDatabase _db;
        /// <summary>
        /// master/slave redis store.
        /// </summary>
        private RedisStorage _storage;

        #endregion

        #region public properties

        /// <summary>
        /// Indicates whether job store supports persistence.
        /// </summary>
        /// <returns/>
        public bool SupportsPersistence
        {
            get { return true; }
        }
        /// <summary>
        /// How long (in milliseconds) the <see cref="T:Quartz.Spi.IJobStore"/> implementation 
        ///             estimates that it will take to release a trigger and acquire a new one. 
        /// </summary>
        public long EstimatedTimeToReleaseAndAcquireTrigger
        {
            get { return 200; }
        }
        /// <summary>
        /// Whether or not the <see cref="T:Quartz.Spi.IJobStore"/> implementation is clustered.
        /// </summary>
        /// <returns/>
        public bool Clustered
        {
            get { return true; }
        }
        /// <summary>
        /// Inform the <see cref="T:Quartz.Spi.IJobStore"/> of the Scheduler instance's Id, 
        ///             prior to initialize being invoked.
        /// </summary>
        public string InstanceId { get; set; }
        /// <summary>
        /// Inform the <see cref="T:Quartz.Spi.IJobStore"/> of the Scheduler instance's name, 
        ///             prior to initialize being invoked.
        /// </summary>
        public string InstanceName { get; set; }
        /// <summary>
        /// Tells the JobStore the pool size used to execute jobs.
        /// </summary>
        public int ThreadPoolSize { get; set; }
        /// <summary>
        /// Redis configuration
        /// </summary>
        public string RedisConfiguration { set; get; }

        /// <summary>
        /// gets / sets the delimiter for concatinate redis keys.
        /// </summary>
        public string KeyDelimiter { get; set; }

        /// <summary>
        /// gets /sets the prefix for redis keys.
        /// </summary>
        public string KeyPrefix { get; set; }

        /// <summary>
        /// trigger lock time out, used to release the orphan triggers in case when a scheduler crashes and still has locks on some triggers. 
        /// make sure the lock time out is bigger than the time for running the longest job.
        /// </summary>
        public int? TriggerLockTimeout { get; set; }

        /// <summary>
        /// redis lock time out in milliseconds.
        /// </summary>
        public int? RedisLockTimeout { get; set; }
        #endregion

        #region Implementation of IJobStore
        /// <summary>
        /// Called by the QuartzScheduler before the <see cref="T:Quartz.Spi.IJobStore"/> is
        ///             used, in order to give the it a chance to Initialize.
        /// here we default triggerLockTime out to 5 mins (number in miliseconds)
        /// default redisLockTimeout to 5 secs (number in miliseconds)
        /// </summary>
        public void Initialize(ITypeLoadHelper loadHelper, ISchedulerSignaler signaler)
        {
            _storeSchema = new RedisJobStoreSchema(KeyPrefix ?? string.Empty, KeyDelimiter ?? ":");
            _db = ConnectionMultiplexer.Connect(RedisConfiguration).GetDatabase();
            _storage = new RedisStorage(_storeSchema, _db, signaler, InstanceId, TriggerLockTimeout ?? 300000, RedisLockTimeout ?? 5000);
        }

        /// <summary>
        /// Called by the QuartzScheduler to inform the <see cref="T:Quartz.Spi.IJobStore"/> that
        ///             the scheduler has started.
        /// </summary>
        public void SchedulerStarted()
        {
            _logger.Info("scheduler has started");
        }

        /// <summary>
        /// Called by the QuartzScheduler to inform the JobStore that
        ///             the scheduler has been paused.
        /// </summary>
        public void SchedulerPaused()
        {
            _logger.Info("scheduler has paused");
        }

        /// <summary>
        /// Called by the QuartzScheduler to inform the JobStore that
        ///             the scheduler has resumed after being paused.
        /// </summary>
        public void SchedulerResumed()
        {
            _logger.Info("scheduler has resumed");
        }

        /// <summary>
        /// Called by the QuartzScheduler to inform the <see cref="T:Quartz.Spi.IJobStore"/> that
        ///             it should free up all of it's resources because the scheduler is
        ///             shutting down.
        /// </summary>
        public void Shutdown()
        {
            _logger.Info("scheduler has shutdown");
            _db.Multiplexer.Dispose();
        }

        /// <summary>
        /// Store the given <see cref="T:Quartz.IJobDetail"/> and <see cref="T:Quartz.ITrigger"/>.
        /// </summary>
        /// <param name="newJob">The <see cref="T:Quartz.IJobDetail"/> to be stored.</param><param name="newTrigger">The <see cref="T:Quartz.ITrigger"/> to be stored.</param><throws>ObjectAlreadyExistsException </throws>
        public void StoreJobAndTrigger(IJobDetail newJob, IOperableTrigger newTrigger)
        {
            _logger.Info("StoreJobAndTrigger");
            DoWithLock(() =>
            {
                _storage.StoreJob(newJob, false);
                _storage.StoreTrigger(newTrigger, false);
            }, "Could store job/trigger");

        }

        /// <summary>
        /// returns true if the given JobGroup is paused
        /// </summary>
        /// <param name="groupName"/>
        /// <returns/>
        public bool IsJobGroupPaused(string groupName)
        {
            _logger.Info("IsJobGroupPaused");
            return DoWithLock(() => _storage.IsJobGroupPaused(groupName),
                              string.Format("Error on IsJobGroupPaused - Group {0}", groupName));
        }

        /// <summary>
        /// returns true if the given TriggerGroup
        ///             is paused
        /// </summary>
        /// <param name="groupName"/>
        /// <returns/>
        public bool IsTriggerGroupPaused(string groupName)
        {
            _logger.Info("IsTriggerGroupPaused");
            return DoWithLock(() => _storage.IsTriggerGroupPaused(groupName),
                              string.Format("Error on IsTriggerGroupPaused - Group {0}", groupName));
        }

        /// <summary>
        /// Store the given <see cref="T:Quartz.IJobDetail"/>.
        /// </summary>
        /// <param name="newJob">The <see cref="T:Quartz.IJobDetail"/> to be stored.</param><param name="replaceExisting">If <see langword="true"/>, any <see cref="T:Quartz.IJob"/> existing in the
        ///             <see cref="T:Quartz.Spi.IJobStore"/> with the same name and group should be
        ///             over-written.
        ///             </param>
        public void StoreJob(IJobDetail newJob, bool replaceExisting)
        {
            _logger.Info("StoreJob");
            DoWithLock(() => _storage.StoreJob(newJob, replaceExisting), "Could not store job");
        }

        /// <summary>
        /// Store jobs and triggers
        /// </summary>
        /// <param name="triggersAndJobs">jobs and triggers indexed by job</param>
        /// <param name="replace">indicate to repalce the existing ones or not</param>
        public void StoreJobsAndTriggers(IDictionary<IJobDetail, global::Quartz.Collection.ISet<ITrigger>> triggersAndJobs, bool replace)
        {
            _logger.Info("StoreJobsAndTriggers");
            foreach (var job in triggersAndJobs)
            {
                DoWithLock(() =>
                {
                    _storage.StoreJob(job.Key, replace);
                    foreach (var trigger in job.Value)
                    {
                        _storage.StoreTrigger(trigger, replace);
                    }

                }, "Could store job/trigger");

            }
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
        public bool RemoveJob(JobKey jobKey)
        {
            _logger.Info("RemoveJob");
            return DoWithLock(() => _storage.RemoveJob(jobKey),
                              "Could not remove a job");
        }

        /// <summary>
        /// Remove jobs 
        /// </summary>
        /// <param name="jobKeys">JobKeys</param>
        /// <returns>succeeds or not</returns>
        public bool RemoveJobs(IList<JobKey> jobKeys)
        {
            _logger.Info("RemoveJobs");
            bool removed = jobKeys.Count > 0;

            foreach (var jobKey in jobKeys)
            {
                DoWithLock(() =>
                {
                    removed = _storage.RemoveJob(jobKey);
                }, "Error on removing job");

            }
            return removed;
        }

        /// <summary>
        /// Retrieve the <see cref="T:Quartz.IJobDetail"/> for the given
        ///             <see cref="T:Quartz.IJob"/>.
        /// </summary>
        /// <returns>
        /// The desired <see cref="T:Quartz.IJob"/>, or null if there is no match.
        /// </returns>
        public IJobDetail RetrieveJob(JobKey jobKey)
        {
            _logger.Info("RetrieveJob");
            return DoWithLock(() => _storage.RetrieveJob(jobKey),
                              "Could not retriev job");
        }

        /// <summary>
        /// Store the given <see cref="T:Quartz.ITrigger"/>.
        /// </summary>
        /// <param name="newTrigger">The <see cref="T:Quartz.ITrigger"/> to be stored.</param><param name="replaceExisting">If <see langword="true"/>, any <see cref="T:Quartz.ITrigger"/> existing in
        ///             the <see cref="T:Quartz.Spi.IJobStore"/> with the same name and group should
        ///             be over-written.</param><throws>ObjectAlreadyExistsException </throws>
        public void StoreTrigger(IOperableTrigger newTrigger, bool replaceExisting)
        {
            _logger.Info("StoreTrigger");
            DoWithLock(() => _storage.StoreTrigger(newTrigger, replaceExisting),
                            "Could not store trigger");
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
        public bool RemoveTrigger(TriggerKey triggerKey)
        {
            _logger.Info("RemoveTrigger");
            return DoWithLock(() => _storage.RemoveTrigger(triggerKey),
                              "Could not remove trigger");
        }

        /// <summary>
        /// remove the requeste triggers by triggerKey
        /// </summary>
        /// <param name="triggerKeys">Trigger Keys</param>
        /// <returns>succeeds or not</returns>
        public bool RemoveTriggers(IList<TriggerKey> triggerKeys)
        {
            _logger.Info("RemoveTriggers");

            bool removed = triggerKeys.Count > 0;

            foreach (var triggerKey in triggerKeys)
            {
                DoWithLock(() =>
                {
                    removed = _storage.RemoveTrigger(triggerKey);
                }, "Error on removing trigger");

            }
            return removed;
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
            _logger.Info("ReplaceTrigger");

            return DoWithLock(() => _storage.ReplaceTrigger(triggerKey, newTrigger),
                              "Error on replacing trigger");
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
            _logger.Info("RetrieveTrigger");

            return DoWithLock(() => _storage.RetrieveTrigger(triggerKey),
                              "could not retrieve trigger");
        }

        /// <summary>
        /// Determine whether a <see cref="T:Quartz.ICalendar"/> with the given identifier already
        ///             exists within the scheduler.
        /// </summary>
        /// <remarks/>
        /// <param name="calName">the identifier to check for</param>
        /// <returns>
        /// true if a calendar exists with the given identifier
        /// </returns>
        public bool CalendarExists(string calName)
        {
            _logger.Info("CalendarExists");

            return DoWithLock(() => _storage.CheckExists(calName),
                             string.Format("could not check if the calendar {0} exists", calName));
        }

        /// <summary>
        /// Determine whether a <see cref="T:Quartz.IJob"/> with the given identifier already
        ///             exists within the scheduler.
        /// </summary>
        /// <remarks/>
        /// <param name="jobKey">the identifier to check for</param>
        /// <returns>
        /// true if a job exists with the given identifier
        /// </returns>
        public bool CheckExists(JobKey jobKey)
        {
            _logger.Info("CheckExists - Job");
            return DoWithLock(() => _storage.CheckExists(jobKey),
                              string.Format("could not check if the job {0} exists", jobKey));
        }

        /// <summary>
        /// Determine whether a <see cref="T:Quartz.ITrigger"/> with the given identifier already
        ///             exists within the scheduler.
        /// </summary>
        /// <remarks/>
        /// <param name="triggerKey">the identifier to check for</param>
        /// <returns>
        /// true if a trigger exists with the given identifier
        /// </returns>
        public bool CheckExists(TriggerKey triggerKey)
        {
            _logger.Info("CheckExists - Trigger");
            return DoWithLock(() => _storage.CheckExists(triggerKey),
                            string.Format("could not check if the trigger {0} exists", triggerKey));
        }

        /// <summary>
        /// Clear (delete!) all scheduling data - all <see cref="T:Quartz.IJob"/>s, <see cref="T:Quartz.ITrigger"/>s
        ///             <see cref="T:Quartz.ICalendar"/>s.
        /// </summary>
        /// <remarks/>
        public void ClearAllSchedulingData()
        {
            _logger.Info("ClearAllSchedulingData");
            DoWithLock(() => _storage.ClearAllSchedulingData(), "Could not clear all the scheduling data");
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
        public void StoreCalendar(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers)
        {
            _logger.Info("StoreCalendar");
            DoWithLock(() => _storage.StoreCalendar(name, calendar, replaceExisting, updateTriggers),
                       string.Format("Error on store calendar - {0}", name));
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
        /// <param name="calName">The name of the <see cref="T:Quartz.ICalendar"/> to be removed.</param>
        /// <returns>
        /// <see langword="true"/> if a <see cref="T:Quartz.ICalendar"/> with the given name
        ///             was found and removed from the store.
        /// </returns>
        public bool RemoveCalendar(string calName)
        {
            _logger.Info("RemoveCalendar");
            return DoWithLock(() => _storage.RemoveCalendar(calName),
                       string.Format("Error on remvoing calendar - {0}", calName));
        }

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
            _logger.Info("RetrieveCalendar");
            return DoWithLock(() => _storage.RetrieveCalendar(calName),
                              string.Format("Error on retrieving calendar - {0}", calName));
        }

        /// <summary>
        /// Get the number of <see cref="T:Quartz.IJob"/>s that are
        ///             stored in the <see cref="T:Quartz.Spi.IJobStore"/>.
        /// </summary>
        /// <returns/>
        public int GetNumberOfJobs()
        {
            _logger.Info("GetNumberOfJobs");
            return DoWithLock(() => _storage.NumberOfJobs(), "Error on getting Number of jobs");
        }

        /// <summary>
        /// Get the number of <see cref="T:Quartz.ITrigger"/>s that are
        ///             stored in the <see cref="T:Quartz.Spi.IJobStore"/>.
        /// </summary>
        /// <returns/>
        public int GetNumberOfTriggers()
        {
            _logger.Info("GetNumberOfTriggers");
            return DoWithLock(() => _storage.NumberOfTriggers(), "Error on getting number of triggers");
        }

        /// <summary>
        /// Get the number of <see cref="T:Quartz.ICalendar"/> s that are
        ///             stored in the <see cref="T:Quartz.Spi.IJobStore"/>.
        /// </summary>
        /// <returns/>
        public int GetNumberOfCalendars()
        {
            _logger.Info("GetNumberOfCalendars");
            return DoWithLock(() => _storage.NumberOfCalendars(), "Error on getting number of calendars");
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
        public global::Quartz.Collection.ISet<JobKey> GetJobKeys(GroupMatcher<JobKey> matcher)
        {
            _logger.Info("GetJobKeys");
            return DoWithLock(() => _storage.JobKeys(matcher), "Error on getting job keys");
        }

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.ITrigger"/>s
        ///             that have the given group name.
        /// <para>
        /// If there are no triggers in the given group name, the result should be a
        ///             zero-length array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        public global::Quartz.Collection.ISet<TriggerKey> GetTriggerKeys(GroupMatcher<TriggerKey> matcher)
        {
            _logger.Info("GetTriggerKeys");
            return DoWithLock(() => _storage.TriggerKeys(matcher), "Error on getting trigger keys");
        }

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.IJob"/>
        ///             groups.
        /// <para>
        /// If there are no known group names, the result should be a zero-length
        ///             array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        public IList<string> GetJobGroupNames()
        {
            _logger.Info("GetJobGroupNames");
            return DoWithLock(() => _storage.JobGroupNames(), "Error on getting job group names");
        }

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.ITrigger"/>
        ///             groups.
        /// <para>
        /// If there are no known group names, the result should be a zero-length
        ///             array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        public IList<string> GetTriggerGroupNames()
        {
            _logger.Info("GetTriggerGroupNames");
            return DoWithLock(() => _storage.TriggerGroupNames(), "Error on getting trigger group names");
        }

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.ICalendar"/> s
        ///             in the <see cref="T:Quartz.Spi.IJobStore"/>.
        /// <para>
        /// If there are no Calendars in the given group name, the result should be
        ///             a zero-length array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        public IList<string> GetCalendarNames()
        {
            _logger.Info("GetCalendarNames");
            return DoWithLock(() => _storage.CalendarNames(), "Error on getting calendar names");
        }

        /// <summary>
        /// Get all of the Triggers that are associated to the given Job.
        /// </summary>
        /// <remarks>
        /// If there are no matches, a zero-length array should be returned.
        /// </remarks>
        public IList<IOperableTrigger> GetTriggersForJob(JobKey jobKey)
        {
            _logger.Info("GetTriggersForJob");
            return DoWithLock(() => _storage.GetTriggersForJob(jobKey), string.Format("Error on getting triggers for job - {0}", jobKey));
        }

        /// <summary>
        /// Get the current state of the identified <see cref="T:Quartz.ITrigger"/>.
        /// </summary>
        /// <seealso cref="T:Quartz.TriggerState"/>
        public TriggerState GetTriggerState(TriggerKey triggerKey)
        {
            _logger.Info("GetTriggerState");
            return DoWithLock(() => _storage.GetTriggerState(triggerKey),
                              string.Format("Error on getting trigger state for trigger - {0}", triggerKey));
        }

        /// <summary>
        /// Pause the <see cref="T:Quartz.ITrigger"/> with the given key.
        /// </summary>
        public void PauseTrigger(TriggerKey triggerKey)
        {
            _logger.Info("PauseTrigger");
            DoWithLock(() => _storage.PauseTrigger(triggerKey),
                              string.Format("Error on pausing trigger - {0}", triggerKey));
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
        public global::Quartz.Collection.ISet<string> PauseTriggers(GroupMatcher<TriggerKey> matcher)
        {
            _logger.Info("PauseTriggers");
            return DoWithLock(() => new global::Quartz.Collection.HashSet<string>(_storage.PauseTriggers(matcher)), "Error on pausing triggers");
        }

        /// <summary>
        /// Pause the <see cref="T:Quartz.IJob"/> with the given key - by
        ///             pausing all of its current <see cref="T:Quartz.ITrigger"/>s.
        /// </summary>
        public void PauseJob(JobKey jobKey)
        {
            _logger.Info("PauseJob");
            DoWithLock(() => _storage.PauseJob(jobKey), string.Format("Error on pausing job - {0}", jobKey));
        }

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
        public IList<string> PauseJobs(GroupMatcher<JobKey> matcher)
        {
            _logger.Info("PauseJobs");
            return DoWithLock(() => _storage.PauseJobs(matcher), "Error on pausing jobs");
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
        public void ResumeTrigger(TriggerKey triggerKey)
        {
            _logger.Info("ResumeTrigger");
            DoWithLock(() => _storage.ResumeTrigger(triggerKey),
                       string.Format("Error on resuming trigger - {0}", triggerKey));
        }

        /// <summary>
        /// Resume (un-pause) all of the <see cref="T:Quartz.ITrigger"/>s
        ///             in the given group.
        /// <para>
        /// If any <see cref="T:Quartz.ITrigger"/> missed one or more fire-times, then the
        ///             <see cref="T:Quartz.ITrigger"/>'s misfire instruction will be applied.
        /// </para>
        /// </summary>
        public IList<string> ResumeTriggers(GroupMatcher<TriggerKey> matcher)
        {
            _logger.Info("ResumeTriggers");
            return DoWithLock(() => _storage.ResumeTriggers(matcher), "Error on resume triggers");
        }

        /// <summary>
        /// Gets the paused trigger groups.
        /// </summary>
        /// <returns/>
        public global::Quartz.Collection.ISet<string> GetPausedTriggerGroups()
        {
            _logger.Info("GetPausedTriggerGroups");
            return DoWithLock(() => _storage.GetPausedTriggerGroups(), "Error on getting paused trigger groups");
        }

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
            _logger.Info("ResumeJob");
            DoWithLock(() => _storage.ResumeJob(jobKey), string.Format("Error on resuming job - {0}", jobKey));
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
        public global::Quartz.Collection.ISet<string> ResumeJobs(GroupMatcher<JobKey> matcher)
        {
            _logger.Info("ResumeJobs");
            return DoWithLock(() => _storage.ResumeJobs(matcher), "Error on resuming jobs");
        }

        /// <summary>
        /// Pause all triggers - equivalent of calling <see cref="M:Quartz.Spi.IJobStore.PauseTriggers(Quartz.Impl.Matchers.GroupMatcher{Quartz.TriggerKey})"/>
        ///             on every group.
        /// <para>
        /// When <see cref="M:Quartz.Spi.IJobStore.ResumeAll"/> is called (to un-pause), trigger misfire
        ///             instructions WILL be applied.
        /// </para>
        /// </summary>
        /// <seealso cref="M:Quartz.Spi.IJobStore.ResumeAll"/>
        public void PauseAll()
        {
            _logger.Info("PauseAll");
            DoWithLock(() => _storage.PauseAllTriggers(), "Error on pausing all");
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
        public void ResumeAll()
        {
            _logger.Info("ResumeAll");
            DoWithLock(() => _storage.ResumeAllTriggers(), "Error on resuming all");
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
            _logger.Info("AcquireNextTriggers");
            return DoWithLock(() => _storage.AcquireNextTriggers(noLaterThan, maxCount, timeWindow),
                              "Error on acquiring next triggers");
        }

        /// <summary>
        /// Inform the <see cref="T:Quartz.Spi.IJobStore"/> that the scheduler no longer plans to
        ///             fire the given <see cref="T:Quartz.ITrigger"/>, that it had previously acquired
        ///             (reserved).
        /// </summary>
        public void ReleaseAcquiredTrigger(IOperableTrigger trigger)
        {
            _logger.Info("ReleaseAcquiredTrigger");
            DoWithLock(() => _storage.ReleaseAcquiredTrigger(trigger), string.Format("Error on releasing acquired trigger - {0}", trigger));
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
        public IList<TriggerFiredResult> TriggersFired(IList<IOperableTrigger> triggers)
        {
            _logger.Info("TriggersFired");
            return DoWithLock(() => _storage.TriggersFired(triggers), "Error on Triggers Fired");
        }

        /// <summary>
        /// Inform the <see cref="T:Quartz.Spi.IJobStore"/> that the scheduler has completed the
        ///             firing of the given <see cref="T:Quartz.ITrigger"/> (and the execution its
        ///             associated <see cref="T:Quartz.IJob"/>), and that the <see cref="T:Quartz.JobDataMap"/>
        ///             in the given <see cref="T:Quartz.IJobDetail"/> should be updated if the <see cref="T:Quartz.IJob"/>
        ///             is stateful.
        /// </summary>
        public void TriggeredJobComplete(IOperableTrigger trigger, IJobDetail jobDetail, SchedulerInstruction triggerInstCode)
        {
            _logger.Info("TriggeredJobComplete");
            DoWithLock(() => _storage.TriggeredJobComplete(trigger, jobDetail, triggerInstCode),
                       string.Format("Error on triggered job complete - job:{0} - trigger:{1}", jobDetail, trigger));
        }


        #endregion


        #region private methods

        /// <summary>
        /// crud opertion to redis with lock 
        /// </summary>
        /// <typeparam name="T">return type of the Function</typeparam>
        /// <param name="fun">Fuction</param>
        /// <param name="errorMessage">error message used to override the default one</param>
        /// <returns></returns>
        private T DoWithLock<T>(Func<T> fun, string errorMessage = "Job Storage error")
        {
            try
            {
                _storage.LockWithWait();
                return fun.Invoke();
            }
            catch (ObjectAlreadyExistsException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(errorMessage, ex);
            }
            finally
            {
                _storage.Unlock();
            }
        }

        /// <summary>
        /// crud opertion to redis with lock 
        /// </summary>
        /// <param name="action">Action</param>
        /// <param name="errorMessage">error message used to override the default one</param>
        private void DoWithLock(Action action, string errorMessage = "Job Storage error")
        {
            try
            {
                _storage.LockWithWait();
                action.Invoke();
            }
            catch (ObjectAlreadyExistsException ex)
            {
                _logger.Error("key exists", ex);
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(errorMessage, ex);
            }
            finally
            {
                _storage.Unlock();
            }
        }

        #endregion
    }
}
