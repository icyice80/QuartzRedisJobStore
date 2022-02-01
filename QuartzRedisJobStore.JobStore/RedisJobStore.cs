using System;
using System.Collections.Generic;
using Quartz;
using Quartz.Impl.Matchers;
using Quartz.Spi;
using StackExchange.Redis;
using System.Threading.Tasks;
using System.Threading;
using Microsoft.Extensions.Logging;
using Quartz.Util;

namespace QuartzRedisJobStore.JobStore
{
    /// <summary>
    /// Redis Job Store 
    /// </summary>
    public class RedisJobStore : IJobStore, IDisposable
    {

        #region private fields
        /// <summary>
        /// logger
        /// </summary>
        ILogger logger = NullLogger.Instance;
        /// <summary>
        /// redis job store schema
        /// </summary>
        RedisJobStoreSchema storeSchema;

        /// <summary>
        /// redis db.
        /// </summary>
        IDatabase db;
        /// <summary>
        /// master/slave redis store.
        /// </summary>
        RedisStorage storage;
        #endregion

        #region public properties
        /// <summary>
        /// Indicates whether job store supports persistence.
        /// </summary>
        /// <returns/>
        public bool SupportsPersistence => true;

        /// <summary>
        /// How long (in milliseconds) the <see cref="T:Quartz.Spi.IJobStore"/> implementation  estimates that it will take to release a trigger and acquire a new one. 
        /// </summary>
        public long EstimatedTimeToReleaseAndAcquireTrigger => 200;

        /// <summary>
        /// Whether or not the <see cref="T:Quartz.Spi.IJobStore"/> implementation is clustered.
        /// </summary>
        /// <returns/>
        public bool Clustered => true;

        /// <summary>
        /// Inform the <see cref="T:Quartz.Spi.IJobStore"/> of the Scheduler instance's Id, prior to initialize being invoked.
        /// </summary>
        public string InstanceId { get; set; }

        /// <summary>
        /// Inform the <see cref="T:Quartz.Spi.IJobStore"/> of the Scheduler instance's name, prior to initialize being invoked.
        /// </summary>
        public string InstanceName { get; set; }

        /// <summary>
        /// Tells the JobStore the pool size used to execute jobs.
        /// </summary>
        public int ThreadPoolSize { get; set; }

        /// <summary>
        /// Redis Configuration - either fill this as a one string (redis://{Password}@{Host}:{Port}?ssl={Ssl}&db={Database})
        /// </summary>
        public string RedisConfiguration { get; set; }

        /// <summary>
        /// Type of some configured <see cref="ILoggerFactory" /> or <see cref="ILoggerProvider" />
        /// </summary>
        public string LoggerFactoryType { get; set; }

        /// <summary>
        /// Redis configuration - Host
        /// </summary>
        public string Host { get; set; }

        /// <summary>
        /// Redis configuration - Port
        /// </summary>
        public int Port { get; set; }

        /// <summary>
        /// Redis configuration - Port
        /// </summary>
        public int Database { get; set; }

        /// <summary>
        /// Redis configuration - Password
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        /// Redis configuration - SSL
        /// </summary>
        public bool Ssl { get; set; }

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
        public async Task Initialize(ITypeLoadHelper loadHelper, ISchedulerSignaler signaler, CancellationToken cancellationToken = default)
        {
            storeSchema = new RedisJobStoreSchema(KeyPrefix ?? string.Empty, KeyDelimiter ?? ":");

            if (!string.IsNullOrEmpty(LoggerFactoryType))
            {
                try
                {
                    using var factoryOrProvider = ObjectUtils.InstantiateType<IDisposable>(loadHelper.LoadType(LoggerFactoryType));
                    if (factoryOrProvider is ILoggerFactory factory)
                        logger = factory.CreateLogger<RedisJobStore>();
                    else if (factoryOrProvider is ILoggerProvider provider)
                        logger = provider.CreateLogger(nameof(RedisJobStore));
                }
                catch
                {
                }
            }

            ConfigurationOptions options;

            if (!string.IsNullOrEmpty(RedisConfiguration))
                options = ConfigurationOptions.Parse(RedisConfiguration);
            else
            {
                options = ConfigurationOptions.Parse($"{Host}:{Port}");
                options.Ssl = Ssl;
                options.Password = Password;
            }
            db = (await ConnectionMultiplexer.ConnectAsync(options)).GetDatabase(Database);
            storage = new RedisStorage(storeSchema, db, signaler, InstanceId, TriggerLockTimeout ?? 300000, RedisLockTimeout ?? 5000, logger);
        }

        /// <summary>
        /// Called by the QuartzScheduler to inform the <see cref="T:Quartz.Spi.IJobStore"/> that
        ///             the scheduler has started.
        /// </summary>
        public Task SchedulerStarted(CancellationToken cancellationToken = default)
        {
            logger.LogInformation("scheduler has started");
            return Task.CompletedTask;
        }

        /// <summary>
        /// Called by the QuartzScheduler to inform the JobStore that
        ///             the scheduler has been paused.
        /// </summary>
        public Task SchedulerPaused(CancellationToken cancellationToken = default)
        {
            logger.LogInformation("scheduler has paused");
            return Task.CompletedTask;
        }

        /// <summary>
        /// Called by the QuartzScheduler to inform the JobStore that
        ///             the scheduler has resumed after being paused.
        /// </summary>
        public Task SchedulerResumed(CancellationToken cancellationToken = default)
        {
            logger.LogInformation("scheduler has resumed");
            return Task.CompletedTask;
        }

        /// <summary>
        /// Called by the QuartzScheduler to inform the <see cref="T:Quartz.Spi.IJobStore"/> that
        ///             it should free up all of it's resources because the scheduler is
        ///             shutting down.
        /// </summary>
        public Task Shutdown(CancellationToken cancellationToken = default)
        {
            logger.LogInformation("scheduler has shutdown");
            db?.Multiplexer.Dispose();
            db = null;
            return Task.CompletedTask;
        }

        /// <summary>
        /// Store the given <see cref="T:Quartz.IJobDetail"/> and <see cref="T:Quartz.ITrigger"/>.
        /// </summary>
        /// <param name="newJob">The <see cref="T:Quartz.IJobDetail"/> to be stored.</param><param name="newTrigger">The <see cref="T:Quartz.ITrigger"/> to be stored.</param><throws>ObjectAlreadyExistsException </throws>
        public Task StoreJobAndTrigger(IJobDetail newJob, IOperableTrigger newTrigger, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("StoreJobAndTrigger");
            return DoWithLock(async () =>
            {
                await storage.StoreJobAsync(newJob, false);
                await storage.StoreTriggerAsync(newTrigger, false);
            }, "Could store job/trigger", cancellationToken);
        }

        /// <summary>
        /// returns true if the given JobGroup is paused
        /// </summary>
        /// <param name="groupName"/>
        /// <returns/>
        public Task<bool> IsJobGroupPaused(string groupName, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("IsJobGroupPaused");
            return DoWithLock(() => storage.IsJobGroupPausedAsync(groupName), string.Format("Error on IsJobGroupPaused - Group {0}", groupName), cancellationToken);
        }

        /// <summary>
        /// returns true if the given TriggerGroup
        ///             is paused
        /// </summary>
        /// <param name="groupName"/>
        /// <returns/>
        public Task<bool> IsTriggerGroupPaused(string groupName, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("IsTriggerGroupPaused");
            return DoWithLock(() => storage.IsTriggerGroupPausedAsync(groupName), string.Format("Error on IsTriggerGroupPaused - Group {0}", groupName), cancellationToken);
        }

        /// <summary>
        /// Store the given <see cref="T:Quartz.IJobDetail"/>.
        /// </summary>
        /// <param name="newJob">The <see cref="T:Quartz.IJobDetail"/> to be stored.</param><param name="replaceExisting">If <see langword="true"/>, any <see cref="T:Quartz.IJob"/> existing in the
        ///             <see cref="T:Quartz.Spi.IJobStore"/> with the same name and group should be over-written.
        ///             </param>
        public Task StoreJob(IJobDetail newJob, bool replaceExisting, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("StoreJob");
            return DoWithLock(() => storage.StoreJobAsync(newJob, replaceExisting), "Could not store job", cancellationToken);
        }

        /// <summary>
        /// Store jobs and triggers
        /// </summary>
        /// <param name="triggersAndJobs">jobs and triggers indexed by job</param>
        /// <param name="replace">indicate to repalce the existing ones or not</param>
        public async Task StoreJobsAndTriggers(IReadOnlyDictionary<IJobDetail, IReadOnlyCollection<ITrigger>> triggersAndJobs, bool replace, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("StoreJobsAndTriggers");
            foreach (var job in triggersAndJobs)
            {
                await DoWithLock(async () =>
                {
                    await storage.StoreJobAsync(job.Key, replace);
                    foreach (var trigger in job.Value)
                        await storage.StoreTriggerAsync(trigger, replace);
                }, "Could store job/trigger", cancellationToken);

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
        public Task<bool> RemoveJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("RemoveJob");
            return DoWithLock(() => storage.RemoveJobAsync(jobKey), "Could not remove a job", cancellationToken);
        }

        /// <summary>
        /// Remove jobs 
        /// </summary>
        /// <param name="jobKeys">JobKeys</param>
        /// <returns>succeeds or not</returns>
        public async Task<bool> RemoveJobs(IReadOnlyCollection<JobKey> jobKeys, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("RemoveJobs");
            var removed = false;

            foreach (var jobKey in jobKeys)
            {
                await DoWithLock(async () =>
                {
                    removed |= await storage.RemoveJobAsync(jobKey);
                }, "Error on removing job", cancellationToken);

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
        public Task<IJobDetail> RetrieveJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("RetrieveJob");
            return DoWithLock(() => storage.RetrieveJobAsync(jobKey), "Could not retrieve job", cancellationToken);
        }

        /// <summary>
        /// Store the given <see cref="T:Quartz.ITrigger"/>.
        /// </summary>
        /// <param name="newTrigger">The <see cref="T:Quartz.ITrigger"/> to be stored.</param><param name="replaceExisting">If <see langword="true"/>, any <see cref="T:Quartz.ITrigger"/> existing in
        ///             the <see cref="T:Quartz.Spi.IJobStore"/> with the same name and group should
        ///             be over-written.</param><throws>ObjectAlreadyExistsException </throws>
        public Task StoreTrigger(IOperableTrigger newTrigger, bool replaceExisting, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("StoreTrigger");
            return DoWithLock(() => storage.StoreTriggerAsync(newTrigger, replaceExisting), "Could not store trigger", cancellationToken);
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
        public Task<bool> RemoveTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("RemoveTrigger");
            return DoWithLock(() => storage.RemoveTriggerAsync(triggerKey), "Could not remove trigger", cancellationToken);
        }

        /// <summary>
        /// remove the requeste triggers by triggerKey
        /// </summary>
        /// <param name="triggerKeys">Trigger Keys</param>
        /// <returns>succeeds or not</returns>
        public async Task<bool> RemoveTriggers(IReadOnlyCollection<TriggerKey> triggerKeys, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("RemoveTriggers");
            var removed = false;

            foreach (var triggerKey in triggerKeys)
            {
                await DoWithLock(async () =>
                {
                    removed |= await storage.RemoveTriggerAsync(triggerKey);
                }, "Error on removing trigger", cancellationToken);
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
        public Task<bool> ReplaceTrigger(TriggerKey triggerKey, IOperableTrigger newTrigger, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("ReplaceTrigger");
            return DoWithLock(() => storage.ReplaceTriggerAsync(triggerKey, newTrigger), "Error on replacing trigger", cancellationToken);
        }

        /// <summary>
        /// Retrieve the given <see cref="T:Quartz.ITrigger"/>.
        /// </summary>
        /// <returns>
        /// The desired <see cref="T:Quartz.ITrigger"/>, or null if there is no
        ///             match.
        /// </returns>
        public Task<IOperableTrigger> RetrieveTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("RetrieveTrigger");
            return DoWithLock(() => storage.RetrieveTriggerAsync(triggerKey), "could not retrieve trigger", cancellationToken);
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
        public Task<bool> CalendarExists(string calName, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("CalendarExists");
            return DoWithLock(() => storage.CheckExistsAsync(calName), string.Format("could not check if the calendar {0} exists", calName), cancellationToken);
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
        public Task<bool> CheckExists(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("CheckExists - Job");
            return DoWithLock(() => storage.CheckExistsAsync(jobKey), string.Format("could not check if the job {0} exists", jobKey), cancellationToken);
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
        public Task<bool> CheckExists(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("CheckExists - Trigger");
            return DoWithLock(() => storage.CheckExistsAsync(triggerKey), string.Format("could not check if the trigger {0} exists", triggerKey), cancellationToken);
        }

        /// <summary>
        /// Clear (delete!) all scheduling data - all <see cref="T:Quartz.IJob"/>s, <see cref="T:Quartz.ITrigger"/>s
        ///             <see cref="T:Quartz.ICalendar"/>s.
        /// </summary>
        /// <remarks/>
        public Task ClearAllSchedulingData(CancellationToken cancellationToken = default)
        {
            logger.LogInformation("ClearAllSchedulingData");
            return DoWithLock(() => storage.ClearAllSchedulingData(), "Could not clear all the scheduling data", cancellationToken);
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
        public Task StoreCalendar(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("StoreCalendar");
            return DoWithLock(() => storage.StoreCalendarAsync(name, calendar, replaceExisting, updateTriggers), string.Format("Error on store calendar - {0}", name), cancellationToken);
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
        public Task<bool> RemoveCalendar(string calName, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("RemoveCalendar");
            return DoWithLock(() => storage.RemoveCalendarAsync(calName), string.Format("Error on remvoing calendar - {0}", calName), cancellationToken);
        }

        /// <summary>
        /// Retrieve the given <see cref="T:Quartz.ITrigger"/>.
        /// </summary>
        /// <param name="calName">The name of the <see cref="T:Quartz.ICalendar"/> to be retrieved.</param>
        /// <returns>
        /// The desired <see cref="T:Quartz.ICalendar"/>, or null if there is no
        ///             match.
        /// </returns>
        public Task<ICalendar> RetrieveCalendar(string calName, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("RetrieveCalendar");
            return DoWithLock(() => storage.RetrieveCalendarAsync(calName), string.Format("Error on retrieving calendar - {0}", calName), cancellationToken);
        }

        /// <summary>
        /// Get the number of <see cref="T:Quartz.IJob"/>s that are
        ///             stored in the <see cref="T:Quartz.Spi.IJobStore"/>.
        /// </summary>
        /// <returns/>
        public Task<int> GetNumberOfJobs(CancellationToken cancellationToken = default)
        {
            logger.LogInformation("GetNumberOfJobs");
            return DoWithLock(() => storage.NumberOfJobsAsync(), "Error on getting Number of jobs", cancellationToken);
        }

        /// <summary>
        /// Get the number of <see cref="T:Quartz.ITrigger"/>s that are
        ///             stored in the <see cref="T:Quartz.Spi.IJobStore"/>.
        /// </summary>
        /// <returns/>
        public Task<int> GetNumberOfTriggers(CancellationToken cancellationToken = default)
        {
            logger.LogInformation("GetNumberOfTriggers");
            return DoWithLock(() => storage.NumberOfTriggersAsync(), "Error on getting number of triggers", cancellationToken);
        }

        /// <summary>
        /// Get the number of <see cref="T:Quartz.ICalendar"/> s that are
        ///             stored in the <see cref="T:Quartz.Spi.IJobStore"/>.
        /// </summary>
        /// <returns/>
        public Task<int> GetNumberOfCalendars(CancellationToken cancellationToken = default)
        {
            logger.LogInformation("GetNumberOfCalendars");
            return DoWithLock(() => storage.NumberOfCalendarsAsync(), "Error on getting number of calendars", cancellationToken);
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
        public Task<IReadOnlyCollection<JobKey>> GetJobKeys(GroupMatcher<JobKey> matcher, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("GetJobKeys");
            return DoWithLock(() => storage.JobKeysAsync(matcher), "Error on getting job keys", cancellationToken);
        }

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.ITrigger"/>s
        ///             that have the given group name.
        /// <para>
        /// If there are no triggers in the given group name, the result should be a
        ///             zero-length array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        public Task<IReadOnlyCollection<TriggerKey>> GetTriggerKeys(GroupMatcher<TriggerKey> matcher, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("GetTriggerKeys");
            return DoWithLock(() => storage.TriggerKeysAsync(matcher), "Error on getting trigger keys", cancellationToken);
        }

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.IJob"/>
        ///             groups.
        /// <para>
        /// If there are no known group names, the result should be a zero-length
        ///             array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        public Task<IReadOnlyCollection<string>> GetJobGroupNames(CancellationToken cancellationToken = default)
        {
            logger.LogInformation("GetJobGroupNames");
            return DoWithLock(() => storage.JobGroupNamesAsync(), "Error on getting job group names", cancellationToken);
        }

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.ITrigger"/>
        ///             groups.
        /// <para>
        /// If there are no known group names, the result should be a zero-length
        ///             array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        public Task<IReadOnlyCollection<string>> GetTriggerGroupNames(CancellationToken cancellationToken = default)
        {
            logger.LogInformation("GetTriggerGroupNames");
            return DoWithLock(() => storage.TriggerGroupNamesAsync(), "Error on getting trigger group names", cancellationToken);
        }

        /// <summary>
        /// Get the names of all of the <see cref="T:Quartz.ICalendar"/> s
        ///             in the <see cref="T:Quartz.Spi.IJobStore"/>.
        /// <para>
        /// If there are no Calendars in the given group name, the result should be
        ///             a zero-length array (not <see langword="null"/>).
        /// </para>
        /// </summary>
        public Task<IReadOnlyCollection<string>> GetCalendarNames(CancellationToken cancellationToken = default)
        {
            logger.LogInformation("GetCalendarNames");
            return DoWithLock(() => storage.CalendarNamesAsync(), "Error on getting calendar names", cancellationToken);
        }

        /// <summary>
        /// Get all of the Triggers that are associated to the given Job.
        /// </summary>
        /// <remarks>
        /// If there are no matches, a zero-length array should be returned.
        /// </remarks>
        public Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("GetTriggersForJob");
            return DoWithLock(() => storage.GetTriggersForJobAsync(jobKey), string.Format("Error on getting triggers for job - {0}", jobKey), cancellationToken);
        }

        /// <summary>
        /// Get the current state of the identified <see cref="T:Quartz.ITrigger"/>.
        /// </summary>
        /// <seealso cref="T:Quartz.TriggerState"/>
        public Task<TriggerState> GetTriggerState(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("GetTriggerState");
            return DoWithLock(() => storage.GetTriggerStateAsync(triggerKey), string.Format("Error on getting trigger state for trigger - {0}", triggerKey), cancellationToken);
        }

        /// <summary>
        /// Pause the <see cref="T:Quartz.ITrigger"/> with the given key.
        /// </summary>
        public Task PauseTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("PauseTrigger");
            return DoWithLock(() => storage.PauseTriggerAsync(triggerKey), string.Format("Error on pausing trigger - {0}", triggerKey), cancellationToken);
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
        public async Task<IReadOnlyCollection<string>> PauseTriggers(GroupMatcher<TriggerKey> matcher, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("PauseTriggers");
            return await DoWithLock(() => storage.PauseTriggersAsync(matcher), "Error on pausing triggers", cancellationToken);
        }

        /// <summary>
        /// Pause the <see cref="T:Quartz.IJob"/> with the given key - by
        ///             pausing all of its current <see cref="T:Quartz.ITrigger"/>s.
        /// </summary>
        public Task PauseJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("PauseJob");
            return DoWithLock(() => storage.PauseJobAsync(jobKey), string.Format("Error on pausing job - {0}", jobKey), cancellationToken);
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
        public Task<IReadOnlyCollection<string>> PauseJobs(GroupMatcher<JobKey> matcher, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("PauseJobs");
            return DoWithLock(() => storage.PauseJobsAsync(matcher), "Error on pausing jobs", cancellationToken);
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
        public Task ResumeTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("ResumeTrigger");
            return DoWithLock(() => storage.ResumeTriggerAsync(triggerKey), string.Format("Error on resuming trigger - {0}", triggerKey), cancellationToken);
        }

        /// <summary>
        /// Resume (un-pause) all of the <see cref="T:Quartz.ITrigger"/>s
        ///             in the given group.
        /// <para>
        /// If any <see cref="T:Quartz.ITrigger"/> missed one or more fire-times, then the
        ///             <see cref="T:Quartz.ITrigger"/>'s misfire instruction will be applied.
        /// </para>
        /// </summary>
        public Task<IReadOnlyCollection<string>> ResumeTriggers(GroupMatcher<TriggerKey> matcher, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("ResumeTriggers");
            return DoWithLock(() => storage.ResumeTriggersAsync(matcher), "Error on resume triggers", cancellationToken);
        }

        /// <summary>
        /// Gets the paused trigger groups.
        /// </summary>
        /// <returns/>
        public Task<IReadOnlyCollection<string>> GetPausedTriggerGroups(CancellationToken cancellationToken = default)
        {
            logger.LogInformation("GetPausedTriggerGroups");
            return DoWithLock(() => storage.GetPausedTriggerGroupsAsync(), "Error on getting paused trigger groups", cancellationToken);
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
        public Task ResumeJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("ResumeJob");
            return DoWithLock(() => storage.ResumeJobAsync(jobKey), string.Format("Error on resuming job - {0}", jobKey), cancellationToken);
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
        public Task<IReadOnlyCollection<string>> ResumeJobs(GroupMatcher<JobKey> matcher, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("ResumeJobs");
            return DoWithLock(() => storage.ResumeJobsAsync(matcher), "Error on resuming jobs", cancellationToken);
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
        public Task PauseAll(CancellationToken cancellationToken = default)
        {
            logger.LogInformation("PauseAll");
            return DoWithLock(() => storage.PauseAllTriggersAsync(), "Error on pausing all", cancellationToken);
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
        public Task ResumeAll(CancellationToken cancellationToken = default)
        {
            logger.LogInformation("ResumeAll");
            return DoWithLock(() => storage.ResumeAllTriggersAsync(), "Error on resuming all", cancellationToken);
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
        public Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggers(DateTimeOffset noLaterThan, int maxCount, TimeSpan timeWindow, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("AcquireNextTriggers");
            return DoWithLock(() => storage.AcquireNextTriggersAsync(noLaterThan, maxCount, timeWindow), "Error on acquiring next triggers", cancellationToken);
        }

        /// <summary>
        /// Inform the <see cref="T:Quartz.Spi.IJobStore"/> that the scheduler no longer plans to
        ///             fire the given <see cref="T:Quartz.ITrigger"/>, that it had previously acquired
        ///             (reserved).
        /// </summary>
        public Task ReleaseAcquiredTrigger(IOperableTrigger trigger, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("ReleaseAcquiredTrigger");
            return DoWithLock(() => storage.ReleaseAcquiredTriggerAsync(trigger), string.Format("Error on releasing acquired trigger - {0}", trigger), cancellationToken);
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
        public Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFired(IReadOnlyCollection<IOperableTrigger> triggers, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("TriggersFired");
            return DoWithLock(() => storage.TriggersFiredAsync(triggers), "Error on Triggers Fired", cancellationToken);
        }

        /// <summary>
        /// Inform the <see cref="T:Quartz.Spi.IJobStore"/> that the scheduler has completed the
        ///             firing of the given <see cref="T:Quartz.ITrigger"/> (and the execution its
        ///             associated <see cref="T:Quartz.IJob"/>), and that the <see cref="T:Quartz.JobDataMap"/>
        ///             in the given <see cref="T:Quartz.IJobDetail"/> should be updated if the <see cref="T:Quartz.IJob"/>
        ///             is stateful.
        /// </summary>
        public Task TriggeredJobComplete(IOperableTrigger trigger, IJobDetail jobDetail, SchedulerInstruction triggerInstCode, CancellationToken cancellationToken = default)
        {
            logger.LogInformation("TriggeredJobComplete");
            return DoWithLock(() => storage.TriggeredJobCompleteAsync(trigger, jobDetail, triggerInstCode), string.Format("Error on triggered job complete - job:{0} - trigger:{1}", jobDetail, trigger), cancellationToken);
        }
        #endregion

        #region private methods

        /// <summary>
        /// crud opertion to redis with lock 
        /// </summary>
        /// <typeparam name="T">return type of the Function</typeparam>
        /// <param name="asyncFunction">Fuction</param>
        /// <param name="errorMessage">error message used to override the default one</param>
        /// <returns></returns>
        async Task<T> DoWithLock<T>(Func<Task<T>> asyncFunction, string errorMessage = "Job Storage error", CancellationToken cancellationToken = default)
        {
            string lockValue = null;
            try
            {
                lockValue = await storage.LockWithWait(cancellationToken);
                return await asyncFunction.Invoke();
            }
            catch (ObjectAlreadyExistsException ex)
            {
                logger.Log(LogLevel.Error, ex, "key exists");
                throw;
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(errorMessage, ex);
            }
            finally
            {
                if (lockValue != null)
                    await storage.UnlockAsync(lockValue);
            }
        }

        /// <summary>
        /// crud opertion to redis with lock 
        /// </summary>
        /// <param name="asyncAction">Action</param>
        /// <param name="errorMessage">error message used to override the default one</param>
        async Task DoWithLock(Func<Task> asyncAction, string errorMessage = "Job Storage error", CancellationToken cancellationToken = default)
        {
            string lockValue = null;
            try
            {
                lockValue = await storage.LockWithWait(cancellationToken);
                await asyncAction.Invoke();
            }
            catch (ObjectAlreadyExistsException ex)
            {
                logger.Log(LogLevel.Error, ex, "key exists");
                throw;
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(errorMessage, ex);
            }
            finally
            {
                if (lockValue != null)
                    await storage.UnlockAsync(lockValue);
            }
        }

        public void Dispose()
        {
            db?.Multiplexer.Dispose();
            db = null;
        }
        #endregion
    }

    public class NullLogger : ILogger
    {
        public static ILogger Instance { get; } = new NullLogger();

        public IDisposable BeginScope<TState>(TState state) => null;
        public bool IsEnabled(LogLevel logLevel) => false;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
        }
    }
}
