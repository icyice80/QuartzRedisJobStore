using System;
using System.Collections.Generic;
using System.Linq;
using Quartz;

namespace QuartzRedisJobStore.JobStore
{
    /// <summary>
    /// schema class for creating keys for hash, set etc. 
    /// </summary>
    public class RedisJobStoreSchema {

        /// <summary>
        /// Default lock name
        /// </summary>
        private const string DefaultLockName = "lock";

        #region Job related key names
        /// <summary>
        /// Job Class Name
        /// </summary>
        public const string JobClass = "job_class_name";
        /// <summary>
        /// Description
        /// </summary>
        public const string Description = "description";
        /// <summary>
        /// Is Durable
        /// </summary>
        public const string IsDurable = "is_durable";

        /// <summary>
        /// Request Recovery
        /// </summary>
        public const string RequestRecovery = "request_recovery";
        /// <summary>
        /// Blocked By
        /// </summary>
        public const string BlockedBy = "blocked_by";
        /// <summary>
        /// block time
        /// </summary>
        public const string BlockTime = "block_time";

        /// <summary>
        /// set name for jobs
        /// </summary>
        private const string JobsSet = "jobs";
        /// <summary>
        /// set name for all the groups who have jobs.
        /// </summary>
        private const string JobGroupsSet = "job_groups";
        #endregion

        #region trigger related key names
        /// <summary>
        /// hash field name - job_hash_key
        /// </summary>
        public const string JobHash = "job_hash_key";
        /// <summary>
        /// hash field name - next_fire_time
        /// </summary>
        public const string NextFireTime = "next_fire_time";
        /// <summary>
        /// hash field name - prev_fire_time
        /// </summary>
        public const string PrevFireTime = "prev_fire_time";
        /// <summary>
        /// hash field name - priority
        /// </summary>
        public const string Priority = "priority";
        /// <summary>
        /// hash field name - trigger_type
        /// </summary>
        public const string TriggerType = "trigger_type";
        /// <summary>
        /// hash field name - calendar_name
        /// </summary>
        public const string CalendarName = "calendar_name";
        /// <summary>
        /// hash field name - start_time
        /// </summary>
        public const string StartTime = "start_time";
        /// <summary>
        /// hash field name - end_time
        /// </summary>
        public const string EndTime = "end_time";
        /// <summary>
        /// hash field name - final_fire_time
        /// </summary>
        public const string FinalFireTime = "final_fire_time";
        /// <summary>
        /// hash field name - fire_instance_id
        /// </summary>
        public const string FireInstanceId = "fire_instance_id";
        /// <summary>
        /// hash field name - misfire_instruction
        /// </summary>
        public const string MisfireInstruction = "misfire_instruction";
        /// <summary>
        /// hash field name - locked_by
        /// </summary>
        public const string LockedBy = "locked_by";
        /// <summary>
        /// hash field name - lock_time
        /// </summary>
        public const string LockTime = "lock_time";

        /// <summary>
        /// hash value - Simple Trigger Type string
        /// </summary>
        public const string TriggerTypeSimple = "SIMPLE";
        /// <summary>
        /// hash field name - repeat_count
        /// </summary>
        public const string RepeatCount = "repeat_count";
        /// <summary>
        /// hash field name - repeat_interval
        /// </summary>
        public const string RepeatInterval = "repeat_interval";
        /// <summary>
        /// hash field name - times_triggered
        /// </summary>
        public const string TimesTriggered = "times_triggered";

      
        /// <summary>
        /// hash value - Cron Trigger Type string
        /// </summary>
        public const string TriggerTypeCron = "CRON";
        /// <summary>
        /// hash field name - cron_Expression
        /// </summary>
        public const string CronExpression = "cron_expression";
        /// <summary>
        /// hash field name - time_zone_id
        /// </summary>
        public const string TimeZoneId = "time_zone_id";

        #endregion
        /// <summary>
        /// hash field name - calendar_serialized.
        /// </summary>
	    public const string CalendarSerialized = "calendar_serialized";

        /// <summary>
        /// Default Delimiter
        /// </summary>
        private const string DefaultDelimiter = ":";

        /// <summary>
        /// name for prefix used to support different schedulers 
        /// </summary>
        private readonly string _prefix;
        /// <summary>
        /// delimiter 
        /// </summary>
        private readonly string _delimiter;


        /// <summary>
        /// constructor, with no prefix
        /// </summary>
        public RedisJobStoreSchema()
            : this(string.Empty)
        {

        }

        /// <summary>
        /// constructor with the customized prefix
        /// </summary>
        /// <param name="prefix">prefix</param>
        public RedisJobStoreSchema(string prefix)
            : this(prefix, DefaultDelimiter)
        {

        }

        /// <summary>
        /// constructor
        /// </summary>
        /// <param name="prefix">prefix</param>
        /// <param name="delimiter">delimiter</param>
        public RedisJobStoreSchema(string prefix, string delimiter)
        {
            _prefix = prefix;
            _delimiter = delimiter;
        }

        /// <summary>
        /// construct a hash key for job
        /// </summary>
        /// <param name="jobKey">Job Key</param>
        /// <returns>hash key</returns>
        public string JobHashKey(JobKey jobKey)
        {
            return this.AddPrefix("job" + _delimiter + jobKey.Group + _delimiter + jobKey.Name);
        }

        /// <summary>
        /// construct a hash Key for jobdatamap
        /// </summary>
        /// <param name="jobKey">Job Key</param>
        /// <returns>hash key</returns>
        public string JobDataMapHashKey(JobKey jobKey)
        {
            return this.AddPrefix("job_data_map" + _delimiter + jobKey.Group + _delimiter + jobKey.Name);
        }

        /// <summary>
        /// construct a set key for a particular group as a jobgroup could have many names.
        /// </summary>
        /// <param name="groupName">Group Name</param>
        /// <returns>key for the set</returns>
        public String JobGroupSetKey(string groupName)
        {
            return AddPrefix("job_group" + _delimiter + groupName);
        }

       

        /// <summary>
        /// set key for holding all the jobs.
        /// </summary>
        /// <returns>set key</returns>
        public String JobsSetKey()
        {
            return this.AddPrefix(JobsSet);
        }

        /// <summary>
        /// set kye for holding all the job groups
        /// </summary>
        /// <returns>set key</returns>
        public String JobGroupsSetKey()
        {
            return this.AddPrefix(JobGroupsSet);
        }

        /// <summary>
        /// construct a jobkey based on jobhashkey
        /// </summary>
        /// <param name="jobHashKey">hash key for a job</param>
        /// <returns>JobKey</returns>
        public JobKey JobKey(string jobHashKey){
            var hashParts = this.Split(jobHashKey);
            return new JobKey(hashParts[2], hashParts[1]);
        }

        /// <summary>
        /// set key for blocked jobs. when the trigger fires, the job of it will be saved into this set, then when trigger completes, the job will be removed from it. 
        /// </summary>
        /// <returns>set key</returns>
        public string BlockedJobsSet()
        {
            return AddPrefix("blocked_jobs");
        }

        /// <summary>
        /// a set key for holding up the triggers for a special job.
        /// </summary>
        /// <param name="jobKey">JobKey</param>
        /// <returns>set key</returns>
        public string JobTriggersSetKey(JobKey jobKey)
        {
            return this.AddPrefix("job_triggers" + _delimiter + jobKey.Group + _delimiter + jobKey.Name);
        }

        /// <summary>
        /// construct a hash key for a trigger
        /// </summary>
        /// <param name="triggerKey">TriggerKey</param>
        /// <returns>hash key</returns>
        public string TriggerHashkey(TriggerKey triggerKey)
        {
            return this.AddPrefix("trigger" + _delimiter + triggerKey.Group + _delimiter + triggerKey.Name);
        }

        /// <summary>
        /// get the trigger group name based on triggerGroup Set key
        /// </summary>
        /// <param name="triggerGroupSetKey">triggerGroupSetKey</param>
        /// <returns>Trigger group name</returns>
        public String TriggerGroup(string triggerGroupSetKey){
            return this.Split(triggerGroupSetKey)[1];
        }

        /// <summary>
        /// construct a set key for trigger group, a group could have many triggers, a trigger belongs to a group.
        /// </summary>
        /// <param name="group">Group</param>
        /// <returns>set key</returns>
        public string TriggerGroupSetKey(string group)
        {
            return this.AddPrefix("trigger_group" + _delimiter + group);
        }

        /// <summary>
        /// a set key which holds all the triggers.
        /// </summary>
        /// <returns>set key</returns>
        public string TriggersSetKey()
        {
            return this.AddPrefix("triggers");
        }

        /// <summary>
        /// a set key which holds all the trigger_groups
        /// </summary>
        /// <returns>set key</returns>
        public String TriggerGroupsSetKey()
        {
            return AddPrefix("trigger_groups");
        }

        /// <summary>
        /// a set key which holds all the trigger groups whose state are paused.
        /// </summary>
        /// <returns>set key</returns>
        public string PausedTriggerGroupsSetKey()
        {
            return AddPrefix("paused_trigger_groups");
        }

        /// <summary>
        /// a set key which holds all the job groups whose state are paused.
        /// </summary>
        /// <returns>set key</returns>
        public String PausedJobGroupsSetKey()
        {
            return AddPrefix("paused_job_groups");
        }

        /// <summary>
        /// construct a triggerkey based the trigger's hashkey
        /// </summary>
        /// <param name="triggerHashKey">Trigger Hash Key</param>
        /// <returns>TriggerKey</returns>
        public TriggerKey TriggerKey(String triggerHashKey)
        {
            var hashParts = triggerHashKey.Split(new[] { _delimiter }, StringSplitOptions.None);
            return new TriggerKey(hashParts[2], hashParts[1]);
        }

        /// <summary>
        /// construct the corresponding sorted set key for triggers based on its triggerState
        /// </summary>
        /// <param name="triggerState">RedisTriggerState</param>
        /// <returns>sorted set key</returns>
        public string TriggerStateSetKey(RedisTriggerState triggerState)
        {
            return this.AddPrefix(triggerState.GetDisplayName());
        }

        /// <summary>
        /// lock key for the trigger
        /// </summary>
        /// <param name="triggerKey">TriggerKey</param>
        /// <returns>a key</returns>
        public String TriggerLockKey(TriggerKey triggerKey)
        {
            return AddPrefix("trigger_lock" + _delimiter + triggerKey.Group + _delimiter + triggerKey.Name);
        }

        /// <summary>
        /// lock key for the job
        /// </summary>
        /// <param name="jobKey">JobKey</param>
        /// <returns>a key</returns>
        public String JobBlockedKey(JobKey jobKey){
            return AddPrefix("job_blocked" + _delimiter + jobKey.Group + _delimiter + jobKey.Name);
        }

        /// <summary>
        /// construct a set key for a special calendar, who could have many triggers.
        /// </summary>
        /// <param name="calendarName">Calendar Name</param>
        /// <returns>set key</returns>
        public string CalendarTriggersSetKey(string calendarName)
        {
            return AddPrefix("calendar_triggers" + _delimiter + calendarName);
        }


        /// <summary>
        /// construct a hash key for a calendar
        /// </summary>
        /// <param name="calendarName">CalendarName</param>
        /// <returns>hash key</returns>
        public string CalendarHashKey(string calendarName)
        {
            return AddPrefix("calendar" + _delimiter + calendarName);
        }

        /// <summary>
        /// get the calendarName based on the its hash key
        /// </summary>
        /// <param name="calendarHashKey">Calendar Hash Key</param>
        /// <returns>Calendar Name</returns>
        public string GetCalendarName(string calendarHashKey)
        {
            return Split(calendarHashKey)[1];
        }

        /// <summary>
        /// a set key which contains all the calendar hash keys.
        /// </summary>
        /// <returns></returns>
        public string CalendarsSetKey()
        {
            return AddPrefix("calendars");
        }

        /// <summary>
        /// Get the job group base the jobgroupset key
        /// </summary>
        /// <param name="jobGroupSetKey">jobGroupSetKey</param>
        /// <returns>Job's Group</returns>
        public String JobGroup(string jobGroupSetKey){
            return Split(jobGroupSetKey)[1];
        }

        /// <summary>
        /// construct a key for LastTriggerReleaseTime which is used to check for releaseing the orphaned triggers.
        /// </summary>
        /// <returns></returns>
        public String LastTriggerReleaseTime()
        {
            return AddPrefix("last_triggers_release_time");
        }

       
        /// <summary>
        /// get the lock key for redis. 
        /// </summary>
        public string LockKey
        {
            get
            {
                return this.AddPrefix(DefaultLockName);
            }
        }

        /// <summary>
        /// prefix the keys
        /// </summary>
        /// <param name="key">key</param>
        /// <returns>key in redis</returns>
        private String AddPrefix(String key)
        {
            return _prefix + key;
        }

        /// <summary>
        /// split the string into a list based on the delimiter.
        /// </summary>
        /// <param name="val"></param>
        /// <returns></returns>
        internal List<String> Split(string val)
        {
            return (val.Split(new [] { _delimiter }, StringSplitOptions.None)).ToList();
        }
    }
}
