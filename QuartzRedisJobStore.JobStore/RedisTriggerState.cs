using System.ComponentModel.DataAnnotations;

namespace QuartzRedisJobStore.JobStore
{
    /// <summary>
    /// Trigger State Enum
    /// </summary>
    public enum RedisTriggerState
    {
        /// <summary>
        /// waiting
        /// </summary>
        [Display(Name ="waitting_triggers")]
        Waiting,

        /// <summary>
        /// paused
        /// </summary>
        [Display(Name = "paused_triggers")]
        Paused,

        /// <summary>
        /// blocked
        /// </summary>
        [Display(Name = "blocked_triggers")]
        Blocked,

        /// <summary>
        /// pause blocked
        /// </summary>
        [Display(Name = "paused_blocked_triggers")]
        PausedBlocked,

        /// <summary>
        /// acquired
        /// </summary>
        [Display(Name = "acquired_triggers")]
        Acquired,

        /// <summary>
        /// completed
        /// </summary>
        [Display(Name = "completed_triggers")]
        Completed,

        /// <summary>
        /// error
        /// </summary>
        [Display(Name = "error_triggers")]
        Error
    }
}
