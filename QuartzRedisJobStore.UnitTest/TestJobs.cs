using Quartz;
using System.Threading.Tasks;

namespace QuartzRedisJobStore.UnitTest
{
    /// <summary>
    /// test job, by default it allows concurrent execution.
    /// </summary>
    public class TestJob : IJob
    {
        #region Implementation of IJob
        /// <summary>
        /// Called by the <see cref="T:Quartz.IScheduler"/> when a <see cref="T:Quartz.ITrigger"/>
        ///             fires that is associated with the <see cref="T:Quartz.IJob"/>.
        /// </summary>
        /// <remarks>
        /// The implementation may wish to set a  result object on the 
        ///             JobExecutionContext before this method exits.  The result itself
        ///             is meaningless to Quartz, but may be informative to 
        ///             <see cref="T:Quartz.IJobListener"/>s or 
        ///             <see cref="T:Quartz.ITriggerListener"/>s that are watching the job's 
        ///             execution.
        /// </remarks>
        /// <param name="context">The execution context.</param>
        public Task Execute(IJobExecutionContext context) {
            return Task.CompletedTask;
        }
        #endregion
    }

    /// <summary>
    /// test job, it disallows concurrent execution
    /// </summary>
    [DisallowConcurrentExecution]
    public class NonConcurrentJob : TestJob {
        
    }

    /// <summary>
    /// test job, its jobdatamap will get persisted everytime when it gets executed. 
    /// </summary>
    [PersistJobDataAfterExecution]
    public class PersistJob : TestJob {
    }
}
