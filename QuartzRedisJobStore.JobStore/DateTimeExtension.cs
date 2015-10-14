using System;

namespace QuartzRedisJobStore.JobStore
{
    /// <summary>
	/// Extension methods for <see cref="DateTime"/>
	/// </summary>
    public static class DateTimeExtension
    {
        /// <summary>
        /// unix time in milliseconds
        /// </summary>
        /// <param name="date">DateTime in utc</param>
        /// <returns></returns>
	    public static double ToUnixTimeMilliSeconds(this DateTime date)
        {
            TimeSpan span = date - new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            return span.TotalMilliseconds;
        }
    }
}
