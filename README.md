# QuartzRedisJobStore
A Quartz Scheduler JobStore using Redis via C#

The project was a ported version of quartz-redis-jobstore (https://github.com/jlinn/quartz-redis-jobstore), currently it lacks of 
supporting redis-cluster. It uses StackExchange.Redis as the redis client. All the tests were run against Redis 2.8.* or higher.
In App.config under UnitTest, it contains some examples of configuring the quartz.properties.



## Limitations
The same limitations outlined in [redis-quartz's readme](https://github.com/jlinn/quartz-redis-jobstore#limitations) apply.
