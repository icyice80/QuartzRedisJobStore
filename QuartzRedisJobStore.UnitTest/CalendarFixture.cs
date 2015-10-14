using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Quartz;
using QuartzRedisJobStore.JobStore;

namespace QuartzRedisJobStore.UnitTest
{
    /// <summary>
    /// calendar related tests
    /// </summary>
    [TestClass]
    public class CalendarFixture : BaseFixture
    {
        /// <summary>
        /// calendarName
        /// </summary>
        const string CalendarName = "iCalendar";

        /// <summary>
        /// Clean up after the test completes
        /// </summary>
        [TestCleanup]
        public void ClearAllJobStoreData()
        {
            System.Diagnostics.Debug.Write("here");
            JobStore?.ClearAllSchedulingData();
            System.Diagnostics.Debug.Write(counter++);
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            CleanUp();
        }

        /// <summary>
        /// store a calendar
        /// </summary>
        [TestMethod]
        public void StoreCalendarSuccesfully() {
            //arrange
            var calendar = CreateCalendar();
            var calendarHashKey = Schema.CalendarHashKey(CalendarName);

            //act
            JobStore.StoreCalendar(CalendarName,calendar,false,false);
            var calendarProperties = Db.HashGetAll(calendarHashKey);
            var serializedCalendar = (from hashEntry in calendarProperties
                                      where hashEntry.Name == RedisJobStoreSchema.CalendarSerialized
                                      select hashEntry.Value).FirstOrDefault();

            var retrievedCalendar = JsonConvert.DeserializeObject(serializedCalendar, _serializerSettings) as ICalendar;

            //assert
            Assert.IsNotNull(retrievedCalendar);
            Assert.AreEqual(retrievedCalendar.Description,calendar.Description);

        }

        /// <summary>
        /// try to store another calendar with the same name, set replacing to false, then 
        /// the original one will not be overriden.
        /// </summary>
        [TestMethod]
        public void StoreCalendar_WithoutReplacingExisting_NoOverride() {
            //arrange
            var calendar1 = CreateCalendar();
            var calendar2 = CreateCalendar("another week days only");

            //act
            JobStore.StoreCalendar(CalendarName,calendar1,false,false);
            JobStore.StoreCalendar(CalendarName,calendar2,false,false);
            var retrievedCalendar = JobStore.RetrieveCalendar(CalendarName);

            //assert    
            Assert.AreEqual(retrievedCalendar.Description,calendar1.Description);
        }

        /// <summary>
        /// try to store another calendar with the same name, set replacing to true, then 
        /// the original one will be overriden.
        /// </summary>
        [TestMethod]
        public void StoreCalendar_WithReplacingExisting_OverrideSuccessfully()
        {
            //arrange
            var calendar1 = CreateCalendar();
            var calendar2 = CreateCalendar("another week days only");

            //act
            JobStore.StoreCalendar(CalendarName, calendar1, false, false);
            JobStore.StoreCalendar(CalendarName, calendar2, true, false);
            var retrievedCalendar = JobStore.RetrieveCalendar(CalendarName);

            //assert    
            Assert.AreEqual(retrievedCalendar.Description, calendar2.Description);
        }

        /// <summary>
        /// retrieve a calendar
        /// </summary>
        [TestMethod]
        public void RetrieveCalendarSuccessfully() {
            //arrange
            var calendar = CreateCalendar();
            JobStore.StoreCalendar(CalendarName, calendar, true, false);

            //act
            var retrievedCalendar = JobStore.RetrieveCalendar(CalendarName);

            //assert
            Assert.AreEqual(calendar.Description,retrievedCalendar.Description);
            var utcNow = new DateTimeOffset(DateTime.UtcNow);
            Assert.AreEqual(calendar.GetNextIncludedTimeUtc(utcNow),retrievedCalendar.GetNextIncludedTimeUtc(utcNow));
        }

        /// <summary>
        /// get total number of calendars in the store
        /// </summary>
        [TestMethod]
        public void GetNumberOfCalendarSuccessfully() {
            //arrange
            JobStore.StoreCalendar("cal1", CreateCalendar(), true, false);
            JobStore.StoreCalendar("cal2", CreateCalendar(), true, false);
            JobStore.StoreCalendar("cal3", CreateCalendar(), true, false);

            //act
            var numbers = JobStore.GetNumberOfCalendars();

            //assert
            Assert.IsTrue(numbers == 3);
        }

        /// <summary>
        /// remove a calendar
        /// </summary>
        [TestMethod]
        public void RemoveCalendarSuccessfully() {
            //arrange
            JobStore.StoreCalendar(CalendarName,CreateCalendar(),false,false);
            
            //act
            var result = JobStore.RemoveCalendar(CalendarName);

            //assert
            Assert.IsTrue(result);
            Assert.IsNull(JobStore.RetrieveCalendar(CalendarName));
        }

        /// <summary>
        /// Get all the calendar names in the store
        /// </summary>
        [TestMethod]
        public void GetCalendarNamesSuccessfully() {
            //arrange
            JobStore.StoreCalendar("cal1",CreateCalendar(),false,false);
            JobStore.StoreCalendar("cal2", CreateCalendar(), false, false);

            //act
            var result = JobStore.GetCalendarNames();

            //assert
            Assert.IsTrue(result.Count == 2);
        }

        /// <summary>
        /// Calendar could not be removed then there are triggers associated with it. 
        /// </summary>
        [TestMethod, ExpectedException(typeof(JobPersistenceException))]
        public void RemoveCalendar_WhenActiveTriggerAssociatedWith_Unsuccessfully() {
            //arrange
            var job = CreateJob();
            JobStore.StoreJob(job,false);
            var trigger = CreateTrigger("trigger", "triggerGroup", job.Key);
            JobStore.StoreTrigger(trigger,false);

            //act
            JobStore.RemoveCalendar(trigger.CalendarName);
        }
    }
}
