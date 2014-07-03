package com.continuuity.internal.app.scheduler;

import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetsModules;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.internal.app.runtime.schedule.DataSetBasedScheduleStore;
import com.continuuity.internal.app.runtime.schedule.ScheduleStoreTableUtil;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.continuuity.test.SlowTests;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.simpl.RAMJobStore;
import org.quartz.simpl.SimpleThreadPool;
import org.quartz.spi.JobStore;

import java.util.List;

/**
*
*/
@Category(SlowTests.class)
public class SchedulerTest {

  private static Injector injector;
  private static Scheduler scheduler;
  private static TransactionExecutorFactory factory;
  private static DataSetAccessor accessor;

  @BeforeClass
  public static void setup() throws Exception {
    injector = Guice.createInjector (new LocationRuntimeModule().getInMemoryModules(),
                                     new DiscoveryRuntimeModule().getInMemoryModules(),
                                     new MetricsClientRuntimeModule().getInMemoryModules(),
                                     new DataFabricModules().getInMemoryModules(),
                                     new DataSetsModules().getInMemoryModule());
    injector.getInstance(InMemoryTransactionManager.class).startAndWait();
    accessor = injector.getInstance(DataSetAccessor.class);
    factory = injector.getInstance(TransactionExecutorFactory.class);
  }

  public static void schedulerSetup(boolean enablePersistence, String schedulerName)
    throws SchedulerException, UnsupportedTypeException {
    JobStore js;
    if (enablePersistence) {
      js = new DataSetBasedScheduleStore(factory, new ScheduleStoreTableUtil(accessor));
    } else {
      js = new RAMJobStore();
    }

    SimpleThreadPool threadPool = new SimpleThreadPool(10, Thread.NORM_PRIORITY);
    threadPool.initialize();
    DirectSchedulerFactory.getInstance().createScheduler(schedulerName, "1", threadPool, js);

    scheduler = DirectSchedulerFactory.getInstance().getScheduler(schedulerName);
    scheduler.start();
  }

  public static void schedulerTearDown() throws SchedulerException {
    scheduler.shutdown();
  }

  @Test
  public void testSchedulerWithoutPersistence() throws SchedulerException, UnsupportedTypeException {
    String schedulerName = "NonPersistentScheduler";
    //start scheduler without enabling persistence.
    schedulerSetup(false, schedulerName);
    JobDetail job = JobBuilder.newJob(LogPrintingJob.class)
                              .withIdentity("developer:application1:mapreduce1")
                              .build();

    Trigger trigger  = TriggerBuilder.newTrigger()
                                     .withIdentity("g1")
                                     .startNow()
                                     .withSchedule(CronScheduleBuilder.cronSchedule("0 0/5 * * * ?"))
                                     .build();

    JobKey key =  job.getKey();

    //Schedule job
    scheduler.scheduleJob(job, trigger);

    //Get the job stored.
    JobDetail jobStored = scheduler.getJobDetail(job.getKey());
    List<? extends Trigger> triggers = scheduler.getTriggersOfJob(job.getKey());

    Assert.assertEquals(jobStored.getKey().getName(), key.getName());
    Assert.assertEquals(1, triggers.size());

    //Shutdown scheduler.
    schedulerTearDown();

    //restart scheduler.
    schedulerSetup(false, schedulerName);

   //read the job
    jobStored = scheduler.getJobDetail(job.getKey());
    // The job with old job key should not exist since it is not persisted.
    Assert.assertNull(jobStored);
    schedulerTearDown();
  }

  @Test
  public void testSchedulerWithPersistence() throws SchedulerException,
                                                    UnsupportedTypeException {
    String schedulerName = "persistentScheduler";
    //start scheduler enabling persistence.
    schedulerSetup(true, schedulerName);
    JobDetail job = JobBuilder.newJob(LogPrintingJob.class)
      .withIdentity("developer:application1:mapreduce2")
      .build();

    Trigger trigger  = TriggerBuilder.newTrigger()
      .withIdentity("p1")
      .startNow()
      .withSchedule(CronScheduleBuilder.cronSchedule("0 0/5 * * * ?"))
      .build();

    JobKey key =  job.getKey();

    //Schedule job
    scheduler.scheduleJob(job, trigger);

    //Get the job stored.
    JobDetail jobStored = scheduler.getJobDetail(job.getKey());
    List<? extends Trigger> triggers = scheduler.getTriggersOfJob(job.getKey());

    Assert.assertEquals(jobStored.getKey().getName(), key.getName());
    Assert.assertEquals(1, triggers.size());

    //Shutdown scheduler.
    schedulerTearDown();

    //restart scheduler.
    schedulerSetup(true, schedulerName);

    //read the job
    jobStored = scheduler.getJobDetail(job.getKey());
    // The job with old job key should exist since it is persisted in Dataset
    Assert.assertNotNull(jobStored);
    Assert.assertEquals(jobStored.getKey().getName(), key.getName());

    triggers = scheduler.getTriggersOfJob(job.getKey());
    Assert.assertEquals(1, triggers.size());

    schedulerTearDown();
  }

  @AfterClass
  public static void cleanup() throws SchedulerException, InterruptedException {
    schedulerTearDown();
    Thread.sleep(10000);
  }
}
