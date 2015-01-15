/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.scheduler;

import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.gateway.auth.AuthModule;
import co.cask.cdap.internal.app.runtime.schedule.DataSetBasedScheduleStore;
import co.cask.cdap.internal.app.runtime.schedule.ScheduleStoreTableUtil;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.internal.TempFolder;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionManager;
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

  private static final TempFolder TEMP_FOLDER = new TempFolder();

  private static Injector injector;
  private static Scheduler scheduler;
  private static TransactionExecutorFactory factory;
  private static DatasetFramework dsFramework;
  private static TransactionManager txService;
  private static DatasetOpExecutor dsOpsService;
  private static DatasetService dsService;

  @BeforeClass
  public static void beforeClass() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder("data").getAbsolutePath());
    injector = Guice.createInjector(new ConfigModule(conf),
                                    new LocationRuntimeModule().getInMemoryModules(),
                                    new DiscoveryRuntimeModule().getInMemoryModules(),
                                    new MetricsClientRuntimeModule().getInMemoryModules(),
                                    new DataFabricModules().getInMemoryModules(),
                                    new DataSetsModules().getLocalModule(),
                                    new DataSetServiceModules().getInMemoryModule(),
                                    new AuthModule(),
                                    new ExploreClientModule());
    txService = injector.getInstance(TransactionManager.class);
    txService.startAndWait();
    dsOpsService = injector.getInstance(DatasetOpExecutor.class);
    dsOpsService.startAndWait();
    dsService = injector.getInstance(DatasetService.class);
    dsService.startAndWait();
    dsFramework = injector.getInstance(DatasetFramework.class);
    factory = injector.getInstance(TransactionExecutorFactory.class);
  }

  @AfterClass
  public static void afterClass() {
    dsService.stopAndWait();
    dsOpsService.stopAndWait();
    txService.stopAndWait();
  }

  public static void schedulerSetup(boolean enablePersistence, String schedulerName)
    throws SchedulerException, UnsupportedTypeException {
    JobStore js;
    if (enablePersistence) {
      CConfiguration conf = injector.getInstance(CConfiguration.class);
      js = new DataSetBasedScheduleStore(factory, new ScheduleStoreTableUtil(dsFramework, conf));
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
