/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule.store;

import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.common.kerberos.NoOpOwnerAdmin;
import co.cask.cdap.common.kerberos.OwnerAdmin;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.internal.app.scheduler.LogPrintingJob;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.security.impersonation.UGIProvider;
import co.cask.cdap.security.impersonation.UnsupportedUGIProvider;
import co.cask.cdap.test.SlowTests;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.tephra.TransactionExecutorFactory;
import org.apache.tephra.TransactionManager;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.simpl.RAMJobStore;
import org.quartz.simpl.SimpleThreadPool;
import org.quartz.spi.JobStore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Tests {@link DatasetBasedTimeScheduleStore} across scheduler restarts to verify we retain scheduler information
 * across restarts.
 */
@Category(SlowTests.class)
public class DatasetBasedTimeScheduleStoreTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final String DUMMY_SCHEDULER_NAME = "dummyScheduler";

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
                                    new NonCustomLocationUnitTestModule().getModule(),
                                    new DiscoveryRuntimeModule().getInMemoryModules(),
                                    new MetricsClientRuntimeModule().getInMemoryModules(),
                                    new DataFabricModules().getInMemoryModules(),
                                    new DataSetsModules().getStandaloneModules(),
                                    new DataSetServiceModules().getInMemoryModules(),
                                    new ExploreClientModule(),
                                    new NamespaceClientRuntimeModule().getInMemoryModules(),
                                    new AuthorizationTestModule(),
                                    new AuthorizationEnforcementModule().getInMemoryModules(),
                                    new AuthenticationContextModules().getMasterModule(),
                                    new AbstractModule() {
                                      @Override
                                      protected void configure() {
                                        bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
                                        bind(OwnerAdmin.class).to(NoOpOwnerAdmin.class);
                                      }
                                    });
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

  private static void schedulerSetup(boolean enablePersistence) throws SchedulerException {
    JobStore js;
    if (enablePersistence) {
      CConfiguration conf = injector.getInstance(CConfiguration.class);
      js = new DatasetBasedTimeScheduleStore(factory, new ScheduleStoreTableUtil(dsFramework, conf), conf);
    } else {
      js = new RAMJobStore();
    }

    SimpleThreadPool threadPool = new SimpleThreadPool(10, Thread.NORM_PRIORITY);
    threadPool.initialize();
    DirectSchedulerFactory.getInstance().createScheduler(DUMMY_SCHEDULER_NAME, "1", threadPool, js);

    scheduler = DirectSchedulerFactory.getInstance().getScheduler(DUMMY_SCHEDULER_NAME);
    scheduler.start();
  }

  public static void schedulerTearDown() throws SchedulerException {
    scheduler.shutdown();
  }

  @Test
  public void testOldJobKeyFormatCompatibility() throws Exception {
    testJobKeyDeletion("v1");
    testJobKeyDeletion(ApplicationId.DEFAULT_VERSION);
  }

  private void testJobKeyDeletion(String version) throws Exception {
    schedulerSetup(true);
    String jobGroup = "jg";
    JobDetail jobDetail = getJobDetail(jobGroup, "job1", version);
    JobDetail versionLessJobDetail = getJobDetail(jobGroup, "job1", null);

    scheduler.addJob(versionLessJobDetail, true);
    scheduler.addJob(jobDetail, true);
    schedulerTearDown();

    schedulerSetup(true);
    Set<JobKey> keys = scheduler.getJobKeys(GroupMatcher.jobGroupEquals(jobGroup));
    if (version.equals(ApplicationId.DEFAULT_VERSION)) {
      Assert.assertEquals(1, keys.size());
      Assert.assertTrue(scheduler.deleteJob(jobDetail.getKey()));
    } else {
      Assert.assertEquals(2, keys.size());
      Assert.assertTrue(scheduler.deleteJob(jobDetail.getKey()));
      Assert.assertTrue(scheduler.deleteJob(versionLessJobDetail.getKey()));
    }
    schedulerTearDown();
    schedulerSetup(true);
    Set<JobKey> jobKeys = scheduler.getJobKeys(GroupMatcher.jobGroupEquals(jobGroup));
    Assert.assertEquals(0, jobKeys.size());
    schedulerTearDown();
  }

  @Test
  public void testSimpleJobDeletion() throws Exception {
    schedulerSetup(true);
    Trigger trigger = TriggerBuilder.newTrigger()
      .withIdentity("g2")
      .usingJobData(LogPrintingJob.KEY, LogPrintingJob.VALUE)
      .startNow()
      .withSchedule(CronScheduleBuilder.cronSchedule("0/1 * * * * ?"))
      .build();

    int oldCount = scheduler.getJobKeys(GroupMatcher.anyJobGroup()).size();

    JobDetail jobDetail = getJobDetail("mr1");
    scheduler.scheduleJob(jobDetail, trigger);
    Assert.assertTrue(scheduler.unscheduleJob(trigger.getKey()));
    Assert.assertTrue(scheduler.deleteJob(jobDetail.getKey()));

    int newCount = scheduler.getJobKeys(GroupMatcher.anyJobGroup()).size();
    Assert.assertEquals(oldCount, newCount);
    schedulerTearDown();
    schedulerSetup(true);

    newCount = scheduler.getJobKeys(GroupMatcher.anyJobGroup()).size();
    Assert.assertEquals(oldCount, newCount);
    schedulerTearDown();
  }

  @Test
  public void testJobProperties() throws SchedulerException, UnsupportedTypeException, InterruptedException {
    schedulerSetup(true);
    JobDetail jobDetail = getJobDetail("mapreduce1");

    Trigger trigger = TriggerBuilder.newTrigger()
      .withIdentity("g2")
      .usingJobData(LogPrintingJob.KEY, LogPrintingJob.VALUE)
      .startNow()
      .withSchedule(CronScheduleBuilder.cronSchedule("0/1 * * * * ?"))
      .build();

    //Schedule job
    scheduler.scheduleJob(jobDetail, trigger);
    //Make sure that the job gets triggered more than once.
    TimeUnit.SECONDS.sleep(3);
    scheduler.deleteJob(jobDetail.getKey());
    schedulerTearDown();
  }

  @Test
  public void testSchedulerWithoutPersistence() throws SchedulerException, UnsupportedTypeException {
    JobKey jobKey = scheduleJobWithTrigger(false);

    verifyJobAndTriggers(jobKey, 1, Trigger.TriggerState.NORMAL);

    //Shutdown scheduler.
    schedulerTearDown();
    //restart scheduler.
    schedulerSetup(false);

    //read the job
    JobDetail jobStored = scheduler.getJobDetail(jobKey);
    // The job with old job key should not exist since it is not persisted.
    Assert.assertNull(jobStored);
    schedulerTearDown();
  }

  @Test
  public void testSchedulerWithPersistenceAcrossRestarts() throws SchedulerException, UnsupportedTypeException {
    JobKey jobKey = scheduleJobWithTrigger(true);

    verifyJobAndTriggers(jobKey, 1, Trigger.TriggerState.NORMAL);

    //Shutdown scheduler.
    schedulerTearDown();
    //restart scheduler.
    schedulerSetup(true);

    // The job with old job key should exist since it is persisted.
    verifyJobAndTriggers(jobKey, 1, Trigger.TriggerState.NORMAL);
    schedulerTearDown();
  }

  @Test
  public void testPausedTriggersAcrossRestarts() throws SchedulerException, UnsupportedTypeException {
    JobKey jobKey = scheduleJobWithTrigger(true);

    List<? extends Trigger> triggers = scheduler.getTriggersOfJob(jobKey);

    // pause triggers for the job
    for (Trigger trigger : triggers) {
      scheduler.pauseTrigger(trigger.getKey());
    }

    //Shutdown scheduler.
    schedulerTearDown();
    //restart scheduler.
    schedulerSetup(true);

    verifyJobAndTriggers(jobKey, 1, Trigger.TriggerState.PAUSED);

    // remove job and check if the associated trigger gets removed too
    Assert.assertTrue("Failed to delete the job", scheduler.deleteJob(jobKey));
    Assert.assertFalse("Trigger for the deleted job still exists", scheduler.checkExists(triggers.get(0).getKey()));
    // check for trigger to not exist in the datastore too from which scheduler will get initialized across restart
    //Shutdown scheduler.
    schedulerTearDown();
    //restart scheduler.
    schedulerSetup(true);
    Assert.assertFalse("Trigger for the deleted job still exists", scheduler.checkExists(triggers.get(0).getKey()));
    schedulerTearDown();
  }

  @Test
  public void testStoreJobsAndTriggers() throws SchedulerException {
    schedulerSetup(true);
    Map<JobDetail, Set<? extends Trigger>> multiJobsTriggers = new HashMap<>();

    JobDetail job1 = getJobDetail("mapreduce1");
    multiJobsTriggers.put(job1, Sets.newHashSet(getTrigger("p1"), getTrigger("p2")));

    JobDetail job2 = getJobDetail("mapreduce2");
    multiJobsTriggers.put(job2, Sets.newHashSet(getTrigger("p3")));

    scheduler.scheduleJobs(multiJobsTriggers, true);

    verifyJobAndTriggers(job1.getKey(), 2, Trigger.TriggerState.NORMAL);
    verifyJobAndTriggers(job2.getKey(), 1, Trigger.TriggerState.NORMAL);

    // verify across restart that the jobs and triggers gets retrieved from the store
    //Shutdown scheduler.
    schedulerTearDown();
    //restart scheduler.
    schedulerSetup(true);
    verifyJobAndTriggers(job1.getKey(), 2, Trigger.TriggerState.NORMAL);
    verifyJobAndTriggers(job2.getKey(), 1, Trigger.TriggerState.NORMAL);

    schedulerTearDown();
  }

  private void verifyJobAndTriggers(JobKey jobKey, int expectedTriggersSize,
                                    Trigger.TriggerState expectedTriggerState) throws SchedulerException {
    JobDetail jobStored = scheduler.getJobDetail(jobKey);
    List<? extends Trigger> triggers = scheduler.getTriggersOfJob(jobKey);
    Assert.assertEquals(jobStored.getKey().getName(), jobKey.getName());
    Assert.assertEquals(expectedTriggersSize, triggers.size());
    verifyTriggerState(triggers, expectedTriggerState);
  }

  private void verifyTriggerState(List<? extends Trigger> triggers,
                                  Trigger.TriggerState expectedTriggerState) throws SchedulerException {
    for (Trigger trigger : triggers) {
      Assert.assertEquals(expectedTriggerState, scheduler.getTriggerState(trigger.getKey()));
    }
  }

  private JobKey scheduleJobWithTrigger(boolean enablePersistence) throws UnsupportedTypeException, SchedulerException {
    //start scheduler with given persistence setting
    schedulerSetup(enablePersistence);
    JobDetail jobDetail = getJobDetail("mapreduce1");

    Trigger trigger = getTrigger("p1");

    //Schedule job
    scheduler.scheduleJob(jobDetail, trigger);
    return jobDetail.getKey();
  }

  private Trigger getTrigger(String triggerName) {
    return TriggerBuilder.newTrigger()
      .withIdentity(triggerName)
      .startNow()
      .withSchedule(CronScheduleBuilder.cronSchedule("0 0/5 * * * ?"))
      .build();
  }

  private JobDetail getJobDetail(String jobName) {
    return JobBuilder.newJob(LogPrintingJob.class)
      .withIdentity(String.format("developer:application1:%s", jobName))
      .build();
  }

  private JobDetail getJobDetail(String jobGroup, String jobName, @Nullable String appVersion) {
    String identity = Strings.isNullOrEmpty(appVersion) ?
      String.format("developer:application1:flow:%s", jobName) :
      String.format("developer:application1:%s:flow:%s", appVersion, jobName);

    return JobBuilder.newJob(LogPrintingJob.class)
      .withIdentity(identity, jobGroup)
      .storeDurably()
      .build();
  }

  @AfterClass
  public static void cleanup() throws SchedulerException, InterruptedException {
    schedulerTearDown();
    Thread.sleep(10000);
  }
}
