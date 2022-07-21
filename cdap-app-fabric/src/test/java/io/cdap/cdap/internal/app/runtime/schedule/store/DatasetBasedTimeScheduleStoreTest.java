/*
 * Copyright © 2014-2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.schedule.store;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.NamespaceAdminTestModule;
import io.cdap.cdap.common.guice.NonCustomLocationUnitTestModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.data.runtime.DataFabricModules;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.data2.metadata.writer.NoOpMetadataServiceClient;
import io.cdap.cdap.explore.guice.ExploreClientModule;
import io.cdap.cdap.internal.app.scheduler.LogPrintingJob;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizationTestModule;
import io.cdap.cdap.security.impersonation.NoOpOwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.impersonation.UnsupportedUGIProvider;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import io.cdap.cdap.test.SlowTests;
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
  private static TransactionManager txService;
  private static DatasetOpExecutorService dsOpsService;
  private static DatasetService dsService;
  private static DatasetBasedTimeScheduleStore datasetBasedTimeScheduleStore;
  private static TransactionRunner transactionRunner;

  @BeforeClass
  public static void beforeClass() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder("data").getAbsolutePath());
    injector = Guice.createInjector(new ConfigModule(conf),
                                    RemoteAuthenticatorModules.getNoOpModule(),
                                    new NonCustomLocationUnitTestModule(),
                                    new InMemoryDiscoveryModule(),
                                    new MetricsClientRuntimeModule().getInMemoryModules(),
                                    new DataFabricModules().getInMemoryModules(),
                                    new DataSetsModules().getStandaloneModules(),
                                    new DataSetServiceModules().getInMemoryModules(),
                                    new ExploreClientModule(),
                                    new NamespaceAdminTestModule(),
                                    new AuthorizationTestModule(),
                                    new AuthorizationEnforcementModule().getInMemoryModules(),
                                    new AuthenticationContextModules().getMasterModule(),
                                    new AbstractModule() {
                                      @Override
                                      protected void configure() {
                                        bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
                                        bind(OwnerAdmin.class).to(NoOpOwnerAdmin.class);
                                        bind(MetadataServiceClient.class).to(NoOpMetadataServiceClient.class);
                                      }
                                    });
    txService = injector.getInstance(TransactionManager.class);
    txService.startAndWait();
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class));
    dsOpsService = injector.getInstance(DatasetOpExecutorService.class);
    dsOpsService.startAndWait();
    dsService = injector.getInstance(DatasetService.class);
    dsService.startAndWait();
    transactionRunner = injector.getInstance(TransactionRunner.class);
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
      datasetBasedTimeScheduleStore = new DatasetBasedTimeScheduleStore(transactionRunner, conf);
      js = datasetBasedTimeScheduleStore;
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

    scheduler.addJob(jobDetail, true);
    schedulerTearDown();

    schedulerSetup(true);
    Set<JobKey> keys = scheduler.getJobKeys(GroupMatcher.jobGroupEquals(jobGroup));
    if (version.equals(ApplicationId.DEFAULT_VERSION)) {
      Assert.assertEquals(1, keys.size());
      Assert.assertTrue(scheduler.deleteJob(jobDetail.getKey()));
    } else {
      Assert.assertEquals(1, keys.size());
      Assert.assertTrue(scheduler.deleteJob(jobDetail.getKey()));
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
  public void testSchedulerWithoutPersistence() throws SchedulerException {
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
  public void testSchedulerWithPersistenceAcrossRestarts() throws SchedulerException {
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
  public void testPausedTriggersAcrossRestarts() throws SchedulerException {
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

  @Test
  public void testTriggersWithoutJobs() throws Exception {
    schedulerSetup(true);

    // Schedule 2 Jobs each with 1 Trigger
    final JobDetail job1 = getJobDetail("MR1");
    final Trigger job1Trigger = getTrigger("MR1Trigger");
    scheduler.scheduleJob(job1, job1Trigger);
    JobDetail job2 = getJobDetail("MR2");
    Trigger job2Trigger = getTrigger("MR2Trigger");
    scheduler.scheduleJob(job2, job2Trigger);

    // Make sure Jobs and Triggers are setup correctly
    verifyJobAndTriggers(job1.getKey(), 1, Trigger.TriggerState.NORMAL);
    verifyJobAndTriggers(job2.getKey(), 1, Trigger.TriggerState.NORMAL);

    // Delete the row corresponding to the Job MR1 to create inconsistency
    datasetBasedTimeScheduleStore.removeJob(job1.getKey());

    // Restart the scheduler it should not throw exception
    schedulerTearDown();
    schedulerSetup(true);

    // Make sure that Job2 still has valid trigger associated with it
    verifyJobAndTriggers(job2.getKey(), 1, Trigger.TriggerState.NORMAL);

    // Since Job is deleted from store, JobDetails should be null
    JobDetail jobDetail = scheduler.getJobDetail(job1.getKey());
    Assert.assertNull(jobDetail);

    // Since we do not add trigger now if the Job is not present, getting Trigger from scheduler should return null
    Trigger trigger = scheduler.getTrigger(job1Trigger.getKey());
    Assert.assertNull(trigger);

    // Make sure we actually deleted the entry from the persistent store as well for the Trigger
    TransactionRunners.run(transactionRunner, context -> {
      DatasetBasedTimeScheduleStore.TriggerStatusV2 triggerStatusV2
        = datasetBasedTimeScheduleStore.readTrigger(
          DatasetBasedTimeScheduleStore.getTimeScheduleStructuredTable(context), job1Trigger.getKey());
      Assert.assertNull(triggerStatusV2);
    });
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

  private JobKey scheduleJobWithTrigger(boolean enablePersistence) throws SchedulerException {
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
  public static void cleanup() throws SchedulerException {
    schedulerTearDown();
  }
}
