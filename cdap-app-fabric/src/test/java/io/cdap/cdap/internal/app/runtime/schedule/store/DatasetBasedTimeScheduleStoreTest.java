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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.primitives.Bytes;
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
import io.cdap.cdap.common.service.Services;
import io.cdap.cdap.data.runtime.DataFabricModules;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.data2.metadata.writer.NoOpMetadataServiceClient;
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
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import io.cdap.cdap.test.SlowTests;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.lang.SerializationUtils;
import org.apache.tephra.TransactionManager;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.quartz.simpl.RAMJobStore;
import org.quartz.simpl.SimpleThreadPool;
import org.quartz.spi.JobStore;

/**
 * Tests {@link DatasetBasedTimeScheduleStore} across scheduler restarts to verify we retain scheduler information
 * across restarts.
 */
@Category(SlowTests.class)
public class DatasetBasedTimeScheduleStoreTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final String DUMMY_SCHEDULER_NAME = "dummyScheduler";
  private static final String LEGACY_TIME_ZONE = "Asia/Kolkata";

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
    Services.startAndWait(txService);
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class));
    dsOpsService = injector.getInstance(DatasetOpExecutorService.class);
    Services.startAndWait(dsOpsService);
    dsService = injector.getInstance(DatasetService.class);
    Services.startAndWait(dsService);
    transactionRunner = injector.getInstance(TransactionRunner.class);
  }

  @AfterClass
  public static void afterClass() {
    Services.stopAndWait(dsService);
    Services.stopAndWait(dsOpsService);
    Services.stopAndWait(txService);
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
    String identity = Strings.isNullOrEmpty(appVersion)
        ? String.format("developer:application1:flow:%s", jobName) :
        String.format("developer:application1:%s:flow:%s", appVersion, jobName);

    return JobBuilder.newJob(LogPrintingJob.class)
        .withIdentity(identity, jobGroup)
        .storeDurably()
        .build();
  }

  @Test
  public void testRepairLegacyCronTrigger() throws Exception {
    TimeZone timeZone = TimeZone.getTimeZone(LEGACY_TIME_ZONE);

    // A trigger persisted under Quartz 2.2 with a legacy oversized increment gets repaired.
    CronTriggerImpl trigger = deserializeAsLegacyTrigger("0 0/50 * * * ?", "0 0/60 * * * ?");
    Assert.assertEquals("0 0/60 * * * ?", trigger.getCronExpression());
    Assert.assertTrue(DatasetBasedTimeScheduleStore.repairLegacyCronTrigger(trigger));
    Assert.assertEquals("0 0 * * * ?", trigger.getCronExpression());
    Assert.assertEquals(timeZone, trigger.getTimeZone());
    Assert.assertNotNull(trigger.clone());
    Assert.assertNotNull(trigger.getFireTimeAfter(new Date(0L)));

    // A trigger that cannot be repaired is reported and left unchanged.
    trigger = deserializeAsLegacyTrigger("0 0/5,30 * * * ?", "0 0/60/1 * * * ?");
    Assert.assertFalse(DatasetBasedTimeScheduleStore.repairLegacyCronTrigger(trigger));
    Assert.assertEquals("0 0/60/1 * * * ?", trigger.getCronExpression());

    // A range with an oversized step is valid in Quartz 2.3+ and must stay untouched.
    trigger = deserializeAsLegacyTrigger("0 10-50/50 * * * ?", "0 10-50/60 * * * ?");
    Assert.assertTrue(DatasetBasedTimeScheduleStore.repairLegacyCronTrigger(trigger));
    Assert.assertEquals("0 10-50/60 * * * ?", trigger.getCronExpression());
    Assert.assertNotNull(trigger.clone());

    // A valid trigger stays untouched.
    trigger = deserializeAsLegacyTrigger("0 0/50 * * * ?", "0 0/50 * * * ?");
    Assert.assertTrue(DatasetBasedTimeScheduleStore.repairLegacyCronTrigger(trigger));
    Assert.assertEquals("0 0/50 * * * ?", trigger.getCronExpression());
  }

  @Test
  public void testLegacyCronTriggersAcrossRestarts() throws Exception {
    final TimeZone timeZone = TimeZone.getTimeZone(LEGACY_TIME_ZONE);
    final String legacyCron = "0 0/60 * * * ?";
    final String repairedCron = "0 0 * * * ?";

    schedulerSetup(true);
    JobDetail job = getJobDetail("legacyCronJob");
    Trigger normalTrigger = getCronTrigger("legacyNormal", "0 0/50 * * * ?", timeZone);
    Trigger pausedTrigger = getCronTrigger("legacyPaused", "0 0/50 * * * ?", timeZone);
    Trigger brokenTrigger = getCronTrigger("legacyBroken", "0 0/5,30 * * * ?", timeZone);
    Trigger healthyTrigger = getCronTrigger("legacyHealthy", "0 0/5 * * * ?", timeZone);
    try {
      scheduler.scheduleJob(
          job, Sets.newHashSet(normalTrigger, pausedTrigger, brokenTrigger, healthyTrigger), true);
      scheduler.pauseTrigger(pausedTrigger.getKey());

      // Rewrite the persisted triggers to what Quartz 2.2 would have stored.
      rewritePersistedCron(normalTrigger.getKey(), "0 0/50 * * * ?", legacyCron);
      rewritePersistedCron(pausedTrigger.getKey(), "0 0/50 * * * ?", legacyCron);
      rewritePersistedCron(brokenTrigger.getKey(), "0 0/5,30 * * * ?", "0 0/60/1 * * * ?");

      // The restart must succeed, repair the legacy triggers, and skip only the unrepairable one.
      schedulerTearDown();
      schedulerSetup(true);

      verifyCronTrigger(normalTrigger.getKey(), repairedCron, timeZone,
                        Trigger.TriggerState.NORMAL);
      verifyCronTrigger(pausedTrigger.getKey(), repairedCron, timeZone,
                        Trigger.TriggerState.PAUSED);
      verifyCronTrigger(healthyTrigger.getKey(), "0 0/5 * * * ?", timeZone,
                        Trigger.TriggerState.NORMAL);
      Assert.assertNull(scheduler.getTrigger(brokenTrigger.getKey()));
      // The unrepairable trigger is kept in the store.
      Assert.assertTrue(persistedTriggerContains(brokenTrigger.getKey(), "0 0/60/1 * * * ?"));

      // The repaired trigger fires at the start of every hour in its own time zone.
      Date fireTime = scheduler.getTrigger(normalTrigger.getKey()).getFireTimeAfter(new Date());
      Assert.assertNotNull(fireTime);
      Calendar calendar = Calendar.getInstance(timeZone);
      calendar.setTime(fireTime);
      Assert.assertEquals(0, calendar.get(Calendar.MINUTE));
      Assert.assertEquals(0, calendar.get(Calendar.SECOND));

      // The restart only repairs the triggers in memory.
      Assert.assertTrue(persistedTriggerContains(normalTrigger.getKey(), legacyCron));
      Assert.assertTrue(persistedTriggerContains(pausedTrigger.getKey(), legacyCron));

      // Pause and resume read the trigger back from the store and persist the repaired trigger.
      scheduler.pauseTrigger(normalTrigger.getKey());
      Assert.assertFalse(persistedTriggerContains(normalTrigger.getKey(), legacyCron));
      Assert.assertTrue(persistedTriggerContains(normalTrigger.getKey(), repairedCron));
      scheduler.resumeTrigger(pausedTrigger.getKey());
      Assert.assertFalse(persistedTriggerContains(pausedTrigger.getKey(), legacyCron));
      Assert.assertTrue(persistedTriggerContains(pausedTrigger.getKey(), repairedCron));

      // The changed states survive another restart.
      schedulerTearDown();
      schedulerSetup(true);
      verifyCronTrigger(normalTrigger.getKey(), repairedCron, timeZone,
                        Trigger.TriggerState.PAUSED);
      verifyCronTrigger(pausedTrigger.getKey(), repairedCron, timeZone,
                        Trigger.TriggerState.NORMAL);
      Assert.assertNull(scheduler.getTrigger(brokenTrigger.getKey()));
    } finally {
      scheduler.deleteJob(job.getKey());
      // The unrepairable trigger is not loaded by the scheduler, so remove it from the store.
      datasetBasedTimeScheduleStore.removeTrigger(brokenTrigger.getKey());
      schedulerTearDown();
    }
  }

  private Trigger getCronTrigger(String triggerName, String cron, TimeZone timeZone) {
    return TriggerBuilder.newTrigger()
      .withIdentity(triggerName)
      .startNow()
      .withSchedule(CronScheduleBuilder.cronSchedule(cron).inTimeZone(timeZone))
      .build();
  }

  private void verifyCronTrigger(TriggerKey triggerKey, String expectedCron,
                                 TimeZone expectedTimeZone,
                                 Trigger.TriggerState expectedState) throws SchedulerException {
    Trigger trigger = scheduler.getTrigger(triggerKey);
    Assert.assertTrue(trigger instanceof CronTrigger);
    Assert.assertEquals(expectedCron, ((CronTrigger) trigger).getCronExpression());
    Assert.assertEquals(expectedTimeZone, ((CronTrigger) trigger).getTimeZone());
    Assert.assertEquals(expectedState, scheduler.getTriggerState(triggerKey));
  }

  /**
   * Replaces the cron expression in serialized bytes. Quartz serializes only the cron string of a
   * cron expression and parses it again on deserialization, so this produces the same bytes as a
   * trigger that an older Quartz version stored with {@code newCron}.
   */
  private static byte[] replaceCron(byte[] serialized, String currentCron, String newCron) {
    byte[] target = currentCron.getBytes(StandardCharsets.UTF_8);
    byte[] replacement = newCron.getBytes(StandardCharsets.UTF_8);
    // The length of the string is part of the serialized form.
    Assert.assertEquals(target.length, replacement.length);
    int index = Bytes.indexOf(serialized, target);
    Assert.assertTrue(index >= 0);
    byte[] result = serialized.clone();
    System.arraycopy(replacement, 0, result, index, replacement.length);
    return result;
  }

  private CronTriggerImpl deserializeAsLegacyTrigger(String validCron,
                                                     String legacyCron) throws Exception {
    CronTriggerImpl trigger = new CronTriggerImpl();
    trigger.setName("legacy-trigger");
    trigger.setGroup("default");
    trigger.setCronExpression(new CronExpression(validCron));
    trigger.setTimeZone(TimeZone.getTimeZone(LEGACY_TIME_ZONE));
    byte[] serialized = replaceCron(SerializationUtils.serialize(trigger), validCron, legacyCron);
    return (CronTriggerImpl) SerializationUtils.deserialize(serialized);
  }

  private static List<Field<?>> getTriggerPrimaryKeys(TriggerKey triggerKey) {
    return ImmutableList.of(
      Fields.stringField(StoreDefinition.TimeScheduleStore.TYPE_FIELD,
                         DatasetBasedTimeScheduleStore.TRIGGER_KEY),
      Fields.stringField(StoreDefinition.TimeScheduleStore.NAME_FIELD, triggerKey.getName()));
  }

  private static byte[] readPersistedTrigger(StructuredTable table,
                                             TriggerKey triggerKey) throws Exception {
    return table.read(getTriggerPrimaryKeys(triggerKey))
      .orElseThrow(() -> new AssertionError("No persisted trigger " + triggerKey))
      .getBytes(StoreDefinition.TimeScheduleStore.VALUE_FIELD);
  }

  private void rewritePersistedCron(TriggerKey triggerKey, String currentCron, String newCron) {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = DatasetBasedTimeScheduleStore.getTimeScheduleStructuredTable(context);
      byte[] value = replaceCron(readPersistedTrigger(table, triggerKey), currentCron, newCron);
      table.upsert(ImmutableList.<Field<?>>builder()
                     .addAll(getTriggerPrimaryKeys(triggerKey))
                     .add(Fields.bytesField(StoreDefinition.TimeScheduleStore.VALUE_FIELD, value))
                     .build());
    });
  }

  private boolean persistedTriggerContains(TriggerKey triggerKey, String cron) {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = DatasetBasedTimeScheduleStore.getTimeScheduleStructuredTable(context);
      return Bytes.indexOf(readPersistedTrigger(table, triggerKey),
                           cron.getBytes(StandardCharsets.UTF_8)) >= 0;
    });
  }

  @AfterClass
  public static void cleanup() throws SchedulerException {
    schedulerTearDown();
  }
}
