/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.schedule;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.schedule.SchedulableProgramType;
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.schedule.store.Schedulers;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.AbstractSatisfiableCompositeTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.SatisfiableTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.proto.ProtoTrigger;
import io.cdap.cdap.proto.ScheduledRuntime;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.TopicId;
import org.apache.twill.common.Threads;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;
import org.quartz.utils.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Class that wraps Quartz scheduler. Needed to delegate start stop operations to classes that extend
 * DefaultSchedulerService.
 */
public final class TimeScheduler {
  private static final Logger LOG = LoggerFactory.getLogger(TimeScheduler.class);
  private static final String PAUSED_NEW_TRIGGERS_GROUP = "NewPausedTriggers";

  private org.quartz.Scheduler scheduler;
  private final Supplier<org.quartz.Scheduler> schedulerSupplier;
  private final MessagingService messagingService;
  private ListeningExecutorService taskExecutorService;
  private boolean schedulerStarted;
  private final TopicId topicId;
  private MetricsCollectionService metricsCollectionService;

  @Inject
  TimeScheduler(Supplier<org.quartz.Scheduler> schedulerSupplier, MessagingService messagingService,
                CConfiguration cConf, MetricsCollectionService metricsCollectionService) {
    this.schedulerSupplier = schedulerSupplier;
    this.messagingService = messagingService;
    this.scheduler = null;
    this.schedulerStarted = false;
    this.topicId = NamespaceId.SYSTEM.topic(cConf.get(Constants.Scheduler.TIME_EVENT_TOPIC));
    this.metricsCollectionService = metricsCollectionService;
  }

  void init() throws SchedulerException {
    try {
      taskExecutorService = MoreExecutors.listeningDecorator(
        Executors.newCachedThreadPool(Threads.createDaemonThreadFactory("time-schedule-task")));
      scheduler = schedulerSupplier.get();
      scheduler.setJobFactory(createJobFactory());
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }
  }

  /**
   * Creates a paused group TimeScheduler#PAUSED_NEW_TRIGGERS_GROUP by adding a dummy job to it if it does not exist
   * already. This is needed so that we can add new triggers to this paused group and they will be paused too.
   *
   * @throws org.quartz.SchedulerException
   */
  private void initNewPausedTriggersGroup() throws org.quartz.SchedulerException {
    // if the dummy job does not already exists in the TimeScheduler#PAUSED_NEW_TRIGGERS_GROUP
    // then create a dummy job which will create the TimeScheduler#PAUSED_NEW_TRIGGERS_GROUP
    if (!scheduler.checkExists(new JobKey(EmptyJob.class.getSimpleName(), PAUSED_NEW_TRIGGERS_GROUP))) {
      JobDetail job = JobBuilder.newJob(EmptyJob.class)
        .withIdentity(EmptyJob.class.getSimpleName(), PAUSED_NEW_TRIGGERS_GROUP)
        .storeDurably(true)
        .build();
      scheduler.addJob(job, true);
    }
    // call pause on this group this ensures that all the new triggers added to this group will also be paused
    scheduler.pauseTriggers(GroupMatcher.triggerGroupEquals(PAUSED_NEW_TRIGGERS_GROUP));
  }

  void start() throws SchedulerException {
    try {
      scheduler.start();
      schedulerStarted = true;
      initNewPausedTriggersGroup();
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }
  }

  boolean isStarted() {
    return schedulerStarted;
  }

  void stop() throws SchedulerException {
    try {
      if (scheduler != null) {
        scheduler.shutdown();
      }
      if (taskExecutorService != null) {
        taskExecutorService.shutdownNow();
      }
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }
  }

  public void addProgramSchedule(ProgramSchedule schedule) throws AlreadyExistsException, SchedulerException {
    // Verify every trigger does not exist first before adding any of them to Quartz scheduler
    try {
      Map<String, TriggerKey> cronTriggerKeyMap = getCronTriggerKeyMap(schedule);
      for (TriggerKey triggerKey : cronTriggerKeyMap.values()) {
        assertTriggerDoesNotExist(triggerKey);
      }
      ProgramId program = schedule.getProgramId();
      SchedulableProgramType programType = program.getType().getSchedulableType();
      JobDetail job = addJob(program, programType);
      for (Map.Entry<String, TriggerKey> entry : cronTriggerKeyMap.entrySet()) {
        scheduleJob(entry.getValue(), schedule.getName(), entry.getKey(), job);
      }
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }
  }

  public void deleteProgramSchedule(ProgramSchedule schedule) throws SchedulerException {
    try {
      Collection<TriggerKey> triggerKeys = getGroupedTriggerKeys(schedule);
      for (TriggerKey triggerKey : triggerKeys) {
        if (!scheduler.checkExists(triggerKey)) {
          continue;
        }
        try {
          Trigger trigger = getTrigger(triggerKey, schedule.getProgramId(), schedule.getName());
          scheduler.unscheduleJob(trigger.getKey());

          JobKey jobKey = trigger.getJobKey();
          if (scheduler.getTriggersOfJob(jobKey).isEmpty()) {
            scheduler.deleteJob(jobKey);
          }
        } catch (NotFoundException e) {
          // no-op
          // ok if it doesn't exist since we're trying to delete it anyway
        }
      }
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }
  }

  public void suspendProgramSchedule(ProgramSchedule schedule) throws NotFoundException, SchedulerException {
    try {
      Collection<TriggerKey> triggerKeys = getGroupedTriggerKeys(schedule);
      // Must assert all trigger keys exist before processing each trigger key
      assertTriggerKeysExist(triggerKeys);
      for (TriggerKey triggerKey : triggerKeys) {
        scheduler.pauseTrigger(triggerKey);
      }
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }
  }

  public void resumeProgramSchedule(ProgramSchedule schedule) throws NotFoundException, SchedulerException {
    try {
      Collection<TriggerKey> triggerKeys = getGroupedTriggerKeys(schedule);
      // Must assert all trigger keys exist before processing each trigger key
      assertTriggerKeysExist(triggerKeys);
      for (TriggerKey triggerKey : triggerKeys) {
        if (triggerKey.getGroup().equals(PAUSED_NEW_TRIGGERS_GROUP)) {
          Trigger neverScheduledTrigger = scheduler.getTrigger(triggerKey);
          TriggerBuilder<? extends Trigger> triggerBuilder = neverScheduledTrigger.getTriggerBuilder();
          // move this key from TimeScheduler#PAUSED_NEW_TRIGGERS_GROUP to the Key#DEFAULT_GROUP group
          // (when no group name is provided default is used)
          Trigger resumedTrigger = triggerBuilder.withIdentity(triggerKey.getName()).build();
          scheduler.rescheduleJob(neverScheduledTrigger.getKey(), resumedTrigger);
          triggerKey = resumedTrigger.getKey();
        }
        scheduler.resumeTrigger(triggerKey);
      }
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }
  }

  /**
   * Assert the schedule with a given name and of a given program does not exist
   *
   * @throws ObjectAlreadyExistsException if the corresponding schedule already exists
   */
  private void assertTriggerDoesNotExist(TriggerKey triggerKey) throws org.quartz.SchedulerException {
    // Once the schedule is resumed we move the trigger from TimeScheduler#PAUSED_NEW_TRIGGERS_GROUP to
    // Key#DEFAULT_GROUP so before adding check if this schedule does not exist.
    // We do not need to check for same schedule in the current list as its already checked in app configuration stage
    if (scheduler.checkExists(triggerKey)) {
      throw new ObjectAlreadyExistsException("Unable to store Trigger with name " + triggerKey.getName() +
                                               "because one already exists with this identification.");
    }
  }

  /**
   * Asserts all the given trigger keys exist
   */
  private void assertTriggerKeysExist(Collection<TriggerKey> triggerKeys)
    throws SchedulerException, org.quartz.SchedulerException {
    for (TriggerKey triggerKey : triggerKeys) {
      if (!scheduler.checkExists(triggerKey)) {
        throw new SchedulerException("Trigger with name '" + triggerKey.getName() + "' does not exist");
      }
    }
  }

  /**
   * Construct a {@link JobDetail} from the given {@link ProgramId} and {@link SchedulableProgramType}.
   * Add the {@link JobDetail} the scheduler and return it
   */
  private JobDetail addJob(ProgramId program, SchedulableProgramType programType) throws SchedulerException {
    String jobKey = jobKeyFor(program, programType).getName();
    JobDetail job = JobBuilder.newJob(DefaultSchedulerService.ScheduledJob.class)
      .withIdentity(jobKey)
      .storeDurably(true)
      .build();
    try {
      scheduler.addJob(job, true);
      return job;
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }
  }

  private void scheduleJob(TriggerKey triggerKey, String scheduleName, String cronEntry, JobDetail job)
    throws SchedulerException {
    try {
      LOG.debug("Scheduling job {} with cron {}", scheduleName, cronEntry);

      TriggerBuilder trigger = TriggerBuilder.newTrigger()
        // all new triggers are added to the paused group which will ensure that the triggers are paused too
        .withIdentity(triggerKey.getName(), PAUSED_NEW_TRIGGERS_GROUP)
        .forJob(job)
        .withSchedule(CronScheduleBuilder
                        .cronSchedule(Schedulers.getQuartzCronExpression(cronEntry))
                        .withMisfireHandlingInstructionDoNothing());
      scheduler.scheduleJob(trigger.build());
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }
  }

  public List<ScheduledRuntime> previousScheduledRuntime(ProgramId program) throws SchedulerException {
    return getScheduledRuntime(program, true);
  }

  public List<ScheduledRuntime> nextScheduledRuntime(ProgramId program) throws SchedulerException {
    return getScheduledRuntime(program, false);
  }

  /**
   * Get all the scheduled run time of the program within the given time range in the future.
   * A program may contain multiple schedules. This method returns the scheduled runtimes for all the schedules
   * within the given time range. This method only takes
   + into account schedules based on time. For schedules based on data, an empty list will
   + be returned.
   *
   * @param program program to fetch the next runtime.
   * @param programType type of program.
   * @param startTimeSecs the start of the time range in seconds (inclusive, i.e. scheduled time larger or
   *                      equal to the start will be returned)
   * @param endTimeSecs the end of the time range in seconds (exclusive, i.e. scheduled time smaller than the end
   *                    will be returned)
   * @return list of scheduled runtimes for the program. Empty list if there are no schedules
   *         or if the program is not found
   * @throws SchedulerException on unforeseen error.
   */
  public List<ScheduledRuntime> getAllScheduledRunTimes(ProgramId program, SchedulableProgramType programType,
                                                        long startTimeSecs, long endTimeSecs)
    throws SchedulerException {
    // decrease the start time by one second to include the next fire time that is equal to the start time
    Date startTime = new Date(TimeUnit.SECONDS.toMillis(startTimeSecs - 1));
    long endTimeMillis = TimeUnit.SECONDS.toMillis(endTimeSecs);
    List<ScheduledRuntime> scheduledRuntimes = new ArrayList<>();
    try {
      for (Trigger trigger : scheduler.getTriggersOfJob(jobKeyFor(program, programType))) {
        // skip the trigger that is not enabled, since it cannot launch program as scheduled
        if (Trigger.TriggerState.PAUSED.equals(scheduler.getTriggerState(trigger.getKey()))) {
          continue;
        }
        Date nextFireTime = trigger.getFireTimeAfter(startTime);
        String triggerKeyString = trigger.getKey().toString();
        while (nextFireTime != null && nextFireTime.getTime() < endTimeMillis) {
          ScheduledRuntime runtime = new ScheduledRuntime(triggerKeyString, nextFireTime.getTime());
          scheduledRuntimes.add(runtime);
          nextFireTime = trigger.getFireTimeAfter(nextFireTime);
        }
      }
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }
    return scheduledRuntimes;
  }

  private List<ScheduledRuntime> getScheduledRuntime(ProgramId program,
                                                     boolean previousRuntimeRequested) throws SchedulerException {
    List<ScheduledRuntime> scheduledRuntimes = new ArrayList<>();
    SchedulableProgramType schedulableType = program.getType().getSchedulableType();
    if (schedulableType == null) {
      throw new IllegalArgumentException("Program " + program + " cannot be scheduled");
    }

    try {
      for (Trigger trigger : scheduler.getTriggersOfJob(jobKeyFor(program, schedulableType))) {
        long time;
        if (previousRuntimeRequested) {
          if (trigger.getPreviousFireTime() == null) {
            // previous fire time can be null for the triggers which are not yet fired
            continue;
          }
          time = trigger.getPreviousFireTime().getTime();
        } else {
          // skip the trigger that is not enabled, since it cannot launch program as scheduled
          if (scheduler.getTriggerState(trigger.getKey()) == Trigger.TriggerState.PAUSED) {
            continue;
          }
          time = trigger.getNextFireTime().getTime();
        }

        ScheduledRuntime runtime = new ScheduledRuntime(trigger.getKey().toString(), time);
        scheduledRuntimes.add(runtime);
      }
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }
    return scheduledRuntimes;
  }

  private static JobKey jobKeyFor(ProgramId program, SchedulableProgramType programType) {
    return new JobKey(AbstractTimeSchedulerService.programIdFor(program, programType));
  }

  private JobFactory createJobFactory() {
    return new JobFactory() {
      @Override
      public Job newJob(TriggerFiredBundle bundle, org.quartz.Scheduler scheduler)
        throws org.quartz.SchedulerException {
        Class<? extends Job> jobClass = bundle.getJobDetail().getJobClass();

        if (DefaultSchedulerService.ScheduledJob.class.isAssignableFrom(jobClass)) {
          return new DefaultSchedulerService.ScheduledJob(messagingService, topicId, metricsCollectionService);
        } else {
          try {
            return jobClass.newInstance();
          } catch (Exception e) {
            throw new org.quartz.SchedulerException("Failed to create instance of " + jobClass, e);
          }
        }
      }
    };
  }

  /**
   * @return Trigger keys in the map returned by {@link #getCronTriggerKeyMap(ProgramSchedule)}
   * @throws org.quartz.SchedulerException
   */
  private Collection<TriggerKey> getGroupedTriggerKeys(ProgramSchedule schedule)
    throws org.quartz.SchedulerException {
    return getCronTriggerKeyMap(schedule).values();
  }

  /**
   * @return A Map with cron expression as keys and corresponding trigger key as values.
   * Trigger keys are created from program name, programType and scheduleName (and cron expression if the trigger
   * in the schedule is a composite trigger) and TimeScheuler#PAUSED_NEW_TRIGGERS_GROUP
   * if it exists in this group else returns the {@link TriggerKey} prepared with null which gets it with
   * {@link Key#DEFAULT_GROUP}
   * @throws org.quartz.SchedulerException
   */
  private Map<String, TriggerKey> getCronTriggerKeyMap(ProgramSchedule schedule)
    throws org.quartz.SchedulerException {
    ProgramId program = schedule.getProgramId();
    SchedulableProgramType programType = program.getType().getSchedulableType();
    io.cdap.cdap.api.schedule.Trigger trigger = schedule.getTrigger();
    Map<String, TriggerKey> cronTriggerKeyMap = new HashMap<>();
    // Get a set of TimeTrigger if the schedule's trigger is a composite trigger
    if (trigger instanceof AbstractSatisfiableCompositeTrigger) {
      Set<SatisfiableTrigger> triggerSet =
        ((AbstractSatisfiableCompositeTrigger) trigger).getUnitTriggers().get(ProtoTrigger.Type.TIME);
      if (triggerSet == null) {
        return ImmutableMap.of();
      }
      for (SatisfiableTrigger timeTrigger : triggerSet) {
        String cron = ((TimeTrigger) timeTrigger).getCronExpression();
        String triggerName =
          AbstractTimeSchedulerService.getTriggerName(program, programType, schedule.getName(), cron);
        cronTriggerKeyMap.put(cron, triggerKeyForName(triggerName));
      }
      return cronTriggerKeyMap;
    }
    // No need to include cron expression in trigger key if the trigger is not composite trigger
    String triggerName = AbstractTimeSchedulerService.scheduleIdFor(program, programType, schedule.getName());
    cronTriggerKeyMap.put(((TimeTrigger) schedule.getTrigger()).getCronExpression(), triggerKeyForName(triggerName));
    return cronTriggerKeyMap;
  }

  /**
   * @return Trigger keys created from trigger name and TimeScheuler#PAUSED_NEW_TRIGGERS_GROUP
   * if it exists in this group else returns the {@link TriggerKey} prepared with null which gets it with
   * {@link Key#DEFAULT_GROUP}
   * @throws org.quartz.SchedulerException
   */
  private synchronized TriggerKey triggerKeyForName(String name) throws org.quartz.SchedulerException {
    TriggerKey neverResumedTriggerKey = new TriggerKey(name, PAUSED_NEW_TRIGGERS_GROUP);
    if (scheduler.checkExists(neverResumedTriggerKey)) {
      return neverResumedTriggerKey;
    }
    return new TriggerKey(name);
  }

  /**
   * Gets a {@link Trigger} associated with this program name, type and schedule name
   */
  private synchronized Trigger getTrigger(TriggerKey key, ProgramId program, String scheduleName)
    throws org.quartz.SchedulerException, NotFoundException {
    Trigger trigger = scheduler.getTrigger(key);
    if (trigger == null) {
      throw new NotFoundException(String.format("Time trigger with trigger key '%s' in schedule '%s' was not found",
                                                key.getName(), program.getParent().schedule(scheduleName).toString()));
    }
    return trigger;
  }

  /**
   * An empty {@link Job} to create a group in the scheduler
   */
  private final class EmptyJob implements Job {
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
      // no-op
    }
  }
}
