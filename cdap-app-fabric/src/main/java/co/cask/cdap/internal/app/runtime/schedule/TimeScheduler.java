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

package co.cask.cdap.internal.app.runtime.schedule;

import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.internal.schedule.TimeSchedule;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.ProtoTrigger;
import co.cask.cdap.proto.ScheduledRuntime;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Class that wraps Quartz scheduler. Needed to delegate start stop operations to classes that extend
 * DefaultSchedulerService.
 */
public final class TimeScheduler implements Scheduler {
  private static final Logger LOG = LoggerFactory.getLogger(TimeScheduler.class);
  private static final String PAUSED_NEW_TRIGGERS_GROUP = "NewPausedTriggers";

  private org.quartz.Scheduler scheduler;
  private final Supplier<org.quartz.Scheduler> schedulerSupplier;
  private final MessagingService messagingService;
  private ListeningExecutorService taskExecutorService;
  private boolean schedulerStarted;
  private final TopicId topicId;

  @Inject
  TimeScheduler(Supplier<org.quartz.Scheduler> schedulerSupplier, MessagingService messagingService,
                CConfiguration cConf) {
    this.schedulerSupplier = schedulerSupplier;
    this.messagingService = messagingService;
    this.scheduler = null;
    this.schedulerStarted = false;
    this.topicId = NamespaceId.SYSTEM.topic(cConf.get(Constants.Scheduler.TIME_EVENT_TOPIC));
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
   * Creates a paused group TimeScheduler#PAUSED_NEW_TRIGGERS_GROUP by adding a dummy job to it if it does not exists
   * already. This is needed so that we can add new triggers to this paused group and they will be paused too.
   *
   * @throws org.quartz.SchedulerException
   */
  private void initNewPausedTriggersGroup() throws org.quartz.SchedulerException {
    // if the dummy job does not already exists in the TimeScheduler#PAUSED_NEW_TRIGGERS_GROUP then create a dummy job
    // which will create the TimeScheduler#PAUSED_NEW_TRIGGERS_GROUP
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

  @Override
  public void addProgramSchedule(ProgramSchedule schedule) throws AlreadyExistsException, SchedulerException {
    checkInitialized();
    ProgramId program = schedule.getProgramId();
    SchedulableProgramType programType = program.getType().getSchedulableType();

    try {
      assertScheduleDoesNotExist(program, programType, schedule.getName());
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }

    JobDetail job = addJob(program, programType);
    scheduleJob(program, programType, schedule.getName(),
                ((ProtoTrigger.TimeTrigger) schedule.getTrigger()).getCronExpression(),
                job, schedule.getProperties());
  }

  @Override
  public void deleteProgramSchedule(ProgramSchedule schedule) throws NotFoundException, SchedulerException {
    deleteSchedule(schedule.getProgramId(), schedule.getProgramId().getType().getSchedulableType(),
                   schedule.getName());
  }

  @Override
  public void suspendProgramSchedule(ProgramSchedule schedule) throws NotFoundException, SchedulerException {
    suspendSchedule(schedule.getProgramId(), schedule.getProgramId().getType().getSchedulableType(),
                    schedule.getName());
  }

  @Override
  public void resumeProgramSchedule(ProgramSchedule schedule) throws NotFoundException, SchedulerException {
    resumeSchedule(schedule.getProgramId(), schedule.getProgramId().getType().getSchedulableType(),
                   schedule.getName());
  }

  /**
   * Assert the schedule with a given name and of a given program does not exist
   *
   * @throws ObjectAlreadyExistsException if the corresponding schedule already exists
   */
  private void assertScheduleDoesNotExist(ProgramId program, SchedulableProgramType programType,
                                          String scheduleName) throws org.quartz.SchedulerException {
    TriggerKey triggerKey = getGroupedTriggerKey(program, programType, scheduleName);
    // Once the schedule is resumed we move the trigger from TimeScheduler#PAUSED_NEW_TRIGGERS_GROUP to
    // Key#DEFAULT_GROUP so before adding check if this schedule does not exist.
    // We do not need to check for same schedule in the current list as its already checked in app configuration stage
    if (scheduler.checkExists(triggerKey)) {
      throw new ObjectAlreadyExistsException("Unable to store Trigger with name " + triggerKey.getName() +
                                               "because one already exists with this identification.");
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

  private void scheduleJob(ProgramId program, SchedulableProgramType programType,
                           String scheduleName, String cronEntry, JobDetail job, Map<String, String> properties)
    throws SchedulerException {
    try {
      TriggerKey triggerKey = getGroupedTriggerKey(program, programType, scheduleName);

      LOG.debug("Scheduling job {} with cron {}", scheduleName, cronEntry);

      TriggerBuilder trigger = TriggerBuilder.newTrigger()
        // all new triggers are added to the paused group which will ensure that the triggers are paused too
        .withIdentity(triggerKey.getName(), PAUSED_NEW_TRIGGERS_GROUP)
        .forJob(job)
        .withSchedule(CronScheduleBuilder
                        .cronSchedule(Schedulers.getQuartzCronExpression(cronEntry))
                        .withMisfireHandlingInstructionDoNothing());
      addProperties(trigger, properties);

      scheduler.scheduleJob(trigger.build());
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }
  }

  @Override
  public void schedule(ProgramId program, SchedulableProgramType programType, Schedule schedule,
                       Map<String, String> properties) throws SchedulerException {
    schedule(program, programType, ImmutableList.of(schedule), properties);
  }

  public synchronized void schedule(ProgramId program, SchedulableProgramType programType,
                                    Iterable<Schedule> schedules,
                                    Map<String, String> properties) throws SchedulerException {
    checkInitialized();
    try {
      validateSchedules(program, programType, schedules);
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }

    JobDetail job = addJob(program, programType);
    for (Schedule schedule : schedules) {
      TimeSchedule timeSchedule = (TimeSchedule) schedule;
      String scheduleName = timeSchedule.getName();
      String cronEntry = timeSchedule.getCronEntry();
      scheduleJob(program, programType, scheduleName, cronEntry, job, properties);
    }
  }

  private void validateSchedules(ProgramId program, SchedulableProgramType programType,
                                 Iterable<Schedule> schedules) throws org.quartz.SchedulerException {
    Preconditions.checkNotNull(schedules);
    for (Schedule schedule : schedules) {
      Preconditions.checkArgument(schedule instanceof TimeSchedule);
      TimeSchedule timeSchedule = (TimeSchedule) schedule;
      assertScheduleDoesNotExist(program, programType, timeSchedule.getName());
    }
  }

  @Override
  public List<ScheduledRuntime> previousScheduledRuntime(ProgramId program, SchedulableProgramType programType)
    throws SchedulerException {
    return getScheduledRuntime(program, programType, true);
  }

  @Override
  public List<ScheduledRuntime> nextScheduledRuntime(ProgramId program, SchedulableProgramType programType)
    throws SchedulerException {
    return getScheduledRuntime(program, programType, false);
  }

  private List<ScheduledRuntime> getScheduledRuntime(ProgramId program, SchedulableProgramType programType,
                                                     boolean previousRuntimeRequested) throws SchedulerException {
    checkInitialized();
    List<ScheduledRuntime> scheduledRuntimes = new ArrayList<>();
    try {
      for (Trigger trigger : scheduler.getTriggersOfJob(jobKeyFor(program, programType))) {
        long time;
        if (previousRuntimeRequested) {
          if (trigger.getPreviousFireTime() == null) {
            // previous fire time can be null for the triggers which are not yet fired
            continue;
          }
          time = trigger.getPreviousFireTime().getTime();
        } else {
          if (scheduler.getTriggerState(trigger.getKey()) == Trigger.TriggerState.PAUSED) {
            // if the trigger is paused, then skip getting the next fire time
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


  @Override
  public synchronized void suspendSchedule(ProgramId program, SchedulableProgramType programType, String scheduleName)
    throws NotFoundException, SchedulerException {
    checkInitialized();
    try {
      scheduler.pauseTrigger(getGroupedTriggerKey(program, programType, scheduleName));
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }
  }

  @Override
  public synchronized void resumeSchedule(ProgramId program, SchedulableProgramType programType, String scheduleName)
    throws NotFoundException, SchedulerException {

    checkInitialized();

    try {
      TriggerKey triggerKey = getGroupedTriggerKey(program, programType, scheduleName);
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
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }
  }

  @Override
  public synchronized void deleteSchedule(ProgramId program, SchedulableProgramType programType, String scheduleName)
    throws NotFoundException, SchedulerException {
    checkInitialized();
    try {
      Trigger trigger = getTrigger(program, programType, scheduleName);
      scheduler.unscheduleJob(trigger.getKey());

      JobKey jobKey = trigger.getJobKey();
      if (scheduler.getTriggersOfJob(jobKey).isEmpty()) {
        scheduler.deleteJob(jobKey);
      }
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }
  }

  @Override
  public void deleteSchedules(ProgramId program, SchedulableProgramType programType)
    throws SchedulerException {
    checkInitialized();
    try {
      scheduler.deleteJob(jobKeyFor(program, programType));
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }
  }

  private void checkInitialized() {
    Preconditions.checkNotNull(scheduler, "Scheduler not yet initialized");
  }

  private static JobKey jobKeyFor(ProgramId program, SchedulableProgramType programType) {
    return new JobKey(AbstractSchedulerService.programIdFor(program, programType));
  }

  private JobFactory createJobFactory() {
    return new JobFactory() {
      @Override
      public Job newJob(TriggerFiredBundle bundle, org.quartz.Scheduler scheduler)
        throws org.quartz.SchedulerException {
        Class<? extends Job> jobClass = bundle.getJobDetail().getJobClass();

        if (DefaultSchedulerService.ScheduledJob.class.isAssignableFrom(jobClass)) {
          return new DefaultSchedulerService.ScheduledJob(messagingService, topicId);
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
   * @return Trigger key created from program, programType and scheduleName and TimeScheuler#PAUSED_NEW_TRIGGERS_GROUP
   * if it exists in this group else returns the {@link TriggerKey} prepared with null which gets it with
   * {@link Key#DEFAULT_GROUP}
   * @throws org.quartz.SchedulerException
   */
  private synchronized TriggerKey getGroupedTriggerKey(ProgramId program, SchedulableProgramType programType,
                                                       String scheduleName)
    throws org.quartz.SchedulerException {

    TriggerKey neverResumedTriggerKey = new TriggerKey(AbstractSchedulerService.scheduleIdFor(program, programType,
                                                                                              scheduleName),
                                                       PAUSED_NEW_TRIGGERS_GROUP);
    if (scheduler.checkExists(neverResumedTriggerKey)) {
      return neverResumedTriggerKey;
    }
    return new TriggerKey(AbstractSchedulerService.scheduleIdFor(program, programType, scheduleName));
  }

  /**
   * Gets a {@link Trigger} associated with this program name, type and schedule name
   */
  private synchronized Trigger getTrigger(ProgramId program, SchedulableProgramType programType, String scheduleName)
    throws org.quartz.SchedulerException, ScheduleNotFoundException {
    Trigger trigger = scheduler.getTrigger(getGroupedTriggerKey(program, programType, scheduleName));
    if (trigger == null) {
      throw new ScheduleNotFoundException(program.getParent().schedule(scheduleName));
    }
    return  trigger;
  }

  /**
   * Adds properties to a {@link TriggerBuilder} to used by the {@link Trigger}
   */
  private void addProperties(TriggerBuilder trigger, Map<String, String> properties) {
    if (properties != null) {
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        trigger.usingJobData(entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * An empty {@link Job} to create a group in the scheduler
   */
  private final class EmptyJob implements Job {
    public void execute(JobExecutionContext context) throws JobExecutionException {
      // no-op
    }
  }
}
