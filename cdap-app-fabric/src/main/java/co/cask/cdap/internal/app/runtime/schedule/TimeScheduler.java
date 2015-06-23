/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.internal.app.services.PropertiesResolver;
import co.cask.cdap.internal.schedule.TimeSchedule;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ScheduledRuntime;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.twill.common.Threads;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Class that wraps Quartz scheduler. Needed to delegate start stop operations to classes that extend
 * DefaultSchedulerService.
 */
final class TimeScheduler implements Scheduler {
  private static final Logger LOG = LoggerFactory.getLogger(TimeScheduler.class);

  private org.quartz.Scheduler scheduler;
  private final Supplier<org.quartz.Scheduler> schedulerSupplier;
  private final ProgramLifecycleService lifecycleService;
  private final PropertiesResolver propertiesResolver;
  private ListeningExecutorService taskExecutorService;
  private boolean schedulerStarted;
  private final Store store;

  @Inject
  TimeScheduler(Supplier<org.quartz.Scheduler> schedulerSupplier, Store store,
                ProgramLifecycleService lifecycleService, PropertiesResolver propertiesResolver) {
    this.schedulerSupplier = schedulerSupplier;
    this.store = store;
    this.lifecycleService = lifecycleService;
    this.scheduler = null;
    this.propertiesResolver = propertiesResolver;
    this.schedulerStarted = false;
  }

  void init() throws SchedulerException {
    try {
      taskExecutorService = MoreExecutors.listeningDecorator(
        Executors.newCachedThreadPool(Threads.createDaemonThreadFactory("time-schedule-task")));
      scheduler = schedulerSupplier.get();
      scheduler.setJobFactory(createJobFactory(store));
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }
  }

  void lazyStart() throws SchedulerException {
    try {
      scheduler.start();
      schedulerStarted = true;
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
  public void schedule(Id.Program program, SchedulableProgramType programType, Schedule schedule)
    throws SchedulerException {
    schedule(program, programType, schedule, ImmutableMap.<String, String>of());
  }

  @Override
  public void schedule(Id.Program program, SchedulableProgramType programType, Schedule schedule,
                       Map<String, String> properties) throws SchedulerException {
    schedule(program, programType, ImmutableList.of(schedule), properties);
  }

  @Override
  public void schedule(Id.Program programId, SchedulableProgramType programType, Iterable<Schedule> schedules)
    throws SchedulerException {
    schedule(programId, programType, schedules, ImmutableMap.<String, String>of());
  }

  @Override
  public void schedule(Id.Program program, SchedulableProgramType programType, Iterable<Schedule> schedules,
                       Map<String, String> properties) throws SchedulerException {
    checkInitialized();
    Preconditions.checkNotNull(schedules);

    String jobKey = jobKeyFor(program, programType).getName();
    JobDetail job = JobBuilder.newJob(DefaultSchedulerService.ScheduledJob.class)
      .withIdentity(jobKey)
      .storeDurably(true)
      .build();
    try {
      scheduler.addJob(job, true);
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }
    for (Schedule schedule : schedules) {
      Preconditions.checkArgument(schedule instanceof TimeSchedule);
      TimeSchedule timeSchedule = (TimeSchedule) schedule;
      String scheduleName = timeSchedule.getName();
      String cronEntry = timeSchedule.getCronEntry();
      String triggerKey = AbstractSchedulerService.scheduleIdFor(program, programType, scheduleName);

      LOG.debug("Scheduling job {} with cron {}", scheduleName, cronEntry);

      TriggerBuilder trigger = TriggerBuilder.newTrigger()
        .withIdentity(triggerKey)
        .forJob(job)
        .withSchedule(CronScheduleBuilder
                        .cronSchedule(getQuartzCronExpression(cronEntry))
                        .withMisfireHandlingInstructionDoNothing());
      if (properties != null) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
          trigger.usingJobData(entry.getKey(), entry.getValue());
        }
      }
      try {
        scheduler.scheduleJob(trigger.build());
      } catch (org.quartz.SchedulerException e) {
        throw new SchedulerException(e);
      }
    }
  }

  @Override
  public List<ScheduledRuntime> previousScheduledRuntime(Id.Program program, SchedulableProgramType programType)
    throws SchedulerException {
    return getScheduledRuntime(program, programType, true);
  }

  @Override
  public List<ScheduledRuntime> nextScheduledRuntime(Id.Program program, SchedulableProgramType programType)
    throws SchedulerException {
    return getScheduledRuntime(program, programType, false);
  }

  private List<ScheduledRuntime> getScheduledRuntime(Id.Program program, SchedulableProgramType programType,
                                                     boolean previousRuntimeRequested) throws SchedulerException {
    checkInitialized();

    List<ScheduledRuntime> scheduledRuntimes = Lists.newArrayList();
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
  public List<String> getScheduleIds(Id.Program program, SchedulableProgramType programType)
    throws SchedulerException {
    checkInitialized();

    List<String> scheduleIds = Lists.newArrayList();
    try {
      for (Trigger trigger : scheduler.getTriggersOfJob(jobKeyFor(program, programType))) {
        scheduleIds.add(trigger.getKey().getName());
      }
    }   catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }
    return scheduleIds;
  }


  @Override
  public void suspendSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName)
    throws NotFoundException, SchedulerException {
    checkInitialized();
    try {
      scheduler.pauseTrigger(new TriggerKey(AbstractSchedulerService.scheduleIdFor(program, programType,
                                                                                   scheduleName)));
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }
  }

  @Override
  public void resumeSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName)
    throws NotFoundException, SchedulerException {
    checkInitialized();
    try {
      scheduler.resumeTrigger(new TriggerKey(AbstractSchedulerService.scheduleIdFor(program, programType,
                                                                                    scheduleName)));
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }
  }

  @Override
  public void updateSchedule(Id.Program program, SchedulableProgramType programType, Schedule schedule)
    throws NotFoundException, SchedulerException {
    updateSchedule(program, programType, schedule, ImmutableMap.<String, String>of());
  }

  @Override
  public void updateSchedule(Id.Program program, SchedulableProgramType programType, Schedule schedule,
                             Map<String, String> properties) throws NotFoundException, SchedulerException {
    // TODO modify the update flow [CDAP-1618]
    deleteSchedule(program, programType, schedule.getName());
    schedule(program, programType, schedule, properties);
  }

  @Override
  public void deleteSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName)
    throws NotFoundException, SchedulerException {
    checkInitialized();
    try {
      Trigger trigger = scheduler.getTrigger(
        new TriggerKey(AbstractSchedulerService.scheduleIdFor(program, programType, scheduleName)));
      if (trigger == null) {
        throw new ScheduleNotFoundException(Id.Schedule.from(program.getApplication(), scheduleName));
      }

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
  public void deleteSchedules(Id.Program program, SchedulableProgramType programType)
    throws SchedulerException {
    checkInitialized();
    try {
      scheduler.deleteJob(jobKeyFor(program, programType));
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }
  }

  @Override
  public void deleteAllSchedules(Id.Namespace namespaceId) throws SchedulerException {
    for (ApplicationSpecification appSpec : store.getAllApplications(namespaceId)) {
      deleteAllSchedules(namespaceId, appSpec);
    }
  }

  private void deleteAllSchedules(Id.Namespace namespaceId, ApplicationSpecification appSpec)
    throws SchedulerException {
    for (ScheduleSpecification scheduleSpec : appSpec.getSchedules().values()) {
      Id.Application appId = Id.Application.from(namespaceId.getId(), appSpec.getName());
      ProgramType programType = ProgramType.valueOfSchedulableType(scheduleSpec.getProgram().getProgramType());
      Id.Program programId = Id.Program.from(appId, programType, scheduleSpec.getProgram().getProgramName());
      deleteSchedules(programId, scheduleSpec.getProgram().getProgramType());
    }
  }

  @Override
  public ScheduleState scheduleState(Id.Program program, SchedulableProgramType programType, String scheduleName)
    throws SchedulerException {
    checkInitialized();
    try {
      Trigger.TriggerState state = scheduler.getTriggerState(
        new TriggerKey(AbstractSchedulerService.scheduleIdFor(program, programType, scheduleName)));
      // Map trigger state to schedule state.
      // This method is only interested in returning if the scheduler is
      // Paused, Scheduled or NotFound.
      switch (state) {
        case NONE:
          return ScheduleState.NOT_FOUND;
        case PAUSED:
          return ScheduleState.SUSPENDED;
        default:
          return ScheduleState.SCHEDULED;
      }
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }
  }

  private void checkInitialized() {
    Preconditions.checkNotNull(scheduler, "Scheduler not yet initialized");
  }

  private static JobKey jobKeyFor(Id.Program program, SchedulableProgramType programType) {
    return new JobKey(AbstractSchedulerService.programIdFor(program, programType));
  }

  //Helper function to adapt cron entry to a cronExpression that is usable by quartz.
  //1. Quartz doesn't support wild-carding of both day-of-the-week and day-of-the-month
  //2. Quartz resolution is in seconds which cron entry doesn't support.
  private String getQuartzCronExpression(String cronEntry) {
    // Checks if the cronEntry is quartz cron Expression or unix like cronEntry format.
    // CronExpression will directly be used for tests.
    String parts [] = cronEntry.split(" ");
    Preconditions.checkArgument(parts.length >= 5 , "Invalid cron entry format");
    if (parts.length == 5) {
      //cron entry format
      StringBuilder cronStringBuilder = new StringBuilder("0 " + cronEntry);
      if (cronStringBuilder.charAt(cronStringBuilder.length() - 1) == '*') {
        cronStringBuilder.setCharAt(cronStringBuilder.length() - 1, '?');
      }
      return cronStringBuilder.toString();
    } else {
      //Use the given cronExpression
      return cronEntry;
    }
  }

  private JobFactory createJobFactory(final Store store) {
    return new JobFactory() {
      @Override
      public Job newJob(TriggerFiredBundle bundle, org.quartz.Scheduler scheduler)
        throws org.quartz.SchedulerException {
        Class<? extends Job> jobClass = bundle.getJobDetail().getJobClass();

        if (DefaultSchedulerService.ScheduledJob.class.isAssignableFrom(jobClass)) {
          return new DefaultSchedulerService.ScheduledJob(store, lifecycleService, propertiesResolver,
                                                          taskExecutorService);
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
}
