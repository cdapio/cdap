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
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.internal.schedule.TimeSchedule;
import co.cask.cdap.proto.Id;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
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

/**
 * Class that wraps Quartz scheduler. Needed to delegate start stop operations to classes that extend
 * DefaultSchedulerService.
 */
final class TimeScheduler implements Scheduler {
  private static final Logger LOG = LoggerFactory.getLogger(TimeScheduler.class);

  private org.quartz.Scheduler scheduler;
  private final StoreFactory storeFactory;
  private final Supplier<org.quartz.Scheduler> schedulerSupplier;
  private final ProgramRuntimeService programRuntimeService;
  private final PreferencesStore preferencesStore;

  TimeScheduler(Supplier<org.quartz.Scheduler> schedulerSupplier, StoreFactory storeFactory,
                ProgramRuntimeService programRuntimeService, PreferencesStore preferencesStore) {
    this.schedulerSupplier = schedulerSupplier;
    this.storeFactory = storeFactory;
    this.programRuntimeService = programRuntimeService;
    this.scheduler = null;
    this.preferencesStore = preferencesStore;
  }

  void start() throws SchedulerException {
    try {
      scheduler = schedulerSupplier.get();
      scheduler.setJobFactory(createJobFactory(storeFactory.create()));
      scheduler.start();
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }
  }

  void stop() throws SchedulerException {
    try {
      if (scheduler != null) {
        scheduler.shutdown();
      }
    } catch (org.quartz.SchedulerException e) {
      throw new SchedulerException(e);
    }
  }

  @Override
  public void schedule(Id.Program programId, SchedulableProgramType programType, Schedule schedule)
    throws SchedulerException {
    schedule(programId, programType, ImmutableList.of(schedule));
  }

  @Override
  public void schedule(Id.Program programId, SchedulableProgramType programType, Iterable<Schedule> schedules)
    throws SchedulerException {
    checkInitialized();
    Preconditions.checkNotNull(schedules);

    String jobKey = jobKeyFor(programId, programType).getName();
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
      String triggerKey = AbstractSchedulerService.scheduleIdFor(programId, programType, scheduleName);

      LOG.debug("Scheduling job {} with cron {}", scheduleName, cronEntry);

      Trigger trigger = TriggerBuilder.newTrigger()
        .withIdentity(triggerKey)
        .forJob(job)
        .withSchedule(CronScheduleBuilder.cronSchedule(getQuartzCronExpression(cronEntry)))
        .build();
      try {
        scheduler.scheduleJob(trigger);
      } catch (org.quartz.SchedulerException e) {
        throw new SchedulerException(e);
      }
    }
  }

  @Override
  public List<ScheduledRuntime> nextScheduledRuntime(Id.Program program, SchedulableProgramType programType)
    throws SchedulerException {
    checkInitialized();

    List<ScheduledRuntime> scheduledRuntimes = Lists.newArrayList();
    try {
      for (Trigger trigger : scheduler.getTriggersOfJob(jobKeyFor(program, programType))) {
        ScheduledRuntime runtime = new ScheduledRuntime(trigger.getKey().toString(),
                                                        trigger.getNextFireTime().getTime());
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
    // TODO modify the update flow [CDAP-1618]
    deleteSchedule(program, programType, schedule.getName());
    schedule(program, programType, schedule);
  }

  @Override
  public void deleteSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName)
    throws NotFoundException, SchedulerException {
    checkInitialized();
    try {
      Trigger trigger = scheduler.getTrigger(
        new TriggerKey(AbstractSchedulerService.scheduleIdFor(program, programType, scheduleName)));
      if (trigger == null) {
        throw new ScheduleNotFoundException(scheduleName);
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
          return new DefaultSchedulerService.ScheduledJob(store, programRuntimeService, preferencesStore);
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
