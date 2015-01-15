/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.proto.Id;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Abstract scheduler service common scheduling functionality. The extending classes should implement
 * prestart and poststop hooks to perform any action before starting the quartz scheduler and after stopping
 * the quartz scheduler.
 */
public abstract class AbstractSchedulerService extends AbstractIdleService implements SchedulerService {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSchedulerService.class);
  private final WrappedScheduler delegate;

  public AbstractSchedulerService(Supplier<org.quartz.Scheduler> schedulerSupplier, StoreFactory storeFactory,
                                  ProgramRuntimeService programRuntimeService, PreferencesStore preferencesStore) {
    this.delegate = new WrappedScheduler(schedulerSupplier, storeFactory, programRuntimeService, preferencesStore);
  }

  /**
   * Start the quartz scheduler service.
   */
  protected final void startScheduler() {
    try {
      delegate.start();
      LOG.info("Started scheduler");
    } catch (SchedulerException e) {
      LOG.error("Error starting scheduler {}", e.getCause(), e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Stop the quartz scheduler service.
   */
  protected final void stopScheduler() {
    try {
      delegate.stop();
      LOG.info("Stopped scheduler");
    } catch (SchedulerException e) {
      LOG.error("Error stopping scheduler {}", e.getCause(), e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void schedule(Id.Program programId, SchedulableProgramType programType, Iterable<Schedule> schedules) {
    delegate.schedule(programId, programType, schedules);
  }

  @Override
  public List<ScheduledRuntime> nextScheduledRuntime(Id.Program program, SchedulableProgramType programType) {
   return delegate.nextScheduledRuntime(program, programType);
  }

  @Override
  public List<String> getScheduleIds(Id.Program program, SchedulableProgramType programType) {
    return delegate.getScheduleIds(program, programType);
  }

  @Override
  public void suspendSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName) {
    delegate.suspendSchedule(program, programType, scheduleName);
  }

  @Override
  public void resumeSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName) {
    delegate.resumeSchedule(program, programType, scheduleName);
  }

  @Override
  public void deleteSchedule(String scheduleId) {
    delegate.deleteSchedule(scheduleId);
  }

  @Override
  public void deleteSchedules(Id.Program program, SchedulableProgramType programType) {
    delegate.deleteSchedules(program, programType);
  }

  @Override
  public ScheduleState scheduleState (Id.Program program, SchedulableProgramType programType, String scheduleName) {
    return delegate.scheduleState(program, programType, scheduleName);
  }

  /**
   * class that wraps Quartz scheduler. Needed to delegate start stop operations to classes that extend
   * DefaultSchedulerService.
   */
  static final class WrappedScheduler implements co.cask.cdap.internal.app.runtime.schedule.Scheduler {
    private Scheduler scheduler;
    private final StoreFactory storeFactory;
    private final Supplier<Scheduler> schedulerSupplier;
    private final ProgramRuntimeService programRuntimeService;
    private final PreferencesStore preferencesStore;

    WrappedScheduler(Supplier<Scheduler> schedulerSupplier, StoreFactory storeFactory,
                     ProgramRuntimeService programRuntimeService, PreferencesStore preferencesStore) {
      this.schedulerSupplier = schedulerSupplier;
      this.storeFactory = storeFactory;
      this.programRuntimeService = programRuntimeService;
      this.scheduler = null;
      this.preferencesStore = preferencesStore;
    }

    void start() throws SchedulerException {
      scheduler = schedulerSupplier.get();
      scheduler.setJobFactory(createJobFactory(storeFactory.create()));
      scheduler.start();
    }

    void stop() throws SchedulerException {
      if (scheduler != null) {
        scheduler.shutdown();
      }
    }

    @Override
    public void schedule(Id.Program programId, SchedulableProgramType programType, Iterable<Schedule> schedules) {
      checkInitialized();
      Preconditions.checkNotNull(schedules);

      String jobKey = getJobKey(programId, programType).getName();
      JobDetail job = JobBuilder.newJob(DefaultSchedulerService.ScheduledJob.class)
        .withIdentity(jobKey)
        .storeDurably(true)
        .build();
      try {
        scheduler.addJob(job, true);
      } catch (SchedulerException e) {
        throw Throwables.propagate(e);
      }
      for (Schedule schedule : schedules) {
        String scheduleName = schedule.getName();
        String cronEntry = schedule.getCronEntry();
        String triggerKey = getScheduleId(programId, programType, scheduleName);

        LOG.debug("Scheduling job {} with cron {}", scheduleName, cronEntry);

        Trigger trigger = TriggerBuilder.newTrigger()
          .withIdentity(triggerKey)
          .forJob(job)
          .withSchedule(CronScheduleBuilder
                          .cronSchedule(getQuartzCronExpression(cronEntry)))
          .build();
        try {
          scheduler.scheduleJob(trigger);
        } catch (SchedulerException e) {
          throw Throwables.propagate(e);
        }
      }
    }

    @Override
    public List<ScheduledRuntime> nextScheduledRuntime(Id.Program program, SchedulableProgramType programType) {
      checkInitialized();

      List<ScheduledRuntime> scheduledRuntimes = Lists.newArrayList();
      try {
        for (Trigger trigger : scheduler.getTriggersOfJob(getJobKey(program, programType))) {
          ScheduledRuntime runtime = new ScheduledRuntime(trigger.getKey().toString(),
                                                          trigger.getNextFireTime().getTime());
          scheduledRuntimes.add(runtime);
        }
      } catch (SchedulerException e) {
        throw Throwables.propagate(e);
      }
      return scheduledRuntimes;
    }

    @Override
    public List<String> getScheduleIds(Id.Program program, SchedulableProgramType programType) {
      checkInitialized();

      List<String> scheduleIds = Lists.newArrayList();
      try {
        for (Trigger trigger : scheduler.getTriggersOfJob(getJobKey(program, programType))) {
          scheduleIds.add(trigger.getKey().getName());
        }
      }   catch (SchedulerException e) {
        throw Throwables.propagate(e);
      }
      return scheduleIds;
    }


    @Override
    public void suspendSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName) {
      checkInitialized();
      try {
        scheduler.pauseTrigger(new TriggerKey(getScheduleId(program, programType, scheduleName)));
      } catch (SchedulerException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void resumeSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName) {
      checkInitialized();
      try {
        scheduler.resumeTrigger(new TriggerKey(getScheduleId(program, programType, scheduleName)));
      } catch (SchedulerException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void deleteSchedule(String scheduleId) {
      checkInitialized();
      try {
        Trigger trigger = scheduler.getTrigger(new TriggerKey(scheduleId));
        Preconditions.checkNotNull(trigger);

        scheduler.unscheduleJob(trigger.getKey());

        JobKey jobKey = trigger.getJobKey();
        if (scheduler.getTriggersOfJob(jobKey).isEmpty()) {
          scheduler.deleteJob(jobKey);
        }
      } catch (SchedulerException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void deleteSchedules(Id.Program program, SchedulableProgramType programType) {
      checkInitialized();
      try {
        scheduler.deleteJob(getJobKey(program, programType));
      } catch (SchedulerException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public ScheduleState scheduleState(Id.Program program, SchedulableProgramType programType, String scheduleName) {
      checkInitialized();
      try {
        Trigger.TriggerState state = scheduler.getTriggerState(new TriggerKey(getScheduleId(program, programType,
                                                                                            scheduleName)));
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
      } catch (SchedulerException e) {
        throw Throwables.propagate(e);
      }
    }

    private void checkInitialized() {
      Preconditions.checkNotNull(scheduler, "Scheduler not yet initialized");
    }

    private String getScheduleId(Id.Program program, SchedulableProgramType programType, String scheduleName) {
      return String.format("%s:%s", getJobKey(program, programType).getName(), scheduleName);
    }


    private JobKey getJobKey(Id.Program program, SchedulableProgramType programType) {
      return new JobKey(String.format("%s:%s:%s:%s", programType.name(), program.getNamespaceId(),
                                      program.getApplicationId(), program.getId()));
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
        public Job newJob(TriggerFiredBundle bundle, org.quartz.Scheduler scheduler) throws SchedulerException {
          Class<? extends Job> jobClass = bundle.getJobDetail().getJobClass();

          if (DefaultSchedulerService.ScheduledJob.class.isAssignableFrom(jobClass)) {
            return new DefaultSchedulerService.ScheduledJob(store, programRuntimeService, preferencesStore);
          } else {
            try {
              return jobClass.newInstance();
            } catch (Exception e) {
              throw new SchedulerException("Failed to create instance of " + jobClass, e);
            }
          }
        }
      };
    }
  }
}
