package com.continuuity.internal.app.runtime.schedule;

import com.continuuity.api.schedule.Schedule;
import com.continuuity.app.Id;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
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
                                  ProgramRuntimeService programRuntimeService) {
    this.delegate = new WrappedScheduler(schedulerSupplier, storeFactory, programRuntimeService);
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
  public void schedule(Id.Program programId, Type programType, Iterable<Schedule> schedules) {
    delegate.schedule(programId, programType, schedules);
  }

  @Override
  public List<ScheduledRuntime> nextScheduledRuntime(Id.Program program, Type programType) {
   return delegate.nextScheduledRuntime(program, programType);
  }

  @Override
  public List<String> getScheduleIds(Id.Program program, Type programType) {
    return delegate.getScheduleIds(program, programType);
  }

  @Override
  public void suspendSchedule(String scheduleId) {
    delegate.suspendSchedule(scheduleId);
  }

  @Override
  public void resumeSchedule(String scheduleId) {
    delegate.resumeSchedule(scheduleId);
  }

  @Override
  public void deleteSchedules(Id.Program program, Type programType,
                              List<String> scheduleIds) {
    delegate.deleteSchedules(program, programType, scheduleIds);
  }

  @Override
  public ScheduleState scheduleState (String scheduleId) {
    return delegate.scheduleState(scheduleId);
  }

  /**
   * class that wraps Quartz scheduler. Needed to delegate start stop operations to classes that extend
   * DefaultSchedulerService.
   */
  static final class WrappedScheduler implements com.continuuity.internal.app.runtime.schedule.Scheduler {
    private Scheduler scheduler;
    private final StoreFactory storeFactory;
    private final Supplier<Scheduler> schedulerSupplier;
    private final ProgramRuntimeService programRuntimeService;

    WrappedScheduler(Supplier<Scheduler> schedulerSupplier, StoreFactory storeFactory,
                     ProgramRuntimeService programRuntimeService) {
      this.schedulerSupplier = schedulerSupplier;
      this.storeFactory = storeFactory;
      this.programRuntimeService = programRuntimeService;
      this.scheduler = null;
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
    public void schedule(Id.Program programId, Type programType, Iterable<Schedule> schedules) {
      Preconditions.checkNotNull(scheduler, "Scheduler not yet initialized");
      Preconditions.checkNotNull(schedules);

      String key = getJobKey(programId, programType);
      JobDetail job = JobBuilder.newJob(DefaultSchedulerService.ScheduledJob.class)
        .withIdentity(key)
        .storeDurably(true)
        .build();
      try {
        scheduler.addJob(job, true);
      } catch (SchedulerException e) {
        throw Throwables.propagate(e);
      }
      int idx = 0;
      for (Schedule schedule : schedules) {
        String scheduleName = schedule.getName();
        String cronEntry = schedule.getCronEntry();
        String triggerKey = String.format("%s:%s:%s:%s:%d:%s",
                                          programType.name(), programId.getAccountId(),
                                          programId.getApplicationId(), programId.getId(), idx, schedule.getName());

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
    public List<ScheduledRuntime> nextScheduledRuntime(Id.Program program, Type programType) {
      Preconditions.checkNotNull(scheduler, "Scheduler not yet initialized");

      List<ScheduledRuntime> scheduledRuntimes = Lists.newArrayList();
      String key = getJobKey(program, programType);
      try {
        for (Trigger trigger : scheduler.getTriggersOfJob(new JobKey(key))) {
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
    public List<String> getScheduleIds(Id.Program program, Type programType) {
      Preconditions.checkNotNull(scheduler, "Scheduler not yet initialized");

      List<String> scheduleIds = Lists.newArrayList();
      String key = getJobKey(program, programType);
      try {
        for (Trigger trigger : scheduler.getTriggersOfJob(new JobKey(key))) {
          scheduleIds.add(trigger.getKey().getName());
        }
      }   catch (SchedulerException e) {
        throw Throwables.propagate(e);
      }
      return scheduleIds;
    }


    @Override
    public void suspendSchedule(String scheduleId) {
      Preconditions.checkNotNull(scheduler, "Scheduler not yet initialized");
      try {
        scheduler.pauseTrigger(new TriggerKey(scheduleId));
      } catch (SchedulerException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void resumeSchedule(String scheduleId) {
      Preconditions.checkNotNull(scheduler, "Scheduler not yet initialized");
      try {
        scheduler.resumeTrigger(new TriggerKey(scheduleId));
      } catch (SchedulerException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void deleteSchedules(Id.Program program, Type programType, List<String> scheduleIds) {
      Preconditions.checkNotNull(scheduler, "Scheduler not yet initialized");
      try {
        for (String scheduleId : scheduleIds) {
          scheduler.pauseTrigger(new TriggerKey(scheduleId));
        }
        String key = getJobKey(program, programType);
        scheduler.deleteJob(new JobKey(key));
      } catch (SchedulerException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public ScheduleState scheduleState(String scheduleId) {
      Preconditions.checkNotNull(scheduler, "Scheduler not yet initialized");
      try {
        Trigger.TriggerState state = scheduler.getTriggerState(new TriggerKey(scheduleId));
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

    private String getJobKey(Id.Program program, Type programType) {
      return String.format("%s:%s:%s:%s", programType.name(), program.getAccountId(),
                           program.getApplicationId(), program.getId());
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
            return new DefaultSchedulerService.ScheduledJob(store, programRuntimeService);
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
