package com.continuuity.internal.app.runtime.schedule;

import com.continuuity.api.schedule.Schedule;
import com.continuuity.app.Id;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.internal.app.runtime.BasicArguments;
import com.continuuity.internal.app.runtime.ProgramOptionConstants;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
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
 * Default Schedule service implementation.
 */
public class DefaultSchedulerService extends AbstractIdleService implements SchedulerService {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultSchedulerService.class);
  private final Supplier<Scheduler> schedulerSupplier;
  private final ProgramRuntimeService programRuntimeService;
  private final StoreFactory storeFactory;
  private Scheduler scheduler;

  @Inject
  public DefaultSchedulerService(Supplier<Scheduler> schedulerSupplier, StoreFactory storeFactory,
                                 ProgramRuntimeService programRuntimeService) {
    this.schedulerSupplier = schedulerSupplier;
    this.programRuntimeService = programRuntimeService;
    this.storeFactory = storeFactory;
  }

  private JobFactory createJobFactory(final Store store) {
    return new JobFactory() {

      @Override
      public Job newJob(TriggerFiredBundle bundle, Scheduler scheduler) throws SchedulerException {
        Class<? extends Job> jobClass = bundle.getJobDetail().getJobClass();

        if (ScheduledJob.class.isAssignableFrom(jobClass)) {
          return new ScheduledJob(store, programRuntimeService);
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

  @Override
  public void startUp() throws Exception {
    scheduler = schedulerSupplier.get();
    scheduler.setJobFactory(createJobFactory(storeFactory.create()));
    scheduler.start();
    LOG.debug("Scheduler started!");
  }

  @Override
  public void shutDown() throws Exception {
    scheduler.shutdown();
    LOG.debug("Scheduler stopped!");
  }

  @Override
  public void schedule(Id.Program programId, Type programType, Iterable<Schedule> schedules) {
    Preconditions.checkNotNull(schedules);

    String key = getJobKey(programId, programType);
    JobDetail job = JobBuilder.newJob(ScheduledJob.class)
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
    try {
      scheduler.pauseTrigger(new TriggerKey(scheduleId));
    } catch (SchedulerException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void resumeSchedule(String scheduleId) {
    try {
      scheduler.resumeTrigger(new TriggerKey(scheduleId));
    } catch (SchedulerException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void deleteSchedules(Id.Program program, Type programType,
                              List<String> scheduleIds) {
    try {
      for (String scheduleId : scheduleIds) {
        scheduler.pauseTrigger(new TriggerKey(scheduleId));
      }
      String key = getJobKey(program, programType);
      scheduler.deleteJob(new JobKey(key));
    } catch (SchedulerException e){
      throw Throwables.propagate(e);
    }
  }

  @Override
  public ScheduleState scheduleState (String scheduleId){
    try {
      Trigger.TriggerState state = scheduler.getTriggerState(new TriggerKey(scheduleId));
      // Map trigger state to schedule state.
      // This method is only interested in returning if the scheduler is
      // Paused, Scheduled or NotFound.
      switch (state){
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
      if (cronStringBuilder.charAt(cronStringBuilder.length() - 1) == '*'){
        cronStringBuilder.setCharAt(cronStringBuilder.length() - 1, '?');
      }
      return cronStringBuilder.toString();
    } else {
      //Use the given cronExpression
      return cronEntry;
    }
  }

  /**
   * Handler that gets called by quartz to schedule a job.
   */
  private static final class ScheduledJob implements Job {

    private final ScheduleTaskRunner taskRunner;
    ScheduledJob(Store store, ProgramRuntimeService programRuntimeService) {
      taskRunner = new ScheduleTaskRunner(store, programRuntimeService);
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
      LOG.debug("Trying to run job {} with trigger {}", context.getJobDetail().getKey().toString(),
                                                       context.getTrigger().getKey().toString());

      String key = context.getJobDetail().getKey().getName();
      String[] parts = key.split(":");
      Preconditions.checkArgument(parts.length == 4);

      Type programType = Type.valueOf(parts[0]);
      String accountId = parts[1];
      String applicationId = parts[2];
      String programId = parts[3];

      LOG.debug("Schedule execute {}", key);
      Arguments args = new BasicArguments(ImmutableMap.of(
          ProgramOptionConstants.LOGICAL_START_TIME, Long.toString(context.getScheduledFireTime().getTime()),
          ProgramOptionConstants.RETRY_COUNT, Integer.toString(context.getRefireCount())
      ));

      taskRunner.run(Id.Program.from(accountId, applicationId, programId), programType, args);
    }
  }

  private String getJobKey(Id.Program program, Type programType) {
    return String.format("%s:%s:%s:%s", programType.name(), program.getAccountId(),
                                        program.getApplicationId(), program.getId());
  }
}
