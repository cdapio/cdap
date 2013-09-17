package com.continuuity.internal.app.runtime.schedule;

import com.continuuity.api.schedule.Schedule;
import com.continuuity.app.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.internal.app.runtime.BasicArguments;
import com.continuuity.internal.app.runtime.ProgramOptionConstants;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default Schedule service implementation.
 */
public class DefaultSchedulerService extends AbstractIdleService implements SchedulerService {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultSchedulerService.class);
  private final Scheduler scheduler;
  private final ProgramRuntimeService programRuntimeService;
  private final StoreFactory storeFactory;

//  private static ScheduleTaskRunner taskRunner;

  @Inject
  public DefaultSchedulerService(Scheduler scheduler, StoreFactory storeFactory,
                                 ProgramRuntimeService programRuntimeService) {
    this.scheduler = scheduler;
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
  public void schedule(Program program, Schedule schedule) {

    String scheduleName = schedule.getName();
    String cronEntry = schedule.getCronEntry();

    String programName = program.getName();
    String accountId = program.getAccountId();
    String applicationId = program.getApplicationId();

    //TODO: Make key in a single place
    String key = String.format("%s:%s:%s:%s", program.getType().name(), accountId, applicationId, programName);

    JobDetail job = JobBuilder.newJob(ScheduledJob.class).withIdentity(key, scheduleName).build();
    LOG.debug("Scheduling job {} with cron {}", scheduleName, cronEntry);
    Trigger trigger = TriggerBuilder.newTrigger()
                                    .withIdentity(scheduleName)
                                .withSchedule(CronScheduleBuilder.cronSchedule(getQuartzCronExpression(cronEntry)))
                                    .build();
    try {
      scheduler.scheduleJob(job, trigger);
    } catch (SchedulerException e) {
      throw Throwables.propagate(e);
    }
  }

  //Helper function to adapt cron entry to a cronExpression that is usable by quartz.
  //1. Quartz doesn't support wild-carding of both day-of-the-week and day-of-the-month
  //2. Quartz resolution is in seconds which cron entry doesn't support.
  private String getQuartzCronExpression(String cronEntry) {
    StringBuilder cronStringBuilder = new StringBuilder("0 " + cronEntry);
    if (cronStringBuilder.charAt(cronStringBuilder.length() - 1) == '*'){
      cronStringBuilder.setCharAt(cronStringBuilder.length() - 1, '?');
    }
    return cronStringBuilder.toString();
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
      LOG.info("Trying run job {}", context.getJobDetail().getKey().toString());

      String key = context.getJobDetail().getKey().getName();
      //TODO: Single place for key logic
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
}
