package com.continuuity.internal.app.runtime.schedule;

import com.continuuity.api.schedule.Schedule;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.app.store.StoreFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SimpleScheduleService extends AbstractIdleService implements SchedulerService {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleScheduleService.class);
  private final Scheduler scheduler;
  private final ProgramRuntimeService programRuntimeService;
  private final StoreFactory storeFactory;

  private static ScheduleTaskRunner taskRunner;

  @Inject
  public SimpleScheduleService(Scheduler scheduler, StoreFactory storeFactory,
                               ProgramRuntimeService programRuntimeService) {
    this.scheduler = scheduler;
    this.programRuntimeService = programRuntimeService;
    this.storeFactory = storeFactory;
  }

  @Override
  public void startUp() {
    try {
      LOG.info("Starting scheduler...");
      taskRunner = new ScheduleTaskRunner(storeFactory, programRuntimeService);
      scheduler.start();
      LOG.info("Scheduler started!");
    } catch (Throwable e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void shutDown() {
    try {
      LOG.info("Stopping scheduler...");
      scheduler.shutdown();
      LOG.info("Scheduler stopped!");
    } catch (SchedulerException e){
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void schedule(Program program, Schedule schedule) {


    String scheduleName = schedule.getName();
    String cronExpression = schedule.getCronExpression();

    String programName = program.getName();
    String accountId = program.getAccountId();
    String applicationId = program.getApplicationId();

    //TODO: Make key in a single place
    String key = String.format("%s:%s:%s", accountId, applicationId, programName);

    JobDetail job = JobBuilder.newJob(ScheduledJob.class).withIdentity(key, scheduleName).build();
    LOG.info("Scheduling job {} with cron {}", scheduleName, cronExpression);
    Trigger trigger = TriggerBuilder.newTrigger()
                                    .withIdentity(scheduleName)
                                    //Add 0 for seconds since quartz needs resolution in seconds
                                .withSchedule(CronScheduleBuilder.cronSchedule(getQuartzCronExpression(cronExpression)))
                                    .build();
    try {
      scheduler.scheduleJob(job, trigger);
    } catch (SchedulerException e) {
      throw Throwables.propagate(e);
    }
  }

  private String getQuartzCronExpression(String cronExpression) {

    StringBuilder cronStringBuilder = new StringBuilder("0 " +cronExpression);

    if (cronStringBuilder.charAt(cronStringBuilder.length()-1) == '*'){
     // cronExpression..(cronExpression.length()-1)
      cronStringBuilder.setCharAt(cronStringBuilder.length()-1, '?');
    }

    return cronStringBuilder.toString();

  }

  @Override
  public void unSchedule(Program program) {

  }

  /**
   *
   */
  public static class ScheduledJob implements Job {

    public ScheduledJob() {

    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
      LOG.info("Trying run job {}", context.getJobDetail().getKey().toString());

      String key = context.getJobDetail().getKey().getName();
      //TODO: Single place for key logic
      String[] parts = key.split(":");
      Preconditions.checkArgument(parts.length == 3);

      String accountId = parts[0];
      String applicationId = parts[1];
      String programId = parts[2];
      LOG.info("Account ID "+ accountId + " application:  "+ applicationId + " programId: " + programId);
      taskRunner.run(accountId, applicationId, programId, null);
    }
  }
}
