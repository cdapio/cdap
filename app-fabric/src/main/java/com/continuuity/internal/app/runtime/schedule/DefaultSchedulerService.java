package com.continuuity.internal.app.runtime.schedule;

import com.continuuity.app.Id;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.app.store.Store;
import com.continuuity.internal.app.runtime.BasicArguments;
import com.continuuity.internal.app.runtime.ProgramOptionConstants;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ScheduleJob class is used in quartz scheduler job store. Retaining the DefaultSchedulerService$ScheduleJob
 * for backwards compatibility.
 * TODO: Refactor in 3.0.0
 */
public class DefaultSchedulerService {

  /**
   * Handler that gets called by quartz to schedule a job.
   */
  static final class ScheduledJob implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduledJob.class);
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
}
