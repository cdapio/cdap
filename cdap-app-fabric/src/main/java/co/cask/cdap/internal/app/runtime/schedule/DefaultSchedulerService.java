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

import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
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
   * Handler that gets called by quartz to execute a scheduled job.
   */
  static final class ScheduledJob implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduledJob.class);
    private final ScheduleTaskRunner taskRunner;

    ScheduledJob(Store store, ProgramRuntimeService programRuntimeService, PreferencesStore preferencesStore) {
      taskRunner = new ScheduleTaskRunner(store, programRuntimeService, preferencesStore);
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
      LOG.debug("Trying to run job {} with trigger {}", context.getJobDetail().getKey().toString(),
                context.getTrigger().getKey().toString());

      String key = context.getTrigger().getKey().getName();
      String[] parts = key.split(":");
      Preconditions.checkArgument(parts.length == 5);

      String accountId = parts[0];
      String applicationId = parts[1];
      ProgramType programType = ProgramType.valueOf(parts[2]);
      String programId = parts[3];
      String scheduleName = parts[4];

      LOG.debug("Schedule execute {}", key);
      Arguments args = new BasicArguments(ImmutableMap.of(
        ProgramOptionConstants.LOGICAL_START_TIME, Long.toString(context.getScheduledFireTime().getTime()),
        ProgramOptionConstants.RETRY_COUNT, Integer.toString(context.getRefireCount()),
        ProgramOptionConstants.SCHEDULE_NAME, scheduleName
      ));

      try {
        taskRunner.run(Id.Program.from(accountId, applicationId, programId), programType, args);
      } catch (TaskExecutionException e) {
        throw new JobExecutionException(e.getMessage(), e.getCause(), e.isRefireImmediately());
      }
    }
  }
}
