/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Preconditions;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Trigger;
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
  public static final class ScheduledJob implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduledJob.class);
    private final ScheduleTaskPublisher taskPublisher;

    ScheduledJob(Store store, MessagingService messagingService, CConfiguration cConf) {
      this.taskPublisher = new ScheduleTaskPublisher(store, messagingService, cConf);
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
      LOG.debug("Trying to run job {} with trigger {}", context.getJobDetail().getKey().toString(),
                context.getTrigger().getKey().toString());
      Trigger trigger = context.getTrigger();
      String key = trigger.getKey().getName();
      String[] parts = key.split(":");
      Preconditions.checkArgument(parts.length == 6, String.format("Trigger's key name %s has %d parts instead of 6",
                                                                   key, parts.length));

      String namespaceId = parts[0];
      String applicationId = parts[1];
      String appVersion = parts[2];
      ProgramType programType = ProgramType.valueOfSchedulableType(SchedulableProgramType.valueOf(parts[3]));
      String programName = parts[4];
      String scheduleName = parts[5];

      LOG.debug("Schedule execute {}", key);

      ProgramId programId = new ApplicationId(namespaceId, applicationId, appVersion).program(programType, programName);
      try {
        taskPublisher.publishNotification(programId, scheduleName, context.getRefireCount(),
                                          context.getScheduledFireTime().getTime());
      } catch (Throwable t) {
        // Do not remove this log line. The exception at higher level gets caught by the quartz scheduler and is not
        // logged in cdap master logs making it hard to debug issues.
        LOG.warn("Error while publishing notification for program {}. {}", programId, t);
        throw new JobExecutionException(t.getMessage(), t.getCause(), false);
      }
    }
  }
}
