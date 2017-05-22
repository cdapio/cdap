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

import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

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

    ScheduledJob(MessagingService messagingService, TopicId topicId) {
      this.taskPublisher = new ScheduleTaskPublisher(messagingService, topicId);
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
      // Skip program type in parts[3] and program name in parts[4]
      String scheduleName = parts[5];

      LOG.debug("Schedule execute {}", key);
      ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

      builder.put(ProgramOptionConstants.SCHEDULE_NAME, scheduleName);

      Map<String, String> userOverrides = ImmutableMap.of(ProgramOptionConstants.LOGICAL_START_TIME,
                                                          Long.toString(context.getScheduledFireTime().getTime()));

      ScheduleId scheduleId = new ApplicationId(namespaceId, applicationId, appVersion).schedule(scheduleName);
      try {
        taskPublisher.publishNotification(Notification.Type.TIME, scheduleId, builder.build(), userOverrides);
      } catch (Throwable t) {
        // Do not remove this log line. The exception at higher level gets caught by the quartz scheduler and is not
        // logged in cdap master logs making it hard to debug issues.
        LOG.warn("Error while publishing notification for schedule {}. {}", scheduleId, t);
        throw new JobExecutionException(t.getMessage(), t.getCause(), false);
      }
    }
  }
}
