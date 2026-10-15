/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.schedule.constraint;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.queue.Job;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A system-injected {@link CheckableConstraint} that defers launching a program-status-triggered
 * schedule when the schedule maps plugin properties from a triggering workflow run whose final
 * {@link WorkflowToken} (containing the required resolved plugin properties key) has not yet been
 * committed to the metadata store.
 *
 * <p>This constraint deliberately does not extend
 * {@link io.cdap.cdap.proto.ProtoConstraint} and is never persisted onto schedules, so enabling or
 * disabling it requires no schema migration and remains compatible with rollback/downgrade.
 */
public class WorkflowTokenAvailableConstraint implements CheckableConstraint {

  private static final Logger LOG =
      LoggerFactory.getLogger(WorkflowTokenAvailableConstraint.class);
  private static final Gson GSON = new Gson();
  private static final String MAPPING_PROPERTY_KEY = "triggering.properties.mapping";
  private static final String REQUIRED_TOKEN_KEY = "resolved.plugin.properties.map";
  private static final String CONSTRAINT_CHECKER_COMPONENT = "constraintchecker";

  private final long maxWaitMillis;
  private final MetricsCollectionService metricsCollectionService;

  public WorkflowTokenAvailableConstraint(
      long maxWaitMillis, MetricsCollectionService metricsCollectionService) {
    this.maxWaitMillis = maxWaitMillis;
    this.metricsCollectionService = metricsCollectionService;
  }

  @Override
  public ConstraintResult check(ProgramSchedule schedule, ConstraintContext context) {
    if (!requiresPluginProperties(schedule)) {
      return ConstraintResult.SATISFIED;
    }

    Job job = context.getJob();
    long effectiveMaxWaitMillis = Math.min(maxWaitMillis, schedule.getTimeoutMillis());
    boolean checkedWorkflowNotification = false;
    boolean timedOut = false;
    long maxSatisfiedWaitMillis = 0L;

    for (Notification notification : job.getNotifications()) {
      ProgramRunId programRunId = extractProgramRunId(notification);
      if (programRunId == null
          || !ProgramType.WORKFLOW.equals(programRunId.getParent().getType())) {
        continue;
      }

      checkedWorkflowNotification = true;
      long notificationTime = getNotificationTime(notification, job.getCreationTime());
      long waitedMillis = Math.max(0L, context.getCheckTimeMillis() - notificationTime);

      WorkflowToken token = context.getWorkflowToken(programRunId);
      if (isFinalTokenReady(token)) {
        maxSatisfiedWaitMillis = Math.max(maxSatisfiedWaitMillis, waitedMillis);
        continue;
      }

      if (waitedMillis < effectiveMaxWaitMillis) {
        LOG.debug(
            "Deferring schedule '{}' (waited {} ms / {} ms): key '{}' is not yet present in"
                + " workflow token of triggering run '{}'.",
            schedule.getScheduleId(),
            waitedMillis,
            effectiveMaxWaitMillis,
            REQUIRED_TOKEN_KEY,
            programRunId);
        emitOutcome(
            schedule, Constants.Metrics.ScheduledJob.SCHEDULE_TOKEN_WAIT_DEFERRED, waitedMillis);
        return new ConstraintResult(
            ConstraintResult.SatisfiedState.NOT_SATISFIED,
            context.getCheckTimeMillis() + TimeUnit.SECONDS.toMillis(1));
      }

      LOG.warn(
          "Timed out after {} ms (max wait {} ms) waiting for key '{}' in workflow token of"
              + " triggering run '{}' for schedule '{}'; proceeding with launch.",
          waitedMillis,
          effectiveMaxWaitMillis,
          REQUIRED_TOKEN_KEY,
          programRunId,
          schedule.getScheduleId());
      emitOutcome(
          schedule, Constants.Metrics.ScheduledJob.SCHEDULE_TOKEN_WAIT_TIMEOUT, waitedMillis);
      timedOut = true;
    }

    if (checkedWorkflowNotification && !timedOut) {
      emitOutcome(
          schedule,
          Constants.Metrics.ScheduledJob.SCHEDULE_TOKEN_WAIT_SATISFIED,
          maxSatisfiedWaitMillis);
    }

    return ConstraintResult.SATISFIED;
  }

  private void emitOutcome(ProgramSchedule schedule, String counterMetric, long waitedMillis) {
    MetricsContext collector =
        metricsCollectionService.getContext(
            ImmutableMap.of(
                Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getEntityName(),
                Constants.Metrics.Tag.APP, schedule.getScheduleId().getApplication(),
                Constants.Metrics.Tag.COMPONENT, CONSTRAINT_CHECKER_COMPONENT,
                Constants.Metrics.Tag.SCHEDULE, schedule.getName()));
    collector.increment(counterMetric, 1L);
    collector.gauge(Constants.Metrics.ScheduledJob.SCHEDULE_TOKEN_WAIT_MS, waitedMillis);
  }

  private long getNotificationTime(Notification notification, long fallbackTimeMillis) {
    Map<String, String> properties = notification.getProperties();
    if (properties == null) {
      return fallbackTimeMillis;
    }
    String endTimeStr = properties.get(ProgramOptionConstants.END_TIME);
    if (endTimeStr == null) {
      return fallbackTimeMillis;
    }
    try {
      return Long.parseLong(endTimeStr);
    } catch (NumberFormatException e) {
      LOG.debug(
          "Failed to parse '{}' ('{}') from notification '{}'; falling back to job creation time.",
          ProgramOptionConstants.END_TIME,
          endTimeStr,
          notification,
          e);
      return fallbackTimeMillis;
    }
  }

  private boolean isFinalTokenReady(@Nullable WorkflowToken token) {
    return token != null && token.get(REQUIRED_TOKEN_KEY, WorkflowToken.Scope.USER) != null;
  }

  private boolean requiresPluginProperties(ProgramSchedule schedule) {
    Map<String, String> properties = schedule.getProperties();
    if (properties == null) {
      return false;
    }
    String mappingJson = properties.get(MAPPING_PROPERTY_KEY);
    if (mappingJson == null || mappingJson.isEmpty()) {
      return false;
    }
    try {
      JsonElement parsed = new JsonParser().parse(mappingJson);
      if (!parsed.isJsonObject()) {
        return false;
      }
      JsonObject mapping = parsed.getAsJsonObject();
      JsonElement pluginProperties = mapping.get("pluginProperties");
      return pluginProperties != null
          && pluginProperties.isJsonArray()
          && pluginProperties.getAsJsonArray().size() > 0;
    } catch (Exception e) {
      LOG.debug(
          "Failed to parse '{}' on schedule '{}'; treating as not requiring plugin properties.",
          MAPPING_PROPERTY_KEY,
          schedule.getScheduleId(),
          e);
      return false;
    }
  }

  @Nullable
  private ProgramRunId extractProgramRunId(Notification notification) {
    if (notification == null
        || notification.getNotificationType() != Notification.Type.PROGRAM_STATUS) {
      return null;
    }
    Map<String, String> properties = notification.getProperties();
    if (properties == null) {
      return null;
    }
    String programRunIdJson = properties.get(ProgramOptionConstants.PROGRAM_RUN_ID);
    if (programRunIdJson == null) {
      return null;
    }
    try {
      return GSON.fromJson(programRunIdJson, ProgramRunId.class);
    } catch (Exception e) {
      LOG.debug("Failed to deserialize programRunId from notification '{}'.", notification, e);
      return null;
    }
  }
}
