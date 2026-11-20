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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.queue.Job;
import io.cdap.cdap.internal.app.runtime.schedule.store.Schedulers;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.util.ArrayList;
import java.util.List;
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
  // These keys are owned by cdap-data-pipeline-base, which app-fabric cannot depend on. Keep them in
  // sync with SmartWorkflow.TRIGGERING_PROPERTIES_MAPPING and SmartWorkflow /
  // PipelineTriggers.RESOLVED_PLUGIN_PROPERTIES_MAP.
  private static final String MAPPING_PROPERTY_KEY = "triggering.properties.mapping";
  private static final String REQUIRED_TOKEN_KEY = "resolved.plugin.properties.map";

  private final long maxWaitMillis;
  private final MetricsCollectionService metricsCollectionService;

  public WorkflowTokenAvailableConstraint(
      long maxWaitMillis, MetricsCollectionService metricsCollectionService) {
    this.maxWaitMillis = maxWaitMillis;
    this.metricsCollectionService = metricsCollectionService;
  }

  @Override
  public ConstraintResult check(ProgramSchedule schedule, ConstraintContext context) {
    List<JsonObject> pluginPropertyMappings = getPluginPropertyMappings(schedule);
    if (pluginPropertyMappings.isEmpty()) {
      return ConstraintResult.SATISFIED;
    }

    Job job = context.getJob();
    // All notifications share one deadline. The wait starts when the last triggering run ended,
    // but never before the job was created, so a slow notification relay cannot use up the wait.
    long waitStartMillis = getWaitStartMillis(job);
    long waitedMillis = Math.max(0L, context.getCheckTimeMillis() - waitStartMillis);
    // The job is deleted once it is older than the schedule timeout, so never wait past that point.
    long timeUntilJobTimeoutMillis =
        schedule.getTimeoutMillis() - (waitStartMillis - job.getCreationTime());
    long effectiveMaxWaitMillis = Math.min(maxWaitMillis, timeUntilJobTimeoutMillis);

    List<ProgramRunId> timedOutRuns = new ArrayList<>();
    boolean checkedWorkflowRun = false;
    for (Notification notification : job.getNotifications()) {
      ProgramRunId programRunId = extractProgramRunId(notification);
      if (programRunId == null
          || programRunId.getType() != ProgramType.WORKFLOW
          || !hasMappingForRun(pluginPropertyMappings, programRunId)) {
        continue;
      }

      checkedWorkflowRun = true;
      if (isFinalTokenReady(context.getWorkflowToken(programRunId))) {
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
      timedOutRuns.add(programRunId);
    }

    if (!timedOutRuns.isEmpty()) {
      LOG.warn(
          "Timed out after {} ms (max wait {} ms) waiting for key '{}' in workflow token of"
              + " triggering runs {} for schedule '{}'; proceeding with launch.",
          waitedMillis,
          effectiveMaxWaitMillis,
          REQUIRED_TOKEN_KEY,
          timedOutRuns,
          schedule.getScheduleId());
      emitOutcome(
          schedule, Constants.Metrics.ScheduledJob.SCHEDULE_TOKEN_WAIT_TIMEOUT, waitedMillis);
    } else if (checkedWorkflowRun) {
      emitOutcome(
          schedule, Constants.Metrics.ScheduledJob.SCHEDULE_TOKEN_WAIT_SATISFIED, waitedMillis);
    }
    return ConstraintResult.SATISFIED;
  }

  private void emitOutcome(ProgramSchedule schedule, String counterMetric, long waitedMillis) {
    MetricsContext collector =
        metricsCollectionService.getContext(
            Schedulers.getScheduleJobMetricsTags(
                schedule.getScheduleId().getApplication(), schedule.getName()));
    collector.increment(counterMetric, 1L);
    collector.gauge(Constants.Metrics.ScheduledJob.SCHEDULE_TOKEN_WAIT_MS, waitedMillis);
  }

  /**
   * Returns the time from which the wait is measured: the latest end time among the job's
   * notifications, or the job creation time if that is later.
   */
  private long getWaitStartMillis(Job job) {
    long waitStartMillis = job.getCreationTime();
    for (Notification notification : job.getNotifications()) {
      waitStartMillis =
          Math.max(waitStartMillis, getNotificationTime(notification, job.getCreationTime()));
    }
    return waitStartMillis;
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

  /**
   * Returns the plugin property mappings of the schedule that name a stage, or an empty list if the
   * schedule does not map any plugin properties.
   */
  private List<JsonObject> getPluginPropertyMappings(ProgramSchedule schedule) {
    List<JsonObject> result = new ArrayList<>();
    Map<String, String> properties = schedule.getProperties();
    String mappingJson = properties == null ? null : properties.get(MAPPING_PROPERTY_KEY);
    if (mappingJson == null || mappingJson.isEmpty()) {
      return result;
    }
    try {
      JsonObject mapping = GSON.fromJson(mappingJson, JsonObject.class);
      JsonElement pluginProperties = mapping == null ? null : mapping.get("pluginProperties");
      if (pluginProperties == null || !pluginProperties.isJsonArray()) {
        return result;
      }
      for (JsonElement element : pluginProperties.getAsJsonArray()) {
        if (element.isJsonObject()
            && !getStringOrEmpty(element.getAsJsonObject(), "stageName").isEmpty()) {
          result.add(element.getAsJsonObject());
        }
      }
    } catch (RuntimeException e) {
      LOG.debug(
          "Failed to parse '{}' on schedule '{}'; treating as not requiring plugin properties.",
          MAPPING_PROPERTY_KEY,
          schedule.getScheduleId(),
          e);
      result.clear();
    }
    return result;
  }

  /**
   * Returns whether any plugin property mapping reads from the pipeline of the given run. A mapping
   * without a {@code pipeline} filter (schedules created before 6.8) applies to every triggering
   * pipeline.
   */
  private static boolean hasMappingForRun(List<JsonObject> mappings, ProgramRunId programRunId) {
    for (JsonObject mapping : mappings) {
      JsonElement pipeline = mapping.get("pipeline");
      if (pipeline == null || !pipeline.isJsonObject()) {
        return true;
      }
      JsonObject pipelineId = pipeline.getAsJsonObject();
      if (programRunId.getNamespace().equals(getStringOrEmpty(pipelineId, "namespace"))
          && programRunId.getApplication().equals(getStringOrEmpty(pipelineId, "name"))) {
        return true;
      }
    }
    return false;
  }

  private static String getStringOrEmpty(JsonObject object, String key) {
    JsonElement value = object.get(key);
    return value != null && value.isJsonPrimitive() ? value.getAsString() : "";
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
    } catch (RuntimeException e) {
      LOG.debug("Failed to deserialize programRunId from notification '{}'.", notification, e);
      return null;
    }
  }
}
