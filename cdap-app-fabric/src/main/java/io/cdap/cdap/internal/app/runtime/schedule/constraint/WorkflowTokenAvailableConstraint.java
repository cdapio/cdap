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
import com.google.gson.JsonParser;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.queue.Job;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.util.Map;
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

  private final long maxWaitMillis;
  private final String mappingPropertyKey;
  private final String requiredTokenKey;

  public WorkflowTokenAvailableConstraint(
      long maxWaitMillis, String mappingPropertyKey, String requiredTokenKey) {
    this.maxWaitMillis = maxWaitMillis;
    this.mappingPropertyKey = mappingPropertyKey;
    this.requiredTokenKey = requiredTokenKey;
  }

  @Override
  public ConstraintResult check(ProgramSchedule schedule, ConstraintContext context) {
    if (!requiresPluginProperties(schedule)) {
      return ConstraintResult.SATISFIED;
    }

    Job job = context.getJob();
    long waitedMillis = Math.max(0L, context.getCheckTimeMillis() - job.getCreationTime());

    for (Notification notification : job.getNotifications()) {
      ProgramRunId programRunId = extractProgramRunId(notification);
      if (programRunId == null
          || !ProgramType.WORKFLOW.equals(programRunId.getParent().getType())) {
        continue;
      }

      WorkflowToken token = context.getWorkflowToken(programRunId);
      if (isFinalTokenReady(token)) {
        continue;
      }

      if (waitedMillis < maxWaitMillis) {
        LOG.debug(
            "Deferring schedule '{}' (waited {} ms / {} ms): key '{}' is not yet present in"
                + " workflow token of triggering run '{}'.",
            schedule.getScheduleId(),
            waitedMillis,
            maxWaitMillis,
            requiredTokenKey,
            programRunId);
        return new ConstraintResult(ConstraintResult.SatisfiedState.NOT_SATISFIED);
      }

      LOG.warn(
          "Timed out after {} ms (max wait {} ms) waiting for key '{}' in workflow token of"
              + " triggering run '{}' for schedule '{}'; proceeding with launch.",
          waitedMillis,
          maxWaitMillis,
          requiredTokenKey,
          programRunId,
          schedule.getScheduleId());
    }

    return ConstraintResult.SATISFIED;
  }

  private boolean isFinalTokenReady(@Nullable WorkflowToken token) {
    return token != null && token.get(requiredTokenKey, WorkflowToken.Scope.USER) != null;
  }

  private boolean requiresPluginProperties(ProgramSchedule schedule) {
    Map<String, String> properties = schedule.getProperties();
    if (properties == null) {
      return false;
    }
    String mappingJson = properties.get(mappingPropertyKey);
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
          mappingPropertyKey,
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
