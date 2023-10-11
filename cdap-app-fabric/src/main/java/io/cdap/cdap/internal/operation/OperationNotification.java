/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.operation;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.common.conf.Constants.Operation;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationError;
import io.cdap.cdap.proto.operation.OperationResource;
import io.cdap.cdap.proto.operation.OperationRunStatus;
import java.lang.reflect.Type;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Encapsulates an operation notification sent through TMS.
 */
public class OperationNotification {

  private final OperationRunId runId;
  private final OperationRunStatus status;
  private final String user;
  @Nullable
  private final Set<OperationResource> resources;
  @Nullable
  private final Instant endTime;
  @Nullable
  private final OperationError error;

  private static final Gson GSON = new Gson();
  private static final Type resourcesType = new TypeToken<Set<OperationResource>>() {
  }.getType();

  /**
   * Default constructor.
   */
  public OperationNotification(OperationRunId runId, OperationRunStatus status,
      String user, @Nullable Set<OperationResource> resources, Instant endTime,
      @Nullable OperationError error
  ) {
    this.runId = runId;
    this.status = status;
    this.user = user;
    this.resources = resources;
    this.endTime = endTime;
    this.error = error;
  }

  /**
   * Parse {@link Notification} to generate {@link OperationNotification}.
   *
   * @param notification notification to parse
   */
  public static OperationNotification fromNotification(Notification notification) {
    Map<String, String> properties = notification.getProperties();

    OperationRunId runId = GSON.fromJson(properties.get(Operation.RUN_ID_NOTIFICATION_KEY),
        OperationRunId.class);
    OperationRunStatus status = OperationRunStatus.valueOf(
        properties.get(Operation.STATUS_NOTIFICATION_KEY));
    String user = properties.get(Operation.USER_ID_NOTIFICATION_KEY);
    OperationError error = GSON.fromJson(properties.get(Operation.ERROR_NOTIFICATION_KEY),
        OperationError.class);
    Set<OperationResource> resources = GSON.fromJson(
        properties.get(Operation.RESOURCES_NOTIFICATION_KEY), resourcesType);
    Instant endTime = Instant.parse(properties.get(Operation.ENDTIME_NOTIFICATION_KEY));

    return new OperationNotification(runId, status, user, resources, endTime, error);
  }

  public OperationRunId getRunId() {
    return runId;
  }

  public OperationRunStatus getStatus() {
    return status;
  }

  public String getUser() {
    return user;
  }

  @Nullable
  public Set<OperationResource> getResources() {
    return resources;
  }

  @Nullable
  public Instant getEndTime() {
    return endTime;
  }

  @Nullable
  public OperationError getError() {
    return error;
  }
}
