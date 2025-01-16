/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.schedule.trigger;

import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.WorkflowId;
import java.io.IOException;
import java.util.List;

/**
 * Context object, exposing information that may be useful processing
 * {@link io.cdap.cdap.proto.Notification} for a trigger.
 */
public class NotificationContext {

  private final List<Notification> notifications;
  private final AppMetadataStore appMetadataStore;
  private final RetryStrategy retryStrategy;

  public NotificationContext(List<Notification> notifications, AppMetadataStore appMetadataStore,
      RetryStrategy retryStrategy) {
    this.notifications = notifications;
    this.appMetadataStore = appMetadataStore;
    this.retryStrategy = retryStrategy;
  }

  public List<Notification> getNotifications() {
    return notifications;
  }

  public RetryStrategy getRetryStrategy() {
    return retryStrategy;
  }

  /**
   * Fetches the {@link WorkflowToken} for the provided {@link ProgramRunId}.
   *
   * @return The workflow token if the program is a workflow, {@code null} otherwise.
   */
  public WorkflowToken getWorkflowToken(ProgramRunId programRunId) throws IOException {
    ProgramId programId = programRunId.getParent();
    if (!programId.getType().equals(ProgramType.WORKFLOW)) {
      return null;
    }
    return appMetadataStore.getWorkflowToken(
        new WorkflowId(programId.getParent(), programId.getProgram()), programRunId.getRun());
  }
}
