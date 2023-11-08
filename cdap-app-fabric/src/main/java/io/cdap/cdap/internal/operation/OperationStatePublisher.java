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


import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationError;
import io.cdap.cdap.proto.operation.OperationResource;
import java.util.Set;

/**
 * Publishes operation state messages.
 */
public interface OperationStatePublisher {

  /**
   * Publishes message with the current resources. The operation status should be RUNNING
   *
   * @param resources Current resources for the operation.
   */
  void publishResources(OperationRunId runId, Set<OperationResource> resources);

  /**
   * Publishes the current operation status as RUNNING.
   */
  void publishRunning(OperationRunId runId);

  /**
   * Publishes the current operation status as FAILED.
   */
  void publishFailed(OperationRunId runId, OperationError error);

  /**
   * Publishes the current operation status as SUCCEEDED.
   */
  void publishSuccess(OperationRunId runId);

  /**
   * Publishes the current operation status as KILLED.
   */
  void publishKilled(OperationRunId runId);

  void publishStopping(OperationRunId runId);

  void publishStarting(OperationRunId runId);
}
