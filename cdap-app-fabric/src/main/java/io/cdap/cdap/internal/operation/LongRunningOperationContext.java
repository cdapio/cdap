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
import io.cdap.cdap.proto.operation.OperationResource;
import java.util.Set;

/**
 * Provides the context for the current operation run.
 */
public class LongRunningOperationContext {

  private final OperationRunId runId;
  private final OperationStatePublisher statePublisher;

  /**
   * Default constructor.
   *
   * @param runId id of the current operation
   * @param type type of the current operation
   * @param statePublisher to publish the operation metadata
   */
  public LongRunningOperationContext(OperationRunId runId, OperationStatePublisher statePublisher) {
    this.runId = runId;
    this.statePublisher = statePublisher;
  }

  /**
   * Get the {@link OperationRunId} for the current run.
   *
   * @return the current runid
   */
  public OperationRunId getRunId() {
    return runId;
  }

  /**
   * Used by the {@link LongRunningOperation} to update the resources operated on in the
   * {@link io.cdap.cdap.proto.operation.OperationMeta} for the run. The input is set as we want all
   * resources to be unique
   *
   * @param resources A set of resources to be updated.
   */
  public void updateOperationResources(Set<OperationResource> resources) {
    statePublisher.publishResources(runId, resources);
  }
}
