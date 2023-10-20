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
import io.cdap.cdap.proto.operation.OperationType;
import java.util.Set;

/**
 * Provides the context for the current operation run.
 */
public interface LongRunningOperationContext {

  /**
   * Get the {@link OperationRunId} for the current run.
   *
   * @return the current runid
   */
  OperationRunId getRunId();

  /**
   * Get the {@link OperationType} to be used by the runner for loading the right operation class.
   *
   * @return the type of the current operation
   */
  OperationType getType();

  /**
   * Used by the {@link LongRunningOperation} to update the resources operated on in the
   * {@link io.cdap.cdap.proto.operation.OperationMeta} for the run. The input is set as we want all
   * resources to be unique
   *
   * @param resources A set of resources to be updated.
   *
   */
  // TODO Add exceptions based on implementations.
  void updateOperationResources(Set<OperationResource> resources);
}
