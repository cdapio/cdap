/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.workflow;

import co.cask.cdap.api.workflow.Workflow;

/**
 * Provides unique id for the nodes in the {@link Workflow}
 */
class WorkflowNodeIdProvider {

  private int nodeId = 0;

  /**
   *
   * @return the new id that can be assigned to the node in the {@link Workflow}
   */
  String getUniqueNodeId() {
    nodeId++;
    return String.valueOf(nodeId);
  }
}
