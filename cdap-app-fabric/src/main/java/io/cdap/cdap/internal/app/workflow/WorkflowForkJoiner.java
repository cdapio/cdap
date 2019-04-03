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

import co.cask.cdap.api.workflow.WorkflowForkNode;
import co.cask.cdap.api.workflow.WorkflowNode;

import java.util.List;

/**
 * Defines an interface for adding {@link WorkflowForkNode}.
 */
interface WorkflowForkJoiner {

  /**
   * Adds a {@link WorkflowForkNode}
   * @param branches {@link List} of branches to be added to the {@link WorkflowForkNode}
   */
  void addWorkflowForkNode(List<List<WorkflowNode>> branches);
}
