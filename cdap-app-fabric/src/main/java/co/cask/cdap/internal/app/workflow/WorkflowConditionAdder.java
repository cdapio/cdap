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

import co.cask.cdap.api.workflow.WorkflowConditionNode;
import co.cask.cdap.api.workflow.WorkflowNode;

import java.util.List;

/**
 * Defines an interface for adding {@link WorkflowConditionNode} to the Workflow.
 */
public interface WorkflowConditionAdder {
  /**
   * Adds a {@link WorkflowConditionNode} to the Workflow.
   * @param conditionNodeName the name of the node representing the condition. We use simple name of the predicate
   *                          class as the name of the condition node.
   * @param predicateClassName the name of the predicate class associated with this {@link WorkflowConditionNode}
   * @param ifBranch the branch that is executed when the predicate evaluates to the true
   * @param elseBranch the branch that is executed when the predicate evaluates to the false
   */
  void addWorkflowConditionNode(String conditionNodeName, String predicateClassName, List<WorkflowNode> ifBranch,
                                List<WorkflowNode> elseBranch);
}
