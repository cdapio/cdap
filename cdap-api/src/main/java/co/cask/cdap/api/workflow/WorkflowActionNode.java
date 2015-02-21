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

package co.cask.cdap.api.workflow;

/**
 * Represents the ACTION node in the {@link Workflow}.
 */
public class WorkflowActionNode extends WorkflowNode {
  private final ScheduleProgramInfo program;

  public WorkflowActionNode(String nodeId, ScheduleProgramInfo program) {
    super(nodeId, WorkflowNodeType.ACTION);
    this.program = program;
  }

  /**
   *
   * @return the program information associated with the {@link WorkflowNode}
   */
  public ScheduleProgramInfo getProgram() {
    return this.program;
  }
}
