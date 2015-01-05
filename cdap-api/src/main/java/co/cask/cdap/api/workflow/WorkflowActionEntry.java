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
 * Represents entry for the {@link WorkflowAction}
 */
public final class WorkflowActionEntry {
  private final String name;
  private final WorkflowSupportedProgram type;

  public WorkflowActionEntry(String name, WorkflowSupportedProgram type) {
    this.name = name;
    this.type = type;
  }

  /**
   *
   * @return name of the {@link WorkflowAction} represented by this entry
   */
  public String getName() {
    return name;
  }

  /**
   *
   * @return type of the {@link WorkflowAction} represented by this entry
   */
  public WorkflowSupportedProgram getType() {
    return type;
  }
}
