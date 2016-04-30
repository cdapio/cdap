/*
 * Copyright © 2016 Cask Data, Inc.
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

import javax.annotation.Nullable;

/**
 * Interface for providing information about {@link WorkflowInfo}.
 */
public interface WorkflowInfoProvider {

  /**
   * @return the {@link WorkflowToken} associated with the current {@link Workflow},
   * if the program is executed as a part of the Workflow; returns {@code null} otherwise.
   */
  @Nullable
  WorkflowToken getWorkflowToken();

  /**
   * @return information about the enclosing {@link Workflow} run, if the program is executed
   * as a part of the Workflow; returns {@code null} otherwise.
   */
  @Nullable
  WorkflowInfo getWorkflowInfo();
}
