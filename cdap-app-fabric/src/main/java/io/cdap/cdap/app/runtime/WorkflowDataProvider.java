/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime;

import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.workflow.WorkflowToken;

import java.util.Set;

/**
 * An interface for classes that can provide data required for the execution of the workflow.
 * This interface is implemented by controllers of the programs which can possibly run inside the Workflow,
 * such as MapReduce and Spark. If these programs were executed as a part of Workflow, then Workflow driver
 * can call these methods to get the data which need to be passed along the further stages in the Workflow.
 */
public interface WorkflowDataProvider {

  /**
   * Returns a {@link WorkflowToken}.
   */
  WorkflowToken getWorkflowToken();

  /**
   * Returns set of field lineage {@link Operation}s.
   */
  Set<Operation> getFieldLineageOperations();
}
