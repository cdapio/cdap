/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.ProgramState;
import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.api.SchedulableProgramContext;
import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.lineage.field.LineageRecorder;
import co.cask.cdap.api.messaging.MessagingContext;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.security.store.SecureStore;

import java.util.Map;

/**
 * Represents the runtime context of a {@link Workflow}. This context is also
 * available to {@link Condition}.
 */
public interface WorkflowContext extends SchedulableProgramContext, RuntimeContext, Transactional, MessagingContext,
  ServiceDiscoverer, DatasetContext, PluginContext, SecureStore, LineageRecorder {

  WorkflowSpecification getWorkflowSpecification();

  /**
   * Returns {@link ConditionSpecification} associated with the condition node in the Workflow.
   * @throws UnsupportedOperationException if it is not called from {@link Predicate} or {@link Condition}
   */
  ConditionSpecification getConditionSpecification();

  long getLogicalStartTime();

  /**
   * @return a {@link WorkflowToken}
   */
  WorkflowToken getToken();

  /**
   * Return an immutable {@link Map} of node ids to {@link WorkflowNodeState}. This can be used
   * from {@link AbstractWorkflow#destroy} method to determine the status of all nodes
   * executed by the Workflow in the current run.
   */
  @Beta
  Map<String, WorkflowNodeState> getNodeStates();

  /**
   * Return the state of the workflow. This method can be used from {@link AbstractWorkflow#destroy}
   * to determine the status of the {@link Workflow}.
   * @return a {@link ProgramState}
   */
  @Beta
  ProgramState getState();

  /**
   * Call this method to disable emitting the field lineage operations from nodes running inside Workflow.
   * Disabling should happen from {@link AbstractWorkflow#initialize} method at which point no node
   * has been executed yet. Calling this method means Workflow is taking responsibility of emitting
   * the field operation. Field operations will be available to workflow as {@link WorkflowNodeState}
   * by calling {@link #getNodeStates} method.If workflow does not call {@link LineageRecorder#record}
   * method, then no field lineage will be recorded.
   */
  void disableFieldLineageOperationsEmitFromNodes();
}
