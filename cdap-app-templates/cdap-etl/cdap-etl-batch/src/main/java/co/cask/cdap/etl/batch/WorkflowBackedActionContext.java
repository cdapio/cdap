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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowNodeState;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.etl.api.batch.BatchActionContext;
import co.cask.cdap.etl.common.BasicArguments;
import co.cask.cdap.etl.common.DatasetContextLookupProvider;
import co.cask.cdap.etl.spec.StageSpec;

import java.util.Map;

/**
 * Implementation of {@link BatchActionContext} within a CDAP workflow.
 */
public class WorkflowBackedActionContext extends AbstractBatchContext implements BatchActionContext {
  private final WorkflowContext workflowContext;

  public WorkflowBackedActionContext(WorkflowContext workflowContext,
                                     Metrics metrics,
                                     StageSpec stageSpec,
                                     BasicArguments arguments) {
    super(workflowContext, metrics, new DatasetContextLookupProvider(workflowContext),
          workflowContext.getLogicalStartTime(), workflowContext.getAdmin(), stageSpec, arguments);
    this.workflowContext = workflowContext;
  }

  @Override
  public WorkflowToken getToken() {
    return workflowContext.getToken();
  }

  @Override
  public Map<String, WorkflowNodeState> getNodeStates() {
    return workflowContext.getNodeStates();
  }

  @Override
  public boolean isSuccessful() {
    return workflowContext.getState().getStatus() == ProgramStatus.COMPLETED;
  }

  @Override
  public <T> T getHadoopJob() {
    throw new UnsupportedOperationException("Deprecated getHadoopJob() method is not supported.");
  }
}
