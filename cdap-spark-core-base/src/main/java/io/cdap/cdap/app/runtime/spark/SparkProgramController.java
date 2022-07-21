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

package io.cdap.cdap.app.runtime.spark;

import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.spark.Spark;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.WorkflowDataProvider;
import io.cdap.cdap.internal.app.runtime.ProgramControllerServiceAdapter;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A {@link ProgramController} for {@link Spark} jobs. This class acts as an adapter for reflecting state changes
 * happening in {@link SparkRuntimeService}
 */
final class SparkProgramController extends ProgramControllerServiceAdapter implements WorkflowDataProvider {

  private final SparkRuntimeContext context;
  private final SparkRuntimeService sparkRuntimeService;

  SparkProgramController(SparkRuntimeService sparkRuntimeService, SparkRuntimeContext context) {
    super(sparkRuntimeService, context.getProgramRunId());
    this.context = context;
    this.sparkRuntimeService = sparkRuntimeService;
  }

  @Override
  protected void gracefulStop(long gracefulTimeoutMillis) {
    sparkRuntimeService.stop(gracefulTimeoutMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  public WorkflowToken getWorkflowToken() {
    WorkflowProgramInfo workflowProgramInfo = context.getWorkflowInfo();
    if (workflowProgramInfo == null) {
      throw new IllegalStateException("No workflow information for Spark program that is started by Workflow.");
    }
    return workflowProgramInfo.getWorkflowToken();
  }

  @Override
  public Set<Operation> getFieldLineageOperations() {
    return context.getFieldLineageOperations();
  }
}
