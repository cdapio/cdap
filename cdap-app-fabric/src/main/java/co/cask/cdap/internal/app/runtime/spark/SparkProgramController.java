/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.WorkflowTokenProvider;
import co.cask.cdap.internal.app.runtime.ProgramControllerServiceAdapter;
import com.google.common.util.concurrent.Service;

/**
 * A {@link ProgramController} for {@link Spark} jobs. This class acts as an adapter for reflecting state changes
 * happening in {@link SparkRuntimeService}
 */
final class SparkProgramController extends ProgramControllerServiceAdapter implements WorkflowTokenProvider {

  private final SparkContext context;

  SparkProgramController(Service sparkRuntimeService, AbstractSparkContext context) {
    super(sparkRuntimeService, context.getProgramId(), context.getRunId());
    this.context = context;
  }

  @Override
  public WorkflowToken getWorkflowToken() {
    WorkflowToken workflowToken = context.getWorkflowToken();
    if (workflowToken == null) {
      throw new IllegalStateException("WorkflowToken cannot be null when the " +
                                        "Spark program is started by Workflow.");
    }

    return workflowToken;
  }
}
