/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.runtime.WorkflowTokenProvider;
import co.cask.cdap.internal.app.runtime.ProgramControllerServiceAdapter;
import co.cask.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Service;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * A ProgramController for MapReduce. It mainly is an adapter for reflecting the state changes in
 * {@link MapReduceRuntimeService}.
 */
final class MapReduceProgramController extends ProgramControllerServiceAdapter implements WorkflowTokenProvider {

  private final BasicMapReduceContext context;

  MapReduceProgramController(Service mapReduceRuntimeService, BasicMapReduceContext context) {
    super(mapReduceRuntimeService, context.getProgramRunId());
    this.context = context;
  }

  @Override
  public WorkflowToken getWorkflowToken() {
    BasicWorkflowToken workflowTokenFromContext = context.getWorkflowToken();

    if (workflowTokenFromContext == null) {
      throw new IllegalStateException("WorkflowToken cannot be null when the " +
                                        "MapReduce program is started by Workflow.");
    }

    try {
      workflowTokenFromContext.setMapReduceCounters(((Job) context.getHadoopJob()).getCounters());
      return workflowTokenFromContext;
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
