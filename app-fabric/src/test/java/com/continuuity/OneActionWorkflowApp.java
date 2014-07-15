/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.workflow.AbstractWorkflowAction;
import com.continuuity.api.workflow.Workflow;
import com.continuuity.api.workflow.WorkflowSpecification;

/**
 *
 */
public class OneActionWorkflowApp implements Application {

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder
                                   .with()
                                   .setName("OneActionWorkflowApp")
                                   .setDescription("Workflow with a single action")
                                   .noStream()
                                   .noDataSet()
                                   .noFlow()
                                   .noProcedure()
                                   .noMapReduce()
                                   .withWorkflows()
                                      .add(new OneActionWorkflow())
                                   .build();
  }

  /**
   *
   */
  private static class OneActionWorkflow implements Workflow {

    @Override
    public WorkflowSpecification configure() {
     return WorkflowSpecification.Builder.with()
                                         .setName("OneActionWorkflow")
                                         .setDescription("One action workflow")
                                         .onlyWith(new EmptyAction())
                                         .build();
    }
  }

  /**
   *
   */
  private static class EmptyAction extends AbstractWorkflowAction {
    @Override
    public void run() {
    }
  }
}
