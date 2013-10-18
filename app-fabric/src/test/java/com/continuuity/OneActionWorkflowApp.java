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
