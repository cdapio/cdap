/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.workflow.AbstractWorkflowAction;
import com.continuuity.api.workflow.Workflow;
import com.continuuity.api.workflow.WorkflowContext;
import com.continuuity.api.workflow.WorkflowSpecification;

/**
 *
 */
public class WorkflowApp implements Application {

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("WorkflowApp")
      .setDescription("WorkflowApp")
      .noStream()
      .noDataSet()
      .noFlow()
      .noProcedure()
      .noBatch()
      .withWorkflow().add(new FunWorkflow()).build();
  }


  public static class FunWorkflow implements Workflow {

    @Override
    public WorkflowSpecification configure() {
      return WorkflowSpecification.Builder.with()
        .setName("FunWorkflow")
        .setDescription("FunWorkflow description")
        .startWith(new DummyAction())
        .last(new DummyAction())
        .build();
    }
  }

  public static final class DummyAction extends AbstractWorkflowAction {

    @Override
    public void initialize(WorkflowContext context) throws Exception {
      super.initialize(context);
      System.out.println("Dummy action initialized: " + context.getSpecification().getName());
    }

    @Override
    public void destroy() {
      super.destroy();
      System.out.println("Dummy action destroyed: " + getContext().getSpecification().getName());
    }

    @Override
    public void run() {
      System.out.println("Dummy action run");
    }
  }
}
