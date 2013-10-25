package com.continuuity.workflows;

import com.continuuity.api.workflow.Workflow;
import com.continuuity.api.workflow.WorkflowSpecification;

/**
 * Alert workflow to find alerts based on rules.
 */
public class AlertWorkflow implements Workflow {

  @Override
  public WorkflowSpecification configure() {
    return WorkflowSpecification.Builder.with()
                                        .setName("AlertWorkflow")
                                        .setDescription("Workflow to find alerts")
                                        .onlyWith(new AlertFinder())
                                        .build();
  }
}
