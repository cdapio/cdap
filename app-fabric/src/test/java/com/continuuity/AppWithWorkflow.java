package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.workflow.AbstractWorkflowAction;
import com.continuuity.api.workflow.Workflow;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * App with workflow.
 */
public class AppWithWorkflow implements Application {

  @Override
  public ApplicationSpecification configure() {
    try {
      return ApplicationSpecification.Builder.with()
        .setName("AppWithWorkflow")
        .setDescription("Sample application")
        .noStream()
        .withDataSets()
        .add(new ObjectStore<String>("input", String.class))
        .add(new ObjectStore<String>("output", String.class))
        .noFlow()
        .noProcedure()
        .noMapReduce()
        .withWorkflows()
        .add(new SampleWorkflow())
        .build();
    } catch (UnsupportedTypeException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Sample workflow. has a dummy action.
   */
  public static class SampleWorkflow implements Workflow {

    @Override
    public WorkflowSpecification configure() {
      return WorkflowSpecification.Builder.with()
        .setName("SampleWorkflow")
        .setDescription("SampleWorkflow description")
        .startWith(new DummyAction())
        .last(new DummyAction())
        .build();
    }
  }

  /**
   * DummyAction
   */
  public static class DummyAction extends AbstractWorkflowAction {
    private static final Logger LOG = LoggerFactory.getLogger(DummyAction.class);
    @Override
    public void run() {
      LOG.info("Ran dummy action");
    }
  }
}

