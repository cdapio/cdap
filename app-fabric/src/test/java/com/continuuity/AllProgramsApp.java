package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.mapreduce.MapReduce;
import com.continuuity.api.mapreduce.MapReduceContext;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.workflow.AbstractWorkflowAction;
import com.continuuity.api.workflow.Workflow;
import com.continuuity.api.workflow.WorkflowSpecification;

import java.io.IOException;

/**
 * App that contains all program types. Used to test Metadata store.
 */
public class AllProgramsApp implements Application {

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("App")
      .setDescription("Application which has everything")
      .withStreams()
        .add(new Stream("stream"))
      .withDataSets()
        .add(new KeyValueTable("kvt"))
      .withFlows()
        .add(new NoOpFlow())
      .withProcedures()
        .add(new NoOpProcedure())
      .withMapReduce()
        .add(new NoOpMR())
      .withWorkflows()
        .add(new NoOpWorkflow())
      .build();
  }

  /**
   *
   */
  public static class NoOpFlow implements Flow {
    @Override
    public FlowSpecification configure() {
     return FlowSpecification.Builder.with()
        .setName("NoOpFlow")
        .setDescription("NoOpflow")
        .withFlowlets()
          .add(new A())
        .connect()
          .fromStream("stream").to("A")
        .build();
    }
  }

  /**
   *
   */
  public static final class A extends AbstractFlowlet {
    public A() {
      super("A");
    }
  }

  /**
   *
   */
  private static class NoOpProcedure extends AbstractProcedure {
    @UseDataSet("kvt")
    private KeyValueTable counters;

    @Handle("dummy")
    public void handle(ProcedureRequest request,
                       ProcedureResponder responder)
      throws IOException {
      responder.sendJson("OK");
    }
  }

  /**
   *
   */
  public static class NoOpMR implements MapReduce {
    @Override
    public MapReduceSpecification configure() {
      return MapReduceSpecification.Builder.with()
        .setName("NoOpMR")
        .setDescription("NoOp Mapreduce")
        .build();
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
    }

    @Override
    public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    }
  }

  /**
   *
   */
  private static class NoOpWorkflow implements Workflow {
    @Override
    public WorkflowSpecification configure() {
      return WorkflowSpecification.Builder.with()
        .setName("NoOpWorkflow")
        .setDescription("NoOp workflow description")
        .onlyWith(new NoOpAction())
        .build();
    }
  }

  /**
   *
   */
  private static class NoOpAction extends AbstractWorkflowAction {
    @Override
    public void run() {

    }
  }

}
