package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.Tick;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.FlowletException;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureContext;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.continuuity.api.procedure.ProcedureSpecification;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Flow and Procedure that checks if arguments
 * are passed correctly. Only used for checking args functionality.
 */
public class ArgumentCheckApp implements Application {

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("ArgumentCheckApp")
      .setDescription("Checks if arguments are passed correctly")
      .noStream()
      .noDataSet()
      .withFlows()
        .add(new SimpleFlow())
      .withProcedures()
        .add(new SimpleProcedure())
      .noMapReduce()
      .noWorkflow()
      .build();
  }

  private class SimpleFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("SimpleFlow")
        .setDescription("Uses user passed value")
        .withFlowlets()
          .add(new SimpleGeneratorFlowlet())
          .add(new SimpleConsumerFlowlet())
        .connect()
          .from(new SimpleGeneratorFlowlet()).to(new SimpleConsumerFlowlet())
        .build();
    }
  }

  private class SimpleGeneratorFlowlet extends AbstractFlowlet {
    private FlowletContext context;
    OutputEmitter<String> out;

    @Override
    public void initialize(FlowletContext context) throws FlowletException {
      this.context = context;
    }

    @Tick(delay = 1L, unit = TimeUnit.NANOSECONDS)
    public void generate() throws Exception {
      String arg = context.getRuntimeArguments().get("arg");
      if (!context.getRuntimeArguments().containsKey("arg") ||
          !context.getRuntimeArguments().get("arg").equals("test")) {
        throw new IllegalArgumentException("User runtime argument functionality not working");
      }
      out.emit(arg);
    }
  }

  private class SimpleConsumerFlowlet extends AbstractFlowlet {

    @ProcessInput
    public void process(String arg) {
      if (!arg.equals("test")) {
        throw new IllegalArgumentException("User argument from prev flowlet not passed");
      }
    }

    @ProcessInput
    public void process(int i) {
      // A dummy process method that has no matching upstream.
    }
  }

  private class SimpleProcedure extends AbstractProcedure {
    private ProcedureContext context;

    @Override
    public ProcedureSpecification configure() {
      return ProcedureSpecification.Builder.with()
        .setName("SimpleProcedure")
        .setDescription(getDescription())
        .build();
    }

    @Override
    public void initialize(ProcedureContext context) {
      this.context = context;
      if (!context.getRuntimeArguments().containsKey("arg")) {
        throw new IllegalArgumentException("User runtime argument fuctionality not working.");
      }
    }

    @Handle("argtest")
    public void handle(ProcedureRequest request, ProcedureResponder responder) throws IOException {
      // Don't need to do much here. As we want to test if the context carries runtime arguments.
      responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS),
                         context.getSpecification().getProperties());
    }
  }
}
