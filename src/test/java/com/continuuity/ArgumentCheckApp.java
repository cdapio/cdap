package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.AbstractGeneratorFlowlet;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.FlowletException;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureContext;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;

import java.io.IOException;

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
      .noBatch()
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

  private class SimpleGeneratorFlowlet extends AbstractGeneratorFlowlet {
    private FlowletContext context;
    OutputEmitter<String> out;

    @Override
    public void initialize(FlowletContext context) throws FlowletException {
      this.context = context;
    }

    @Override
    public void generate() throws Exception {
      String arg = context.getSpecification().getArguments().get("arg");
      if (!context.getSpecification().getArguments().containsKey("arg") ||
          !context.getSpecification().getArguments().get("arg").equals("test")) {
        throw new IllegalArgumentException("User argument functionality not working");
      }
      out.emit(arg);
    }
  }

  private class SimpleConsumerFlowlet extends AbstractFlowlet {
    public void process(String arg) {
      if(!arg.equals("test")) {
        throw new IllegalArgumentException("User argument from prev flowlet not passed");
      }
    }
  }

  private class SimpleProcedure extends AbstractProcedure {
    private ProcedureContext context;

    @Override
    public void initialize(ProcedureContext context) {
      this.context = context;
    }

    @Handle("arg")
    public void handle(ProcedureRequest request, ProcedureResponder responder) throws OperationException, IOException {
      responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS),
                         context.getSpecification().getArguments());
    }
  }


}
