package com.continuuity.gateway.handlers;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureSpecification;

/**
 * App to test Procedure API Handling.
 */
public class ProcedureTestApp implements Application {
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("ProcedureTestApp")
      .setDescription("App to test Procedure API Handling")
      .noStream()
      .noDataSet()
      .noFlow()
      .withProcedures()
      .add(new TestProcedure())
      .noMapReduce()
      .noWorkflow()
      .build();
  }

  /**
   * TestProcedure handler.
   */
  public static class TestProcedure extends AbstractProcedure {
    @Override
    public ProcedureSpecification configure() {
      return ProcedureSpecification.Builder.with()
        .setName("TestProcedure")
        .setDescription("Test Procedure")
        .build();
    }

    @SuppressWarnings("UnusedDeclaration")
    @Handle("TestMethod")
    public void testMethod1(ProcedureRequest request, ProcedureResponder responder) throws Exception {
      responder.sendJson(request.getArguments());
    }
  }
}
