package com.continuuity.examples.resourcespammer;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;

/**
 * Procedure designed to use up lots of cpu {@code CPUSpammer}.
 */
public class CPUSpamProcedure extends AbstractProcedure {
  private final Spammer spammer;

  public CPUSpamProcedure() {
    spammer = new Spammer(32);
  }

  @Handle("spam")
  public void spam(ProcedureRequest request, ProcedureResponder responder) throws Exception {
    long start = System.currentTimeMillis();
    spammer.spam();
    long duration = System.currentTimeMillis() - start;
    responder.sendJson(ProcedureResponse.Code.SUCCESS, duration);
  }
}
