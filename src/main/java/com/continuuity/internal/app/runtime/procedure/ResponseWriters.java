package com.continuuity.internal.app.runtime.procedure;

import com.continuuity.api.procedure.ProcedureResponse;

/**
 *
 */
final class ResponseWriters {

  public static final ProcedureResponse.Writer CLOSED_WRITER = new ClosedResponseWriter();

  private ResponseWriters() {}

}
