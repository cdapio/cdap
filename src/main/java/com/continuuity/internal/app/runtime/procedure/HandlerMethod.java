package com.continuuity.internal.app.runtime.procedure;

import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;

/**
 *
 */
interface HandlerMethod {

  void handle(ProcedureRequest request, ProcedureResponder responder);
}
