package com.continuuity.api.procedure;

import java.io.IOException;

/**
 *
 */
public interface ProcedureResponder {

  ProcedureResponse.Writer response(ProcedureResponse response) throws IOException;
}
