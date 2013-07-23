/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.procedure;

import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;

import java.io.IOException;

/**
 * An abstract base class for implementing different {@link ProcedureResponder}.
 */
public abstract class AbstractProcedureResponder implements ProcedureResponder {

  @Override
  public final void sendJson(Object object) throws IOException {
    sendJson(ProcedureResponse.Code.SUCCESS, object);
  }

  @Override
  public final void sendJson(ProcedureResponse.Code code, Object object) throws IOException {
    sendJson(new ProcedureResponse(code), object);
  }
}
