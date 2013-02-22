/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.procedure;

import java.io.IOException;

/**
 * This interface defines a response generator. Caller should either call {@link #stream(ProcedureResponse)}
 * to stream data back to client or call {@link #sendJson(ProcedureResponse, Object)} to send json
 * object back to client.
 */
public interface ProcedureResponder {
  /**
   * Adds the response to be returned to the caller. Calling this method multiple times to the same
   * {@link ProcedureResponder} will return the same {@link ProcedureResponse.Writer} instance and the
   * latter submitted {@link ProcedureResponse} would be ignored.
   *
   * @param response A {@link ProcedureResponse} containing the response to be returned.
   * @return  An instance of {@link ProcedureResponse.Writer} for writing data of the response.
   * @throws IOException When there is issue sending the response to the callee.
   */
  ProcedureResponse.Writer stream(ProcedureResponse response) throws IOException;

  /**
   * Send a response with a json body.
   *
   * @param response A {@link ProcedureResponse} containing the response to be returned.
   * @param object An object to be serialized into json and send as response body.
   * @throws IOException When there is issue sending the response to the callee.
   */
  void sendJson(ProcedureResponse response, Object object) throws IOException;
}
