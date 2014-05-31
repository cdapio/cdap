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
   * {@link ProcedureResponder} will return the same {@link ProcedureResponse.Writer} instance and any
   * newer {@link ProcedureResponse} will be ignored.
   *
   * @param response A {@link ProcedureResponse} containing the response to be returned.
   * @return  An instance of {@link ProcedureResponse.Writer} for writing data of the response.
   * @throws IOException When there is an issue sending the response to the callee.
   */
  ProcedureResponse.Writer stream(ProcedureResponse response) throws IOException;

  /**
   * Sends a response with a json body.
   *
   * @param response A {@link ProcedureResponse} containing the response to be returned.
   * @param object An object to be serialized into json and sent as response body.
   * @throws IOException When there is issue sending the response to the callee.
   */
  void sendJson(ProcedureResponse response, Object object) throws IOException;

  /**
   * Sends a response with the given response code and a json body.
   *
   * @param code The response code to be returned.
   * @param object An object to be serialized into json and sent as response body.
   * @throws IOException When there is issue sending the response to the callee.
   */
  void sendJson(ProcedureResponse.Code code, Object object) throws IOException;

  /**
   * Sends a success response ({@link ProcedureResponse.Code#SUCCESS}) with a json body.
   *
   * @param object An object to be serialized into json and sent as response body.
   * @throws IOException When there is issue sending the response to the callee.
   */
  void sendJson(Object object) throws IOException;

  /**
   * Sends an error response to client with the given error message.
   *
   * @param errorCode The error code
   * @param errorMessage The error message sent back to client. If it is {@code null}, no error message will be sent.
   * @throws IOException When there is an issue sending the response to the callee.
   */
  void error(ProcedureResponse.Code errorCode, String errorMessage) throws IOException;
}
