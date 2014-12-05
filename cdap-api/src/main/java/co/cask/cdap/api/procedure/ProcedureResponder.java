/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.api.procedure;

import java.io.IOException;

/**
 * This interface defines a response generator. Caller should either call {@link #stream(ProcedureResponse)}
 * to stream data back to client or call {@link #sendJson(ProcedureResponse, Object)} to send json
 * object back to client.
 *
 * @deprecated As of version 2.6.0, replaced by {@link co.cask.cdap.api.service.http.HttpServiceResponder}
 */
@Deprecated
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
