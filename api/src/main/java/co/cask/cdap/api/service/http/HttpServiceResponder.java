/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.api.service.http;

import com.google.common.collect.Multimap;
import com.google.gson.Gson;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Interface with methods for sending HTTP responses.
 */
public interface HttpServiceResponder {

  /**
   * Sends JSON response back to the client with a default status.
   *
   * @param object object that will be serialized into JSON and sent back as content
   */
  void sendJson(Object object);

  /**
   * Sends JSON response back to the client.
   *
   * @param status status of the HTTP response
   * @param object object that will be serialized into JSON and sent back as content
   */
  void sendJson(int status, Object object);

  /**
   * Sends JSON response back to the client using the given {@link Gson} object.
   *
   * @param status the status of the HTTP response
   * @param object the object that will be serialized into JSON and sent back as content
   * @param type the type of object
   * @param gson the Gson object for serialization
   */
  void sendJson(int status, Object object, Type type, Gson gson);

  /**
   * Sends a UTF-8 encoded string response back to the HTTP client with a default response status.
   *
   * @param data the string data to be sent back
   */
  void sendString(String data);

  /**
   * Sends a string response back to the HTTP client.
   *
   * @param status the status of the HTTP response
   * @param data the data to be sent back
   * @param charset the Charset used to encode the string
   */
  void sendString(int status, String data, Charset charset);

  /**
   * Sends only a status code back to client without any content.
   *
   * @param status status of the HTTP response
   */
  void sendStatus(int status);

  /**
   * Sends a status code and headers back to client without any content.
   *
   * @param status status of the HTTP response
   * @param headers headers to send
   */
  void sendStatus(int status, Multimap<String, String> headers);

  /**
   * Sends an error message back to the client with the specified status code.
   *
   * @param status status of the HTTP response
   * @param errorMessage error message sent back to the client
   */
  void sendError(int status, String errorMessage);

  /**
   * Sends response back to client.
   *
   * @param status status of the response
   * @param content content to be sent back
   * @param contentType type of content
   * @param headers headers to be sent back
   */
  void send(int status, ByteBuffer content, String contentType, Multimap<String, String> headers);
}
