/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.api.service.http;

import com.google.common.collect.Multimap;
import com.google.gson.Gson;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 *
 */
public interface HttpServiceResponder {

  void sendJson(Object object);

  /**
   * Sends json response back to the client.
   *
   * @param status Status of the response.
   * @param object Object that will be serialized into Json and sent back as content.
   */
  void sendJson(int status, Object object);

  /**
   * Sends json response back to the client using the given gson object.
   *
   * @param status Status of the response.
   * @param object Object that will be serialized into Json and sent back as content.
   * @param type Type of object.
   * @param gson Gson object for serialization.
   */
  void sendJson(int status, Object object, Type type, Gson gson);

  /**
   * Send a UTF-8 encoded string response back to the http client with response status of 200 OK.
   * @param data string data to be sent back.
   */
  void sendString(String data);

  /**
   * Send a string response back to the http client.
   *
   * @param status status of the Http response.
   * @param data string data to be sent back.
   */
  void sendString(int status, String data, Charset charset);

  /**
   * Send only a status code back to client without any content.
   *
   * @param status status of the Http response.
   */
  void sendStatus(int status);

  /**
   * Send only a status code back to client without any content.
   *
   * @param status status of the Http response.
   * @param headers Headers to send.
   */
  void sendStatus(int status, Multimap<String, String> headers);

  /**
   * Sends error message back to the client.
   *
   * @param status Status of the response.
   * @param errorMessage Error message sent back to the client.
   */
  void sendError(int status, String errorMessage);

  /**
   * Send response back to client.
   *
   * @param status Status of the response.
   * @param content Content to be sent back.
   * @param contentType Type of content.
   * @param headers Headers to be sent back.
   */
  void send(int status, ByteBuffer content, String contentType, Multimap<String, String> headers);
}
