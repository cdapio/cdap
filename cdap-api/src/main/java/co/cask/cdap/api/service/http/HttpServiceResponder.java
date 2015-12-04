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

package co.cask.cdap.api.service.http;

import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;

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
   * @param status status of the HTTP response
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
   * @param status status of the HTTP response
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
   * @param headers headers to be sent back
   * @deprecated Use {@link #sendStatus(int, Map)} or {@link #sendStatus(int, Iterable)} instead. This method
   *             will be removed in future release.
   */
  @Deprecated
  void sendStatus(int status, Multimap<String, String> headers);

  /**
   * Sends a status code and headers back to client without any content.
   *
   * @param status status of the HTTP response
   * @param headers headers to send
   */
  void sendStatus(int status, Map<String, String> headers);

  /**
   * Sends a status code and headers back to client without any content.
   *
   * @param status status of the HTTP response
   * @param headers headers to send; each {@link Map.Entry} contains the header name and value to be sent, hence
   *                multiple values for the same header name is allowed.
   */
  void sendStatus(int status, Iterable<? extends Map.Entry<String, String>> headers);

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
   * @param status status of the HTTP response
   * @param content content to be sent back
   * @param contentType type of content
   * @param headers headers to be sent back
   * @deprecated Use {@link #send(int, ByteBuffer, String, Map)} or {@link #send(int, ByteBuffer, String, Iterable)}
   *             instead. This method will be removed in future release.
   */
  @Deprecated
  void send(int status, ByteBuffer content, String contentType, Multimap<String, String> headers);

  /**
   * Sends response back to client.
   *
   * @param status status of the HTTP response
   * @param content content to be sent back
   * @param contentType type of content
   * @param headers headers to be sent back
   */
  void send(int status, ByteBuffer content, String contentType, Map<String, String> headers);

  /**
   * Sends response back to client.
   *
   * @param status status of the HTTP response
   * @param content content to be sent back
   * @param contentType type of content
   * @param headers headers to send; each {@link Map.Entry} contains the header name and value to be sent, hence
   *                multiple values for the same header name is allowed.
   */
  void send(int status, ByteBuffer content, String contentType, Iterable<? extends Map.Entry<String, String>> headers);

  /**
   * Sends response back to client with response body produced by the given {@link HttpContentProducer}.
   *
   * @param status status of the HTTP response
   * @param producer a {@link HttpContentProducer} to produce content to be sent back
   * @param contentType type of content
   */
  void send(int status, HttpContentProducer producer, String contentType);

  /**
   * Sends response back to client with response body produced by the given {@link HttpContentProducer}.
   *
   * @param status status of the HTTP response
   * @param producer a {@link HttpContentProducer} to produce content to be sent back
   * @param contentType type of content
   * @param headers headers to be sent back
   */
  void send(int status, HttpContentProducer producer, String contentType, Map<String, String> headers);

  /**
   * Sends response back to client with response body produced by the given {@link HttpContentProducer}.
   *
   * @param status status of the HTTP response
   * @param producer a {@link HttpContentProducer} to produce content to be sent back
   * @param contentType type of content
   * @param headers headers to send; each {@link Map.Entry} contains the header name and value to be sent, hence
   *                multiple values for the same header name is allowed.
   */
  void send(int status, HttpContentProducer producer, String contentType,
            Iterable<? extends Map.Entry<String, String>> headers);

  /**
   * Sends response back to client using content in the given {@link Location} as the response body.
   *
   * @param status status of the HTTP response
   * @param location location containing the response body
   * @param contentType type of content
   * @throws IOException if failed to open an {@link InputStream} from the given {@link Location}.
   */
  void send(int status, Location location, String contentType) throws IOException;

  /**
   * Sends response back to client using content in the given {@link Location} as the response body.
   *
   * @param status status of the HTTP response
   * @param location location containing the response body
   * @param contentType type of content
   * @param headers headers to be sent back
   * @throws IOException if failed to open an {@link InputStream} from the given {@link Location}.
   */
  void send(int status, Location location, String contentType, Map<String, String> headers) throws IOException;

  /**
   * Sends response back to client using content in the given {@link Location} as the response body.
   *
   * @param status status of the HTTP response
   * @param location location containing the response body
   * @param contentType type of content
   * @param headers headers to send; each {@link Map.Entry} contains the header name and value to be sent, hence
   *                multiple values for the same header name is allowed.
   * @throws IOException if failed to open an {@link InputStream} from the given {@link Location}.
   */
  void send(int status, Location location, String contentType,
            Iterable<? extends Map.Entry<String, String>> headers) throws IOException;
}
