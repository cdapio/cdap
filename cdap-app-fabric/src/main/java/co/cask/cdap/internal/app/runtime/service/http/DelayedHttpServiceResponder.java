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

package co.cask.cdap.internal.app.runtime.service.http;

import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.gson.Gson;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Implementation of {@link HttpServiceResponder} which delegates calls to
 * the HttpServiceResponder's methods to the matching methods for a {@link HttpResponder}.
 * A response is buffered until execute() is called. This allows you to send the correct response upon
 * a transaction failure, and to not always delegating to the user response.
 */
public final class DelayedHttpServiceResponder implements HttpServiceResponder {
  private static final Logger LOG = LoggerFactory.getLogger(DelayedHttpServiceResponder.class);
  private static final Gson GSON = new Gson();
  private final HttpResponder responder;
  private final MetricsCollector metricsCollector;
  private BufferedResponse bufferedResponse;

  /**
   * Instantiates the class from a {@link HttpResponder}
   *
   * @param responder the responder which will be bound to
   */
  public DelayedHttpServiceResponder(HttpResponder responder, MetricsCollector metricsCollector) {
    this.responder = responder;
    this.metricsCollector = metricsCollector;
  }

  /**
   * Sends JSON response back to the client with status code 200 OK.
   *
   * @param object the object that will be serialized into JSON and sent back as content
   */
  @Override
  public void sendJson(Object object) {
    sendJson(HttpResponseStatus.OK.getCode(), object);
  }

  /**
   * Sends JSON response back to the client.
   *
   * @param status the status of the HTTP response
   * @param object the object that will be serialized into JSON and sent back as content
   */
  @Override
  public void sendJson(int status, Object object) {
    sendJson(status, object, object.getClass(), GSON);
  }

   /**
    * Sends JSON response back to the client using the given {@link Gson} object.
    *
    * @param status the status of the HTTP response
    * @param object the object that will be serialized into JSON and sent back as content
    * @param type the type of object
    * @param gson the Gson object for serialization
   */
  @Override
  public void sendJson(int status, Object object, Type type, Gson gson) {
    send(status, Charsets.UTF_8.encode(gson.toJson(object, type)), "application/json", null, false);
  }

  /**
   * Sends a UTF-8 encoded string response back to the HTTP client with a default response status.
   *
   * @param data the data to be sent back
   */
  @Override
  public void sendString(String data) {
    sendString(HttpResponseStatus.OK.getCode(), data, Charsets.UTF_8);
  }

  /**
   * Sends a string response back to the HTTP client.
   *
   * @param status the status of the HTTP response
   * @param data the data to be sent back
   * @param charset the Charset used to encode the string
   */
  @Override
  public void sendString(int status, String data, Charset charset) {
    send(status, charset.encode(data), "text/plain; charset=" + charset.name(), null, false);
  }

  /**
   * Sends only a status code back to the client without any content.
   *
   * @param status the status of the HTTP response
   */
  @Override
  public void sendStatus(int status) {
    sendStatus(status, null);
  }

  /**
   * Sends a status code and headers back to client without any content.
   *
   * @param status the status of the HTTP response
   * @param headers the headers to send
   */
  @Override
  public void sendStatus(int status, Multimap<String, String> headers) {
    send(status, null, null, headers, false);
  }

  /**
   * Sends error message back to the client with the specified status code.
   *
   * @param status the status of the response
   * @param errorMessage the error message sent back to the client
   */
  @Override
  public void sendError(int status, String errorMessage) {
    sendString(status, errorMessage, Charsets.UTF_8);
  }

  /**
   * Sends response back to client.
   *
   * @param status the status of the response
   * @param content the content to be sent back
   * @param contentType the type of content
   * @param headers the headers to be sent back
   */
  @Override
  public void send(int status, ByteBuffer content, String contentType, Multimap<String, String> headers) {
    send(status, content, contentType, headers, true);
  }

  private void send(int status, ByteBuffer content, String contentType,
                    Multimap<String, String> headers, boolean copy) {
    if (bufferedResponse != null) {
      LOG.warn("Multiple calls to one of the 'send*' methods has been made. Only the last response will be sent.");
    }

    ChannelBuffer channelBuffer = null;
    if (content != null) {
      channelBuffer = copy ? ChannelBuffers.copiedBuffer(content) : ChannelBuffers.wrappedBuffer(content);
    }

    bufferedResponse = new BufferedResponse(status, channelBuffer, contentType, headers);
  }

  /**
   * Since calling one of the send methods multiple times logs a warning, upon transaction failures this
   * method is called to allow setting the failure response without an additional warning.
   */
  public void setTransactionFailureResponse() {
    ByteBuffer buffer = Charsets.UTF_8.encode("Transaction failure when committing changes. Aborted transaction.");
    bufferedResponse = new BufferedResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode(),
                                            ChannelBuffers.wrappedBuffer(buffer),
                                            "text/plain; charset=" + Charsets.UTF_8.name(), null);
  }

  /**
   * Calls to other responder methods in this class only cache the response to be sent. The response is actually
   * sent only when this method is called.
   */
  public void execute() {
    Preconditions.checkState(bufferedResponse != null,
                             "Can not call execute before one of the other responder methods are called.");
    responder.sendContent(HttpResponseStatus.valueOf(bufferedResponse.getStatus()), bufferedResponse.getChannelBuffer(),
                          bufferedResponse.getContentType(), bufferedResponse.getHeaders());
    emitMetrics(bufferedResponse.getStatus());
  }

  private void emitMetrics(int status) {
    StringBuilder builder = new StringBuilder(50);
    builder.append("response.");
    if (status < 100) {
      builder.append("unknown");
    } else if (status < 200) {
      builder.append("information");
    } else if (status < 300) {
      builder.append("successful");
    } else if (status < 400) {
      builder.append("redirect");
    } else if (status < 500) {
      builder.append("client.error");
    } else if (status < 600) {
      builder.append("server.error");
    } else {
      builder.append("unknown");
    }
    builder.append(".count");

    metricsCollector.increment(builder.toString(), 1);
    metricsCollector.increment("requests.count", 1);
  }

  private static final class BufferedResponse {

    private final int status;
    private final ChannelBuffer channelBuffer;
    private final String contentType;
    private final Multimap<String, String> headers;

    private BufferedResponse(int status, ChannelBuffer channelBuffer,
                             String contentType, Multimap<String, String> headers) {
      this.status = status;
      this.channelBuffer = channelBuffer;
      this.contentType = contentType;
      this.headers = headers == null ? null : Multimaps.unmodifiableMultimap(headers);
    }

    public int getStatus() {
      return status;
    }

    public ChannelBuffer getChannelBuffer() {
      return channelBuffer;
    }

    public String getContentType() {
      return contentType;
    }

    public Multimap<String, String> getHeaders() {
      return headers;
    }
  }
}
