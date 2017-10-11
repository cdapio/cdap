/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.api.service.http.HttpContentProducer;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * An abstract implementation of {@link HttpServiceResponder} to simplify
 * concrete implementation of {@link HttpServiceResponder}.
 */
public abstract class AbstractHttpServiceResponder implements HttpServiceResponder {

  @Override
  public final void sendJson(Object object) {
    sendJson(HttpResponseStatus.OK.code(), object);
  }

  @Override
  public final void sendJson(int status, Object object) {
    sendJson(status, object, object.getClass(), new Gson());
  }

  @Override
  public final void sendJson(int status, Object object, Type type, Gson gson) {
    doSend(status, "application/json",
           Unpooled.copiedBuffer(gson.toJson(object, type), StandardCharsets.UTF_8), null, null);
  }

  @Override
  public final void sendString(String data) {
    sendString(HttpResponseStatus.OK.code(), data, StandardCharsets.UTF_8);
  }

  @Override
  public final void sendString(int status, String data, Charset charset) {
    doSend(status, "text/plain; charset=" + charset.name(), Unpooled.copiedBuffer(data, charset), null, null);
  }

  @Override
  public final void sendStatus(int status) {
    sendStatus(status, Collections.<String, String>emptyMap());
  }

  @Override
  public final void sendStatus(int status, Map<String, String> headers) {
    sendStatus(status, headers.entrySet());
  }

  @Override
  public final void sendStatus(int status, Iterable<? extends Map.Entry<String, String>> headers) {
    doSend(status, "text/plain", null, null, createHeaders(headers));
  }

  @Override
  public final void sendError(int status, String errorMessage) {
    sendString(status, errorMessage, Charsets.UTF_8);
  }

  @Override
  public final void send(int status, ByteBuffer content, String contentType, Map<String, String> headers) {
    send(status, content, contentType, headers.entrySet());
  }

  @Override
  public final void send(int status, ByteBuffer content, String contentType,
                   Iterable<? extends Map.Entry<String, String>> headers) {
    doSend(status, contentType, Unpooled.copiedBuffer(content), null, createHeaders(headers));
  }

  @Override
  public final void send(int status, Location location, String contentType) throws IOException {
    send(status, location, contentType, Collections.<String, String>emptyMap());
  }

  @Override
  public final void send(int status, Location location,
                         String contentType, Map<String, String> headers) throws IOException {
    send(status, location, contentType, headers.entrySet());
  }

  @Override
  public final void send(int status, Location location, String contentType,
                         Iterable<? extends Map.Entry<String, String>> headers) throws IOException {
    send(status, new LocationHttpContentProducer(location), contentType, headers);
  }

  @Override
  public final void send(int status, HttpContentProducer producer, String contentType) {
    send(status, producer, contentType, Collections.<String, String>emptyMap());
  }

  @Override
  public final void send(int status, HttpContentProducer producer, String contentType, Map<String, String> headers) {
    send(status, producer, contentType, headers.entrySet());
  }

  @Override
  public final void send(int status, HttpContentProducer producer, String contentType,
                         Iterable<? extends Map.Entry<String, String>> headers) {
    doSend(status, contentType, null, producer, createHeaders(headers));
  }

  /**
   * Sub-class to implement on how to send a response.
   *
   * @param status response status code
   * @param contentType response content type
   * @param content response body if not null
   * @param contentProducer {@link HttpContentProducer} for producing the response body if not null; if this
   *                        is non-null, content should be null.
   * @param headers response headers
   */
  protected abstract void doSend(int status, String contentType,
                                 @Nullable ByteBuf content,
                                 @Nullable HttpContentProducer contentProducer,
                                 @Nullable HttpHeaders headers);

  /**
   * Creates a {@link Multimap} from an {@link Iterable} of {@link Map.Entry}.
   */
  protected final HttpHeaders createHeaders(Iterable<? extends Map.Entry<String, String>> entries) {
    DefaultHttpHeaders headers = new DefaultHttpHeaders();
    for (Map.Entry<String, String> entry : entries) {
      headers.add(entry.getKey(), entry.getValue());
    }
    return headers;
  }
}
