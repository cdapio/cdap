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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import org.apache.twill.filesystem.Location;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * An abstract implementation of {@link HttpServiceResponder} to simplify
 * concrete implementation of {@link HttpServiceResponder}.
 */
public abstract class AbstractHttpServiceResponder implements HttpServiceResponder {

  @Override
  public final void sendJson(Object object) {
    sendJson(HttpResponseStatus.OK.getCode(), object);
  }

  @Override
  public final void sendJson(int status, Object object) {
    sendJson(status, object, object.getClass(), new Gson());
  }

  @Override
  public final void sendJson(int status, Object object, Type type, Gson gson) {
    doSend(status, "application/json",
           ChannelBuffers.wrappedBuffer(Charsets.UTF_8.encode(gson.toJson(object, type))), null, null);
  }

  @Override
  public final void sendString(String data) {
    sendString(HttpResponseStatus.OK.getCode(), data, Charsets.UTF_8);
  }

  @Override
  public final void sendString(int status, String data, Charset charset) {
    doSend(status, "text/plain; charset=" + charset.name(),
           ChannelBuffers.wrappedBuffer(charset.encode(data)), null, null);
  }

  @Override
  public final void sendStatus(int status) {
    sendStatus(status, ImmutableMap.<String, String>of());
  }

  @Override
  public final void sendStatus(int status, Multimap<String, String> headers) {
    doSend(status, "text/plain", null, null, headers);
  }

  @Override
  public final void sendStatus(int status, Map<String, String> headers) {
    sendStatus(status, headers.entrySet());
  }

  @Override
  public final void sendStatus(int status, Iterable<? extends Map.Entry<String, String>> headers) {
    sendStatus(status, createMultimap(headers));
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
    send(status, content, contentType, createMultimap(headers));
  }

  @Override
  public final void send(int status, ByteBuffer content, String contentType, Multimap<String, String> headers) {
    doSend(status, contentType, ChannelBuffers.copiedBuffer(content), null, headers);
  }

  @Override
  public final void send(int status, Location location, String contentType) throws IOException {
    send(status, location, contentType, ImmutableMap.<String, String>of());
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
    send(status, producer, contentType, ImmutableMap.<String, String>of());
  }

  @Override
  public final void send(int status, HttpContentProducer producer, String contentType, Map<String, String> headers) {
    send(status, producer, contentType, headers.entrySet());
  }

  @Override
  public final void send(int status, HttpContentProducer producer, String contentType,
                         Iterable<? extends Map.Entry<String, String>> headers) {
    doSend(status, contentType, null, producer, createMultimap(headers));
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
                                 @Nullable ChannelBuffer content,
                                 @Nullable HttpContentProducer contentProducer,
                                 @Nullable Multimap<String, String> headers);

  /**
   * Creates a {@link Multimap} from an {@link Iterable} of {@link Map.Entry}.
   */
  protected final <K, V> Multimap<K, V> createMultimap(Iterable<? extends Map.Entry<K, V>> entries) {
    ImmutableMultimap.Builder<K, V> builder = ImmutableMultimap.builder();
    for (Map.Entry<K, V> entry : entries) {
      builder.put(entry);
    }
    return builder.build();
  }
}
