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

package co.cask.cdap.internal.app.runtime.service.http;

import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.http.HttpResponder;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 *
 */
final class DefaultHttpServiceResponder implements HttpServiceResponder {

  private final HttpResponder responder;

  DefaultHttpServiceResponder(HttpResponder responder) {
    this.responder = responder;
  }

  @Override
  public void sendJson(Object object) {
    responder.sendJson(HttpResponseStatus.OK, object);
  }

  @Override
  public void sendJson(int status, Object object) {
    responder.sendJson(HttpResponseStatus.valueOf(status), object);
  }

  @Override
  public void sendJson(int status, Object object, Type type, Gson gson) {
    responder.sendJson(HttpResponseStatus.valueOf(status), object, type, gson);
  }

  @Override
  public void sendString(String data) {
    responder.sendString(HttpResponseStatus.OK, data);
  }

  @Override
  public void sendString(int status, String data, Charset charset) {
    responder.sendContent(HttpResponseStatus.valueOf(status), ChannelBuffers.wrappedBuffer(charset.encode(data)),
                          "text/plain; charset=" + charset.name(), ImmutableMultimap.<String, String>of());
  }

  @Override
  public void sendStatus(int status) {
    responder.sendStatus(HttpResponseStatus.valueOf(status));
  }

  @Override
  public void sendStatus(int status, Multimap<String, String> headers) {
    responder.sendStatus(HttpResponseStatus.valueOf(status), headers);
  }

  @Override
  public void sendError(int status, String errorMessage) {
    responder.sendError(HttpResponseStatus.valueOf(status), errorMessage);
  }

  @Override
  public void send(int status, ByteBuffer content, String contentType, Multimap<String, String> headers) {
    responder.sendContent(HttpResponseStatus.valueOf(status),
                          ChannelBuffers.wrappedBuffer(content), contentType, headers);
  }
}
