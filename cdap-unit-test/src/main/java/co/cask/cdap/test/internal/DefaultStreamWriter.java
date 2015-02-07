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

package co.cask.cdap.test.internal;

import co.cask.cdap.data.stream.service.StreamHandler;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.StreamWriter;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 *
 */
public final class DefaultStreamWriter implements StreamWriter {

  private final Id.Stream streamId;
  private final StreamHandler streamHandler;

  @Inject
  public DefaultStreamWriter(StreamHandler streamHandler,
                             @Assisted Id.Stream streamId) throws IOException {

    this.streamHandler = streamHandler;
    this.streamId = streamId;
  }

  @Override
  public void createStream() throws IOException {

    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                                                     "/v2/streams/" + streamId.getName());

    MockResponder responder = new MockResponder();
    try {
      streamHandler.create(httpRequest, responder, streamId.getName());
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, IOException.class);
      throw Throwables.propagate(e);
    }
    if (responder.getStatus() != HttpResponseStatus.OK) {
      throw new IOException("Failed to create stream. Status = " + responder.getStatus());
    }
  }

  @Override
  public void send(String content) throws IOException {
    send(Charsets.UTF_8.encode(content));
  }

  @Override
  public void send(byte[] content) throws IOException {
    send(content, 0, content.length);
  }

  @Override
  public void send(byte[] content, int off, int len) throws IOException {
    send(ByteBuffer.wrap(content, off, len));
  }

  @Override
  public void send(ByteBuffer buffer) throws IOException {
    send(ImmutableMap.<String, String>of(), buffer);
  }

  @Override
  public void send(Map<String, String> headers, String content) throws IOException {
    send(headers, Charsets.UTF_8.encode(content));
  }

  @Override
  public void send(Map<String, String> headers, byte[] content) throws IOException {
    send(headers, content, 0, content.length);
  }

  @Override
  public void send(Map<String, String> headers, byte[] content, int off, int len) throws IOException {
    send(headers, ByteBuffer.wrap(content, off, len));
  }

  @Override
  public void send(Map<String, String> headers, ByteBuffer buffer) throws IOException {
    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                                                     "/v2/streams/" + streamId.getName());

    for (Map.Entry<String, String> entry : headers.entrySet()) {
      httpRequest.setHeader(streamId.getName() + "." + entry.getKey(), entry.getValue());
    }
    ChannelBuffer content = ChannelBuffers.wrappedBuffer(buffer);
    httpRequest.setContent(content);
    httpRequest.setHeader(HttpHeaders.Names.CONTENT_LENGTH, content.readableBytes());

    MockResponder responder = new MockResponder();
    try {
      streamHandler.enqueue(httpRequest, responder, streamId.getName());
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, IOException.class);
      throw Throwables.propagate(e);
    }
    if (responder.getStatus() != HttpResponseStatus.OK) {
      throw new IOException("Failed to write to stream. Status = " + responder.getStatus());
    }
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
  }
}
