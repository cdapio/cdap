package com.continuuity.test.internal;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data.stream.service.StreamHandler;
import com.continuuity.test.StreamWriter;
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

  private final String accountId;
  private final QueueName streamName;
  private final StreamHandler streamHandler;

  @Inject
  public DefaultStreamWriter(StreamHandler streamHandler,
                             @Assisted QueueName streamName,
                             @Assisted("accountId") String accountId) throws IOException {

    this.streamHandler = streamHandler;
    this.streamName = streamName;
    this.accountId = accountId;
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
                                                     "/v2/streams/" + streamName.getSimpleName());

    for (Map.Entry<String, String> entry : headers.entrySet()) {
      httpRequest.setHeader(streamName.getSimpleName() + "." + entry.getKey(), entry.getValue());
    }
    ChannelBuffer content = ChannelBuffers.wrappedBuffer(buffer);
    httpRequest.setContent(content);
    httpRequest.setHeader(HttpHeaders.Names.CONTENT_LENGTH, content.readableBytes());

    MockResponder responder = new MockResponder();
    try {
      streamHandler.enqueue(httpRequest, responder, streamName.getSimpleName());
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
