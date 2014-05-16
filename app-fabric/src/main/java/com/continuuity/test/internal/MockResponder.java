/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.test.internal;

import com.continuuity.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;

/**
 * A mock implementation of {@link HttpResponder} that only record the response status.
 */
public final class MockResponder implements HttpResponder {
  private HttpResponseStatus status = null;
  private ChannelBuffer content = null;
  private static final Gson GSON = new Gson();


  public HttpResponseStatus getStatus() {
    return status;
  }

  public <T> T decodeResponseContent(TypeToken<T> type) {
    JsonReader jsonReader = new JsonReader(new InputStreamReader
                                             (new ChannelBufferInputStream(content), Charsets.UTF_8));
    return GSON.fromJson(jsonReader, type.getType());
  }

  @Override
  public void sendJson(HttpResponseStatus status, Object object) {
    sendJson(status, object, object.getClass());
  }

  @Override
  public void sendJson(HttpResponseStatus status, Object object, Type type) {
    sendJson(status, object, type, GSON);
  }

  @Override
  public void sendJson(HttpResponseStatus status, Object object, Type type, Gson gson) {
    try {
      ChannelBuffer channelBuffer = ChannelBuffers.dynamicBuffer();
      JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(new ChannelBufferOutputStream(channelBuffer),
                                                                    Charsets.UTF_8));
      try {
        gson.toJson(object, type, jsonWriter);
      } finally {
        jsonWriter.close();
      }

      sendContent(status, channelBuffer, "application/json", ImmutableMultimap.<String, String>of());
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void sendString(HttpResponseStatus status, String data) {
    this.status = status;
  }

  @Override
  public void sendStatus(HttpResponseStatus status) {
    sendContent(status, null, null, ImmutableMultimap.<String, String>of());
  }

  @Override
  public void sendStatus(HttpResponseStatus status, Multimap<String, String> headers) {
    sendContent(status, null, null, headers);
  }

  @Override
  public void sendByteArray(HttpResponseStatus status, byte[] bytes, Multimap<String, String> headers) {
    ChannelBuffer channelBuffer = ChannelBuffers.wrappedBuffer(bytes);
    sendContent(status, channelBuffer, "application/octet-stream", headers);
  }

  @Override
  public void sendBytes(HttpResponseStatus status, ByteBuffer buffer, Multimap<String, String> headers) {
    sendContent(status, ChannelBuffers.wrappedBuffer(buffer), "application/octet-stream", headers);
  }

  @Override
  public void sendError(HttpResponseStatus status, String errorMessage) {
    Preconditions.checkArgument(!status.equals(HttpResponseStatus.OK), "Response status cannot be OK for errors");

    ChannelBuffer errorContent = ChannelBuffers.wrappedBuffer(Charsets.UTF_8.encode(errorMessage));
    sendContent(status, errorContent, "text/plain; charset=utf-8", ImmutableMultimap.<String, String>of());
  }

  @Override
  public void sendChunkStart(HttpResponseStatus status, Multimap<String, String> headers) {
    this.status = status;
  }

  @Override
  public void sendChunk(ChannelBuffer content) {
    // No-op
  }

  @Override
  public void sendChunkEnd() {
    // No-op
  }

  @Override
  public void sendContent(HttpResponseStatus status,
                          ChannelBuffer content, String contentType, Multimap<String, String> headers) {
    if (content != null) {
      this.content = content;
    }
    this.status = status;
  }

  @Override
  public void sendFile(File file, Multimap<String, String> headers) {
    this.status = HttpResponseStatus.OK;
  }
}
