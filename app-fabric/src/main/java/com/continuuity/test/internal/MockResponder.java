/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.test.internal;

import com.continuuity.http.HttpResponder;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.File;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * A mock implementation of {@link HttpResponder} that only record the response status.
 */
public final class MockResponder implements HttpResponder {
  private HttpResponseStatus status = null;
  private String response = null;
  private static final Gson GSON = new Gson();

  public HttpResponseStatus getStatus() {
    return status;
  }

  public String getResponse() {
    return response;
  }

  @Override
  public void sendJson(HttpResponseStatus status, Object object) {
    sendJson(status, object, null, null);
  }

  @Override
  public void sendJson(HttpResponseStatus status, Object object, Type type) {
    sendJson(status, object, type, null);
  }

  @Override
  public void sendJson(HttpResponseStatus status, Object object, Type type, Gson gson) {
    this.status = status;
    Map<String, String> o = GSON.fromJson(object.toString(), new TypeToken<Map<String, String>>() { }.getType());
    this.response = o.get("status");
  }

  @Override
  public void sendString(HttpResponseStatus status, String data) {
    this.status = status;
  }

  @Override
  public void sendStatus(HttpResponseStatus status) {
    this.status = status;
  }

  @Override
  public void sendStatus(HttpResponseStatus status, Multimap<String, String> headers) {
    this.status = status;
  }

  @Override
  public void sendByteArray(HttpResponseStatus status, byte[] bytes, Multimap<String, String> headers) {
    this.status = status;
  }

  @Override
  public void sendBytes(HttpResponseStatus status, ByteBuffer buffer, Multimap<String, String> headers) {
    this.status = status;
  }

  @Override
  public void sendError(HttpResponseStatus status, String errorMessage) {
    this.status = status;
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
    this.status = status;
  }

  @Override
  public void sendFile(File file, Multimap<String, String> headers) {
    this.status = HttpResponseStatus.OK;
  }
}
