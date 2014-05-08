/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.test.internal;

import com.continuuity.http.HttpResponder;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.File;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;

/**
 * A mock implementation of {@link HttpResponder} that only record the response status.
 */
public final class MockResponder implements HttpResponder {
  private HttpResponseStatus status = null;

  public HttpResponseStatus getStatus() {
    return status;
  }

  @Override
  public void sendJson(HttpResponseStatus status, Object object) {
    this.status = status;
  }

  @Override
  public void sendJson(HttpResponseStatus status, Object object, Type type) {
    this.status = status;
  }

  @Override
  public void sendJson(HttpResponseStatus status, Object object, Type type, Gson gson) {
    this.status = status;
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
