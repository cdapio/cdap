/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.http.core;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.File;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * InternalHttpResponder is used when a handler is being called internally by some other handler, and thus there
 * is no need to go through the network.  It stores the status code and content in memory and returns them when asked.
 */
public class InternalHttpResponder implements HttpResponder {
  private int statusCode;
  private byte[] body;
  private File file;
  private List<byte[]> contentChunks;
  private int totalChunkedSize;

  private final ThreadLocal<Gson> gson = new ThreadLocal<Gson>() {
    @Override
    protected Gson initialValue() {
      return new Gson();
    }
  };

  public InternalHttpResponder() {
    contentChunks = Lists.newLinkedList();
    totalChunkedSize = 0;
    statusCode = 0;
    file = null;
  }

  @Override
  public void sendJson(HttpResponseStatus status, Object object){
    sendJson(status, object, object.getClass());
  }

  @Override
  public void sendJson(HttpResponseStatus status, Object object, Type type){
    sendJson(status, object, type, gson.get());
  }

  @Override
  public void sendJson(HttpResponseStatus status, Object object, Type type, Gson gson) {
    setResponseContent(status, gson.toJson(object, type).getBytes(Charsets.UTF_8));
  }

  @Override
  public void sendString(HttpResponseStatus status, String data){
    if (data == null) {
      sendStatus(status);
    } else {
      setResponseContent(status, data.getBytes(Charsets.UTF_8));
    }
  }

  @Override
  public void sendStatus(HttpResponseStatus status) {
    statusCode = status.getCode();
  }

  @Override
  public void sendByteArray(HttpResponseStatus status, byte[] bytes, Multimap<String, String> headers) {
    setResponseContent(status, bytes);
  }

  @Override
  public void sendBytes(HttpResponseStatus status, ByteBuffer buffer, Multimap<String, String> headers) {
    setResponseContent(status, buffer.array());
  }

  @Override
  public void sendError(HttpResponseStatus status, String errorMessage){
    Preconditions.checkArgument(!status.equals(HttpResponseStatus.OK), "Response status cannot be OK for errors");

    setResponseContent(status, errorMessage.getBytes(Charsets.UTF_8));
  }

  @Override
  public void sendChunkStart(HttpResponseStatus status, Multimap<String, String> headers) {
    statusCode = status.getCode();
    contentChunks.clear();
    totalChunkedSize = 0;
  }

  @Override
  public void sendChunk(ChannelBuffer content) {
    byte[] chunk = content.array();
    contentChunks.add(content.array());
    totalChunkedSize += chunk.length;
  }

  @Override
  public void sendChunkEnd() {
    body = new byte[totalChunkedSize];
    int index = 0;
    for (byte[] chunk : contentChunks) {
      System.arraycopy(chunk, 0, body, index, chunk.length);
      index += chunk.length;
    }
    contentChunks.clear();
  }

  @Override
  public void sendContent(HttpResponseStatus status, ChannelBuffer content, String contentType,
                          Multimap<String, String> headers) {
    setResponseContent(status, content.array());
  }

  private void setResponseContent(HttpResponseStatus status, byte[] content) {
    statusCode = status.getCode();
    body = content;
  }

  @Override
  public void sendFile(File file, Multimap<String, String> headers) {
    this.file = file;
  }

  public InternalHttpResponse getResponse() {
    return new BasicInternalHttpResponse(statusCode, body, file);
  }
}
