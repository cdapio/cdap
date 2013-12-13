/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.http.core;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.gson.Gson;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * InternalHttpResponder is used when a handler is being called internally by some other handler, and thus there
 * is no need to go through the network.  It stores the status code and content in memory and returns them when asked.
 */
public class InternalHttpResponder implements HttpResponder {
  private int statusCode;
  private List<ChannelBuffer> contentChunks;
  private InputSupplier<? extends InputStream> inputSupplier;

  private static final Gson gson = new Gson();

  public InternalHttpResponder() {
    contentChunks = Lists.newLinkedList();
    statusCode = 0;
  }

  @Override
  public void sendJson(HttpResponseStatus status, Object object){
    sendJson(status, object, object.getClass());
  }

  @Override
  public void sendJson(HttpResponseStatus status, Object object, Type type){
    sendJson(status, object, type, gson);
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
  public void sendStatus(HttpResponseStatus status, Multimap<String, String> headers) {
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
  }

  @Override
  public void sendChunk(ChannelBuffer content) {
    contentChunks.add(content);
  }

  @Override
  public void sendChunkEnd() {
    ChannelBuffer[] chunks = new ChannelBuffer[contentChunks.size()];
    contentChunks.toArray(chunks);
    final ChannelBuffer body = ChannelBuffers.wrappedBuffer(chunks);
    body.markReaderIndex();

    inputSupplier = new InputSupplier<InputStream>() {
      @Override
      public InputStream getInput() throws IOException {
        body.resetReaderIndex();
        return new ChannelBufferInputStream(body);
      }
    };
  }

  @Override
  public void sendContent(HttpResponseStatus status, ChannelBuffer content, String contentType,
                          Multimap<String, String> headers) {
    setResponseContent(status, content.array());
  }

  private void setResponseContent(HttpResponseStatus status, byte[] content) {
    statusCode = status.getCode();
    inputSupplier = ByteStreams.newInputStreamSupplier(content);
  }

  @Override
  public void sendFile(File file, Multimap<String, String> headers) {
    statusCode = HttpResponseStatus.OK.getCode();
    inputSupplier = Files.newInputStreamSupplier(file);
  }

  public InternalHttpResponse getResponse() {
    return new BasicInternalHttpResponse(statusCode, inputSupplier);
  }
}
