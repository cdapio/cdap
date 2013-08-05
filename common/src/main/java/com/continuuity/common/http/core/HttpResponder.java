/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.http.core;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;

import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * HttpResponder responds back to the client that initiated the request. Caller can use sendJson method to respond
 * back to the client in json format.
 */
public class HttpResponder {
  private final Channel channel;
  private final boolean keepalive;

  public HttpResponder(Channel channel, boolean keepalive) {
    this.channel = channel;
    this.keepalive = keepalive;
  }

  /**
   * Sends json response back to the client.
   * @param status Status of the response.
   * @param object Object that will be serialized into Json and sent back as content.
   */
  public synchronized void sendJson(HttpResponseStatus status, Object object){
    try {
      HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);

      ChannelBuffer channelBuffer = ChannelBuffers.dynamicBuffer();
      JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(new ChannelBufferOutputStream(channelBuffer),
                                                                    Charsets.UTF_8));
      new Gson().toJson(object, object.getClass(), jsonWriter);
      jsonWriter.close();

      response.setContent(channelBuffer);
      response.setHeader(HttpHeaders.Names.CONTENT_TYPE, "application/json");
      response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, channelBuffer.readableBytes());
      if (keepalive) {
        response.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
      }

      ChannelFuture future = channel.write(response);
      if (!keepalive) {
        future.addListener(ChannelFutureListener.CLOSE);
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Send a string response back to the http client.
   * @param status status of the Http response.
   * @param data string data to be sent back.
   */
  public synchronized void sendString(HttpResponseStatus status, String data){
    try {
      HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);

      ChannelBuffer channelBuffer = ChannelBuffers.wrappedBuffer(Charsets.UTF_8.encode(data));
      response.setContent(channelBuffer);
      response.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=utf-8");
      response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, channelBuffer.readableBytes());
      if (keepalive) {
        response.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
      }

      ChannelFuture future = channel.write(response);
      if (!keepalive) {
        future.addListener(ChannelFutureListener.CLOSE);
      }

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Sends error message back to the client.
   *
   * @param status Status of the response.
   * @param errorMessage Error message sent back to the client.
   */
  public synchronized void sendError(HttpResponseStatus status, String errorMessage){
    Preconditions.checkArgument(!status.equals(HttpResponseStatus.OK), "Response status cannot be OK for errors");

    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);

    ChannelBuffer errorContent = ChannelBuffers.wrappedBuffer(Charsets.UTF_8.encode(errorMessage));
    response.setContent(errorContent);
    response.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=utf-8");
    response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, errorContent.readableBytes());
    if (keepalive) {
      response.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
    }

    ChannelFuture future = channel.write(response);
    if (!keepalive) {
      future.addListener(ChannelFutureListener.CLOSE);
    }
  }
}
