/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.http.core;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpDispatcher that routes HTTP requests to appropriate http handler methods. The mapping between uri paths to
 * methods that handle particular path is managed using jax-rs annotations. {@code HttpMethodHandler} routes to method
 * whose @Path annotation matches the http request uri.
 */
public class HttpDispatcher extends SimpleChannelHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HttpDispatcher.class);
  private final HttpResourceHandler httpMethodHandler;

  public HttpDispatcher(HttpResourceHandler methodHandler) {
    this.httpMethodHandler = methodHandler;
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    Object message = e.getMessage();
    if (!(message instanceof HttpRequest)) {
      super.messageReceived(ctx, e);
      return;
    }
    handleRequest((HttpRequest) message, ctx.getChannel());
  }

  private void handleRequest(HttpRequest httpRequest, Channel channel) {
    Preconditions.checkNotNull(httpMethodHandler, "Http Handler factory cannot be null");
    httpMethodHandler.handle(httpRequest, new HttpResponder(channel));
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    LOG.error("Exception caught in channel processing.", e.getCause());
    ctx.getChannel().close();
  }

  /**
   * Sends 404 and closes the channel.
   * @param status
   * @param channel
   */
  private void errorResponse(HttpResponseStatus status, Channel channel, String content) {
    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
    response.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=utf-8");
    response.setContent(ChannelBuffers.wrappedBuffer(Charsets.UTF_8.encode(content)));
    Channels.write(channel, response).addListener(ChannelFutureListener.CLOSE);
  }
}
