/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.http.core;

import com.google.common.base.Preconditions;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpDispatcher that routes HTTP requests to appropriate http handler methods. The mapping between uri paths to
 * methods that handle particular path is managed using jax-rs annotations. {@code HttpMethodHandler} routes to method
 * whose @Path annotation matches the http request uri.
 */
public class HttpDispatcher extends SimpleChannelUpstreamHandler {

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
    httpMethodHandler.handle(httpRequest, new BasicHttpResponder(channel, HttpHeaders.isKeepAlive(httpRequest)));
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    LOG.error("Exception caught in channel processing.", e.getCause());
    ctx.getChannel().close();
  }
}
