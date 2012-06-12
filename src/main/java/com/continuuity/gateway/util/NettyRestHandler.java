package com.continuuity.gateway.util;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a base class for the gateway's Netty-based Http request handlers. It
 * implements common methods such as returning an error or sending an OK response.
 */
public class NettyRestHandler extends SimpleChannelUpstreamHandler {
  private static final Logger LOG = LoggerFactory
      .getLogger(NettyRestHandler.class);

  /**
   * Respond to the client with an error. That closes the connection.
   *
   * @param channel the channel on which the request came
   * @param status  the HTTP status to return
   */
  protected void respondError(Channel channel, HttpResponseStatus status) {
    HttpResponse response = new DefaultHttpResponse(
        HttpVersion.HTTP_1_1, status);
    response.addHeader(HttpHeaders.Names.CONTENT_LENGTH, 0);
    ChannelFuture future = channel.write(response);
    future.addListener(ChannelFutureListener.CLOSE);
  }

  /**
   * Respond to the client with a "method not allowed" error. This writes the
   * mandated Allow: header to inform the client what methods are accepted.
   * This also closes the connection.
   *
   * @param channel        the channel on which the request came
   * @param allowedMethods the HTTP methods that are accepted
   */
  protected void respondNotAllowed(Channel channel, HttpMethod[] allowedMethods) {
    HttpResponse response = new DefaultHttpResponse(
        HttpVersion.HTTP_1_1, HttpResponseStatus.METHOD_NOT_ALLOWED);
    StringBuilder allowed = new StringBuilder();
    String comma = "";
    for (HttpMethod method : allowedMethods) {
      allowed.append(method);
      allowed.append(comma);
      comma = ", ";
    }
    response.addHeader(HttpHeaders.Names.ALLOW, allowed.toString());
    response.addHeader(HttpHeaders.Names.CONTENT_LENGTH, 0);
    ChannelFuture future = channel.write(response);
    future.addListener(ChannelFutureListener.CLOSE);
  }

  /**
   * Respond to the client with success. This keeps the connection alive
   * unless specified otherwise in the original request.
   *
   * @param channel the channel on which the request came
   * @param request the original request (to determine whether to keep the connection alive)
   * @param content the content of the response to send
   */
  protected void respondSuccess(Channel channel, HttpRequest request, byte[] content) {
    HttpResponse response = new DefaultHttpResponse(
        HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    boolean keepAlive = HttpHeaders.isKeepAlive(request);
    response.addHeader(HttpHeaders.Names.CONTENT_LENGTH, content == null ? 0 : content.length);
    if (content != null) {
      response.setContent(ChannelBuffers.wrappedBuffer(content));
    }
    ChannelFuture future = channel.write(response);
    if (!keepAlive) {
      future.addListener(ChannelFutureListener.CLOSE);
    }
  }
}
