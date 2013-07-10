package com.continuuity.gateway.util;

import com.continuuity.common.metrics.MetricsHelper;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpChunk;
import org.jboss.netty.handler.codec.http.DefaultHttpChunkTrailer;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.continuuity.common.metrics.MetricsHelper.Status.BadRequest;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Values;

/**
 * This is a base class for the gateway's Netty-based Http request handlers.
 * It implements common methods such as returning an error or sending an OK
 * response.
 */
public class NettyRestHandler extends SimpleChannelUpstreamHandler {
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory
    .getLogger(NettyRestHandler.class);

  /**
   * Respond to the client with an error. That closes the connection.
   *
   * @param channel the channel on which the request came
   * @param status  the HTTP status to return
   */
  protected void respondError(Channel channel, HttpResponseStatus status) {
    respondError(channel, status, null);
  }

  /**
   * Respond to the client with an error and a message. That closes the connection.
   *
   * @param channel the channel on which the request came
   * @param status  the HTTP status to return
   * @param reason the reason for the error, will be returned in the body of the response.
   */
  protected void respondError(Channel channel, HttpResponseStatus status, String reason) {

    HttpResponse response = new DefaultHttpResponse(
      HttpVersion.HTTP_1_1, status);
    if (reason != null) {
      ChannelBuffer body = ChannelBuffers.wrappedBuffer(Charsets.UTF_8.encode(reason));
      response.addHeader(Names.CONTENT_LENGTH, body.readableBytes());
      response.setContent(body);
    } else {
      response.addHeader(Names.CONTENT_LENGTH, 0);
    }
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
  protected void respondNotAllowed(Channel channel, Iterable<HttpMethod> allowedMethods) {
    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.METHOD_NOT_ALLOWED);
    StringBuilder allowed = new StringBuilder();
    String comma = "";
    for (HttpMethod method : allowedMethods) {
      allowed.append(method);
      allowed.append(comma);
      comma = ", ";
    }
    response.addHeader(Names.ALLOW, allowed.toString());
    response.addHeader(Names.CONTENT_LENGTH, 0);
    ChannelFuture future = channel.write(response);
    future.addListener(ChannelFutureListener.CLOSE);
  }

  /**
   * Respond to the client with success. This keeps the connection alive
   * unless specified otherwise in the original request.
   *
   * @param channel the channel on which the request came
   * @param request the original request (to determine whether to keep
   *                the connection alive)
   */
  protected void respondSuccess(Channel channel, HttpRequest request) {
    respond(channel, request, null, null, (ChannelBuffer) null);
  }

  /**
   * Respond to the client with success. This keeps the connection alive
   * unless specified otherwise in the original request.
   *
   * @param channel the channel on which the request came
   * @param request the original request (to determine whether to keep the
   *                connection alive)
   * @param content the content of the response to send
   */
  protected void respondSuccess(Channel channel, HttpRequest request,
                                ChannelBuffer content) {
    respond(channel, request, null, null, content);
  }

  protected void respondSuccess(Channel channel, HttpRequest request,
                                byte[] content) {
    respond(channel, request, null, null, ChannelBuffers.wrappedBuffer(content));
  }

  /**
   * Respond to the client with success. This keeps the connection alive
   * unless specified otherwise in the original request.
   *
   * @param channel the channel on which the request came
   * @param request the original request (to determine whether to keep the
   *                connection alive)
   * @param status  the status code to respond with. Defaults to 200-OK if null
   */
  protected void respondSuccess(Channel channel, HttpRequest request,
                                HttpResponseStatus status) {
    respond(channel, request, status, null, (ChannelBuffer) null);
  }

  protected void respond(Channel channel, HttpRequest request,
                         HttpResponseStatus status,
                         Map<String, String> headers, byte[] content) {
    respond(channel, request, status, headers, ChannelBuffers.wrappedBuffer(content));
  }

  /**
   * Respond to the client with success. This keeps the connection alive
   * unless specified otherwise in the original request.
   *
   * @param channel the channel on which the request came
   * @param request the original request (to determine whether to keep the
   *                connection alive)
   * @param status  the status code to respond with. Defaults to 200-OK if null
   * @param headers additional headers to send with the response. May be null.
   * @param content the content of the response to send
   */
  protected void respond(Channel channel, HttpRequest request,
                         HttpResponseStatus status,
                         Map<String, String> headers, ChannelBuffer content) {
    HttpResponse response = new DefaultHttpResponse(
      HttpVersion.HTTP_1_1, status != null ? status : HttpResponseStatus.OK);
    boolean keepAlive = HttpHeaders.isKeepAlive(request);
    if (headers != null) {
      for (Map.Entry<String, String> entry : headers.entrySet()) {
        response.addHeader(entry.getKey(), entry.getValue());
      }
    }
    response.addHeader(Names.CONTENT_LENGTH, content == null ? 0 : content.readableBytes());
    response.setContent(content);

    ChannelFuture future = channel.write(response);
    if (!keepAlive) {
      future.addListener(ChannelFutureListener.CLOSE);
    }
  }

  /**
   * Respond to the client saying the response will be in chunks. Add chunks to response using @{link respondChunk}
   * and @{link respondChunkEnd}.
   * @param channel the channel on which the request came
   * @param status  the status code to respond with. Defaults to 200-OK if null
   * @param headers additional headers to send with the response. May be null.
   */
  protected void respondChunkStart(Channel channel,
                         HttpResponseStatus status,
                         Map<String, String> headers) {
    HttpResponse response = new DefaultHttpResponse(
      HttpVersion.HTTP_1_1, status != null ? status : HttpResponseStatus.OK);
    if (headers != null) {
      for (Map.Entry<String, String> entry : headers.entrySet()) {
        response.addHeader(entry.getKey(), entry.getValue());
      }
    }

    response.setChunked(true);
    response.setHeader(Names.TRANSFER_ENCODING, Values.CHUNKED);
    channel.write(response);
  }

  /**
   * Add a chunk of data to the response. @{link respondChunkStart} should be called before calling this method.
   * @{link respondChunkEnd} should be called after all chunks are done.
   * @param channel the channel on which the request came
   * @param content the chunk of content to send
   */
  protected void respondChunk(Channel channel, ChannelBuffer content) {
    channel.write(new DefaultHttpChunk(content));
  }

  /**
   * Called after all chunks are done. This keeps the connection alive
   * unless specified otherwise in the original request.
   * @param channel the channel on which the request came
   * @param request the original request (to determine whether to keep the
   *                connection alive)
   */
  protected void respondChunkEnd(Channel channel, HttpRequest request) {
    boolean keepAlive = HttpHeaders.isKeepAlive(request);
    ChannelFuture future = channel.write(new DefaultHttpChunkTrailer());
    if (!keepAlive) {
      future.addListener(ChannelFutureListener.CLOSE);
    }
  }

  protected void respondToPing(Channel channel, HttpRequest request) {
    respondSuccess(channel, request, ChannelBuffers.wrappedBuffer(Charsets.UTF_8.encode("OK.\n")));
  }

  protected void respondBadRequest(MessageEvent message, HttpRequest request,
                                   MetricsHelper helper, String reason,
                                   HttpResponseStatus status, Exception e) {
    if (LOG.isTraceEnabled()) {
      reason = (e == null || e.getMessage() == null) ? reason : reason + ": " + e.getMessage();
      LOG.trace("Received an unsupported request (" + reason + ") with URI '" + request.getUri() + "'");
    }
    helper.finish(BadRequest);
    respondError(message.getChannel(), status, reason);
  }

  protected void respondBadRequest(MessageEvent message, HttpRequest request, MetricsHelper helper, String reason,
                                   HttpResponseStatus status) {
    respondBadRequest(message, request, helper, reason, status, null);
  }

  protected void respondBadRequest(MessageEvent message, HttpRequest request, MetricsHelper helper, String reason) {
    respondBadRequest(message, request, helper, reason, HttpResponseStatus.BAD_REQUEST);
  }

  protected void respondBadRequest(MessageEvent message, HttpRequest request, MetricsHelper helper, String reason,
                                   Exception e) {
    respondBadRequest(message, request, helper, reason, HttpResponseStatus.BAD_REQUEST, e);
  }

  protected void respondJson(Channel channel, HttpRequest request, HttpResponseStatus status, byte[] jsonContent) {
    Map<String, String> headers = Maps.newHashMap();
    headers.put(Names.CONTENT_TYPE, "application/json");
    respond(channel, request, status, headers, jsonContent);
  }

  protected JsonObject getJsonStatus(int status, String message) {
    JsonObject object = new JsonObject();
    object.addProperty("status", status);
    object.addProperty("message", message);
    return object;
  }
}
