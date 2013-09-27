package com.continuuity.common.http.core;

import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;

/**
 * Wrap HttpResponder to call post handler hook.
 */
final class WrappedHttpResponder extends HttpResponder {
  private static final Logger LOG = LoggerFactory.getLogger(WrappedHttpResponder.class);

  private final HttpResponder delegate;
  private final Iterable<? extends HandlerHook> handlerHooks;
  private final HttpRequest httpRequest;
  private final HandlerInfo handlerInfo;
  private volatile HttpResponseStatus status;

  public WrappedHttpResponder(HttpResponder delegate, Iterable<? extends HandlerHook> handlerHooks,
                              HttpRequest httpRequest, HandlerInfo handlerInfo) {
    super(null, false);
    this.delegate = delegate;
    this.handlerHooks = handlerHooks;
    this.httpRequest = httpRequest;
    this.handlerInfo = handlerInfo;
  }

  @Override
  public void sendJson(HttpResponseStatus status, Object object) {
    runHook(status);
    delegate.sendJson(status, object);
  }

  @Override
  public void sendJson(HttpResponseStatus status, Object object, Type type) {
    runHook(status);
    delegate.sendJson(status, object, type);
  }

  @Override
  public void sendJson(HttpResponseStatus status, Object object, Type type, Gson gson) {
    runHook(status);
    delegate.sendJson(status, object, type, gson);
  }

  @Override
  public void sendString(HttpResponseStatus status, String data) {
    runHook(status);
    delegate.sendString(status, data);
  }

  @Override
  public void sendStatus(HttpResponseStatus status) {
    runHook(status);
    delegate.sendStatus(status);
  }

  @Override
  public void sendByteArray(HttpResponseStatus status, byte[] bytes, Multimap<String, String> headers) {
    runHook(status);
    delegate.sendByteArray(status, bytes, headers);
  }

  @Override
  public void sendBytes(HttpResponseStatus status, ByteBuffer buffer, Multimap<String, String> headers) {
    runHook(status);
    delegate.sendBytes(status, buffer, headers);
  }

  @Override
  public void sendError(HttpResponseStatus status, String errorMessage) {
    runHook(status);
    delegate.sendError(status, errorMessage);
  }

  @Override
  public void sendChunkStart(HttpResponseStatus status, Multimap<String, String> headers) {
    this.status = status;
    delegate.sendChunkStart(status, headers);
  }

  @Override
  public void sendChunk(ChannelBuffer content) {
    delegate.sendChunk(content);
  }

  @Override
  public void sendChunkEnd() {
    runHook(status);
    delegate.sendChunkEnd();
  }

  @Override
  public void sendContent(HttpResponseStatus status, ChannelBuffer content, String contentType,
                          Multimap<String, String> headers) {
    runHook(status);
    delegate.sendContent(status, content, contentType, headers);
  }

  private void runHook(HttpResponseStatus status) {
    for (HandlerHook hook : handlerHooks) {
      try {
        hook.postCall(httpRequest, status, handlerInfo);
      } catch (Throwable t) {
        LOG.error("Post handler hook threw exception: ", t);
      }
    }
  }
}
