package com.continuuity.common.http.core;

import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

/**
 * A base implementation of {@link HandlerHook} that provides no-op for both
 * {@link HandlerHook#preCall(org.jboss.netty.handler.codec.http.HttpRequest, HttpResponder, HandlerInfo)}
 * and {@link HandlerHook#postCall(org.jboss.netty.handler.codec.http.HttpRequest,
 * org.jboss.netty.handler.codec.http.HttpResponseStatus, HandlerInfo)} methods.
 */
public abstract class AbstractHandlerHook implements HandlerHook {
  @Override
  public boolean preCall(HttpRequest request, HttpResponder responder, HandlerInfo handlerInfo) {
    return true;
  }

  @Override
  public void postCall(HttpRequest request, HttpResponseStatus status, HandlerInfo handlerInfo) {
    // no-op
  }
}
