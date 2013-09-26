package com.continuuity.common.http.core;

import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.lang.reflect.Method;

/**
 * A base implementation of {@link HandlerHook} that provides no-op for both
 * {@link #preCall(org.jboss.netty.handler.codec.http.HttpRequest, HttpResponder, java.lang.reflect.Method)}
 * and {@link #postCall(org.jboss.netty.handler.codec.http.HttpRequest,
 * org.jboss.netty.handler.codec.http.HttpResponseStatus, java.lang.reflect.Method)} methods.
 */
public class AbstractHandlerHook implements HandlerHook {
  @Override
  public boolean preCall(HttpRequest request, HttpResponder responder, Method method) {
    return true;
  }

  @Override
  public void postCall(HttpRequest request, HttpResponseStatus status, Method method) {
    // no-op
  }
}
