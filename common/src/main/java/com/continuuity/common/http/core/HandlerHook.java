package com.continuuity.common.http.core;

import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;
import java.lang.reflect.Method;

/**
 * Interface that needs to be implemented to intercept handler method calls.
 */
public interface HandlerHook {

  /**
   * preCall is run before a handler method call is made. If any of the preCalls throw exception then the request
   * processing will be terminated, however postCall hooks will still be called.
   * @param request HttpRequest being processed.
   * @param responder HttpResponder to send response
   * @param method Handler method that will be called.
   * @return true if the request processing can continue, otherwise the hook should send response and return false to
   * stop further request processing.
   */
  boolean preCall(HttpRequest request, HttpResponder responder, Method method);

  /**
   * postCall is run after a handler method call is made. If any of the postCalls throw and exception then the
   * remaining postCalls will not be called, and this will not affect the status of the request.
   * @param request HttpRequest being processed.
   * @param status Http status returned to the client.
   * @param method Handler method that was called.
   */
  void postCall(HttpRequest request, HttpResponseStatus status, @Nullable Method method);
}
