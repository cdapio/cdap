package com.continuuity.common.http.core;

import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

/**
 * Interface that needs to be implemented to intercept handler method calls.
 */
public interface HandlerHook {

  /**
   * preCall is run before a handler method call is made. If any of the preCalls throw exception or return false then
   * no other subsequent preCalls will be called and the request processing will be terminated,
   * also no postCall hooks will be called.
   *
   *
   * @param request HttpRequest being processed.
   * @param responder HttpResponder to send response.
   * @param handlerInfo Info on handler method that will be called.
   * @return true if the request processing can continue, otherwise the hook should send response and return false to
   * stop further processing.
   */
  boolean preCall(HttpRequest request, HttpResponder responder, HandlerInfo handlerInfo);

  /**
   * postCall is run after a handler method call is made. If any of the postCalls throw and exception then the
   * remaining postCalls will still be called. If the handler method was not called then postCall hooks will not be
   * called.
   *
   * @param request HttpRequest being processed.
   * @param status Http status returned to the client.
   * @param handlerInfo Info on handler method that was called.
   */
  void postCall(HttpRequest request, HttpResponseStatus status, HandlerInfo handlerInfo);
}
