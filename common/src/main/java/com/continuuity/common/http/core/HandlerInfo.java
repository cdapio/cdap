package com.continuuity.common.http.core;

/**
 * Contains information about {@link HttpHandler} method.
 */
public class HandlerInfo {
  private final String handlerName;
  private final String methodName;

  public HandlerInfo(String handlerName, String methodName) {
    this.handlerName = handlerName;
    this.methodName = methodName;
  }

  public String getHandlerName() {
    return handlerName;
  }

  public String getMethodName() {
    return methodName;
  }
}
