/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.http.core;

import java.lang.reflect.Method;

/**
 * HttpResourceModel contains information needed to handle Http call for a given path. Used as a destination in
 * {@code PatternPathRouterWithGroups} to route URI paths to right Http end points.
 */
public class HttpResourceModel {

  private final String httpMethod;
  private final Method method;
  private final Object object;

  /**
   * Construct a resource model with HttpMethod, method that handles httprequest, Object that contains the method.
   * @param httpMethod http method that is handled by the resource.
   * @param method handler that handles the http request.
   * @param object instance of the class containing the method to handle a http request.
   */
  public HttpResourceModel(String httpMethod, Method method, Object object){
    this.httpMethod = httpMethod;
    this.method = method;
    this.object = object;
  }

  /**
   * @return httpMethod.
   */
  public String getHttpMethod() {
    return httpMethod;
  }

  /**
   * @return handler method that handles an http end-point.
   */
  public Method getMethod() {
    return method;
  }

  /**
   * @return instance of class containing the http handler.
   */
  public Object getObject() {
    return object;
  }
}
