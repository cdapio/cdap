/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.http.core;

import org.jboss.netty.handler.codec.http.HttpMethod;

import java.lang.reflect.Method;
import java.util.Set;

/**
 * HttpResourceModel contains information needed to handle Http call for a given path. Used as a destination in
 * {@code PatternPathRouterWithGroups} to route URI paths to right Http end points.
 */
public class HttpResourceModel {

  private final Set<HttpMethod> httpMethods;
  private final Method method;
  private final Object object;

  /**
   * Construct a resource model with HttpMethod, method that handles httprequest, Object that contains the method.
   * @param httpMethods Set of http methods that is handled by the resource.
   * @param method handler that handles the http request.
   * @param object instance of the class containing the method to handle a http request.
   */
  public HttpResourceModel(Set<HttpMethod> httpMethods, Method method, Object object){
    this.httpMethods = httpMethods;
    this.method = method;
    this.object = object;
  }

  /**
   * @return httpMethods.
   */
  public Set<HttpMethod> getHttpMethod() {
    return httpMethods;
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
