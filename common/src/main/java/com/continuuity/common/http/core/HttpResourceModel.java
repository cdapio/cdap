/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.http.core;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.commons.beanutils.ConvertUtils;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.PathParam;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

/**
 * HttpResourceModel contains information needed to handle Http call for a given path. Used as a destination in
 * {@code PatternPathRouterWithGroups} to route URI paths to right Http end points.
 */
public final class HttpResourceModel {

  private static final Logger LOG = LoggerFactory.getLogger(HttpResourceModel.class);
  private final Set<HttpMethod> httpMethods;
  private final String path;
  private final Method method;
  private final HttpHandler handler;

  /**
   * Construct a resource model with HttpMethod, method that handles httprequest, Object that contains the method.
   *
   * @param httpMethods Set of http methods that is handled by the resource.
   * @param path path associated with this model.
   * @param method handler that handles the http request.
   * @param handler instance {@code HttpHandler}.
   */
  public HttpResourceModel(Set<HttpMethod> httpMethods, String path, Method method, HttpHandler handler){
    this.httpMethods = httpMethods;
    this.path = path;
    this.method = method;
    this.handler = handler;
  }

  /**
   * @return httpMethods.
   */
  public Set<HttpMethod> getHttpMethod() {
    return httpMethods;
  }

  /**
   * @return path associated with this model.
   */
  public String getPath() {
    return path;
  }

  /**
   * @return handler method that handles an http end-point.
   */
  public Method getMethod() {
    return method;
  }

  /**
   * @return instance of {@code HttpHandler}.
   */
  public HttpHandler getHttpHandler() {
    return handler;
  }

  /**
   * Handle http Request.
   *
   * @param request  HttpRequest to be handled.
   * @param responder HttpResponder to write the response.
   * @param groupValues Values needed for the invocation.
   */
  public void handle(HttpRequest request, HttpResponder responder, Map<String, String> groupValues){
    //TODO: Refactor group values.
    try {
      if (httpMethods.contains(request.getMethod())){
        //Setup args for reflection call
        Object [] args = new Object[method.getParameterTypes().length];
        int parameterIndex = 0;
        args[parameterIndex] = request;
        parameterIndex++;
        args[parameterIndex] = responder;

        if (method.getParameterTypes().length > 2) {
          Class<?>[] parameterTypes = method.getParameterTypes();
          for (Annotation[] annotations : method.getParameterAnnotations()) {
             for (Annotation annotation : annotations){
               if (annotation.annotationType().isAssignableFrom(PathParam.class)){
                 PathParam param = (PathParam) annotation;
                 String value = groupValues.get(param.value());
                 Preconditions.checkArgument(value != null, "Could not resolve value for parameter %s", param.value());
                 parameterIndex++;
                 args[parameterIndex] = ConvertUtils.convert(value, parameterTypes[parameterIndex]);
               }
             }
          }
          Preconditions.checkArgument(method.getParameterTypes().length == parameterIndex + 1,
                                      "Could not resolve all parameters for method %s", method.getName());
        }
        method.invoke(handler, args);
      } else {
        //Found a matching resource but could not find the right HttpMethod so return 405
        responder.sendError(HttpResponseStatus.METHOD_NOT_ALLOWED,
                            String.format("Problem accessing: %s. Reason: Method Not Allowed", request.getUri()));
      }
    } catch (Throwable e) {
      LOG.error("Error processing path {} {}", request.getUri(), e, e);
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                          String.format("Error in executing path: %s", request.getUri()));
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("httpMethods", httpMethods)
      .add("path", path)
      .add("method", method)
      .add("handler", handler)
      .toString();
  }
}
