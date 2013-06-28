/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.http.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.beanutils.ConvertUtils;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;

// TODO: Check path collision
/**
 * HttpResourceHandler handles the http request. HttpResourceHandler looks up all Jax-rs annotations in classes
 * and dispatches to appropriate method on receiving requests.
 */
public class HttpResourceHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HttpResourceHandler.class);
  private PatternPathRouterWithGroups<HttpResourceModel> patternRouter = new PatternPathRouterWithGroups<HttpResourceModel>();

  /**
   * Construct HttpResourceHandler. Reads all annotations from all the handler classes and methods passed in, constructs
   * patternPathRouter which is routable by path to {@code HttpResourceModel} as destination of the route.
   * @param handlers Iterable of HttpHandler.
   */
  public HttpResourceHandler(Iterable<HttpHandler> handlers){
    for (HttpHandler handler : handlers){
      if (!handler.getClass().getSuperclass().equals(Object.class)){
        LOG.warn("{} is inherited. The annotations from base case will not be inherited",
                 handler.getClass().getName());
      }

      String basePath = "";
      if (handler.getClass().isAnnotationPresent(Path.class)){
        basePath =  handler.getClass().getAnnotation(Path.class).value();
      }

      for (Method method:  handler.getClass().getDeclaredMethods()){
        if (method.isAnnotationPresent(Path.class)){
          Preconditions.checkArgument(method.getParameterTypes().length >= 2,
                                     "No HttpRequest and HttpResponder parameter in the http handler signature");
          Preconditions.checkArgument(method.getParameterTypes()[0].isAssignableFrom(HttpRequest.class),
                                     "HttpRequest should be the first argument in the http handler");
          Preconditions.checkArgument(method.getParameterTypes()[1].isAssignableFrom(HttpResponder.class),
                                     "HttpResponder should be the first argument in the http handler");

          String relativePath = method.getAnnotation(Path.class).value();
          String absolutePath = String.format("%s/%s", basePath, relativePath);
          Set<HttpMethod> httpMethods = getHttpMethods(method);
          Preconditions.checkArgument(httpMethods.size() >= 1,
                                      String.format("No HttpMethod found for method: %s", method.getName()));
          patternRouter.add(absolutePath, new HttpResourceModel(httpMethods, method, handler));

        } else {
          LOG.warn("No Path annotation for method {}. HTTP calls will not be routed to this method",
                   method.getName());
        }
      }
    }
  }

  /**
   * Fetches the HttpMethod from annotations and returns String representation of HttpMethod.
   * Return emptyString if not present.
   * @param method Method handling the http request.
   * @return String representation of HttpMethod from annotations or emptyString as a default.
   */
  private Set<HttpMethod> getHttpMethods(Method method){
    Set<HttpMethod> httpMethods = Sets.newHashSet();

    if (method.isAnnotationPresent(GET.class)){
      httpMethods.add(HttpMethod.GET);
    } else if (method.isAnnotationPresent(PUT.class)){
      httpMethods.add(HttpMethod.PUT);
    } else if (method.isAnnotationPresent(POST.class)){
      httpMethods.add(HttpMethod.POST);
    } else if (method.isAnnotationPresent(DELETE.class)){
      httpMethods.add(HttpMethod.DELETE);
    }

    return httpMethods;
  }

  /**
   * Call the appropriate handler for handling the httprequest. 404 if path is not found. 405 if path is found but
   * httpMethod does not match what's configured.
   * @param request instance of {@code HttpRequest}
   * @param responder instance of {@code HttpResponder} to handle the request.
   */
  public void handle(HttpRequest request, HttpResponder responder){

    Map<String, String> groupValues = Maps.newHashMap();
    List<HttpResourceModel> resourceModels = patternRouter.getDestinations(request.getUri(), groupValues);

    HttpResourceModel httpResourceModel = getMatchedResourceModel(resourceModels, request.getMethod());

    if (httpResourceModel != null){
      //Found a httpresource route to it.
      try {
        Method method = httpResourceModel.getMethod();
        Object object = httpResourceModel.getObject();

        //Setup args for reflection call
        Object [] args = new Object[groupValues.size() + 2];
        int index = 0;
        args[index] = request;
        index++;
        args[index] = responder;

        Class<?>[] parameterTypes = method.getParameterTypes();
        for (Map.Entry<String, String> entry : groupValues.entrySet()){
          index++;
          args[index] = ConvertUtils.convert(entry.getValue(), parameterTypes[index]);
        }

        method.invoke(object, args);
      } catch (Throwable e) {
        LOG.error("Error processing path {} {}", request.getUri(), e);
        responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                            String.format("Error in executing path: %s", request.getUri()));
      }
    } else if (resourceModels.size() > 0)  {
      //Found a matching resource but could not find the right HttpMethod so return 405
      responder.sendError(HttpResponseStatus.METHOD_NOT_ALLOWED,
                          String.format("Problem accessing: %s. Reason: Method Not Allowed", request.getUri()));
    } else {
      responder.sendError(HttpResponseStatus.NOT_FOUND, String.format("Problem accessing: %s. Reason: Not Found",
                                                                      request.getUri()));
    }
  }

  /**
   * Get HttpResourceModel which matches the HttpMethod of the request.
   * @param resourceModels List of ResourceModels
   * @param targetHttpMethod HttpMethod
   * @return HttpResourceModel that matches httpMethod that needs to be handled. null if there are no matches.
   */
  private HttpResourceModel getMatchedResourceModel(List<HttpResourceModel> resourceModels,
                                                    HttpMethod targetHttpMethod){
    for (HttpResourceModel resourceModel : resourceModels){
      for (HttpMethod httpMethod : resourceModel.getHttpMethod()) {
        if (targetHttpMethod.equals(httpMethod)){
          return resourceModel;
        }
      }
    }
    return null;
  }
}
