/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.http.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

// TODO: lifecycle methods
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
   * @param handlers list of HttpHandlers.
   */
  private HttpResourceHandler(List<Object> handlers){

    for (Object handler : handlers){
      if (!handler.getClass().getSuperclass().equals(Object.class)){
        LOG.warn("{} is inherited. The annotations from base case will not be inherited",
                 handler.getClass().getCanonicalName());
      }

      String basePath = "";
      if (handler.getClass().isAnnotationPresent(Path.class)){
        basePath =  handler.getClass().getAnnotation(Path.class).value();
      }

      for (Method method:  handler.getClass().getDeclaredMethods()){
        if (method.isAnnotationPresent(Path.class)){
          Preconditions.checkArgument(method.getParameterTypes().length >= 1,
                                      "No HttpResponder parameter in the http handler signature");
          Preconditions.checkArgument(method.getParameterTypes()[0].isAssignableFrom(HttpResponder.class),
                                      "HttpResponder should be the first argument in the http handler");

          String relativePath = method.getAnnotation(Path.class).value();
          String absolutePath = String.format("%s/%s", basePath, relativePath);
          patternRouter.add(absolutePath, new HttpResourceModel(getHttpMethod(method), method, handler));

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
  private String getHttpMethod(Method method){
    if (method.isAnnotationPresent(GET.class)){
      return HttpMethod.GET.getName();
    } else if (method.isAnnotationPresent(PUT.class)){
      return HttpMethod.PUT.getName();
    } else if (method.isAnnotationPresent(POST.class)){
      return HttpMethod.POST.getName();
    } else if (method.isAnnotationPresent(DELETE.class)){
      return HttpMethod.DELETE.getName();
    } else {
      return "";
    }
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

    HttpResourceModel httpResourceModel = filterResults(resourceModels, request.getMethod());

    if (httpResourceModel != null){
      //Found a httpresource route to it.
      try {
        Method method = httpResourceModel.getMethod();
        Object object = httpResourceModel.getObject();

        //Setup args for reflection call
        Object [] args = new Object[groupValues.size() + 1];
        int index = 0;
        args[index] = (Object) responder;
        //TODO: Fix reflection call to work with Generic types other than String.
        for (Map.Entry<String, String> entry : groupValues.entrySet()){
          index++;
          args[index] = (Object) entry.getValue();
        }

        method.invoke(object, args);
      } catch (IllegalAccessException e) {
        LOG.error("Error processing path {} {}", request.getUri(), e);
        responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                            String.format("Error in executing path: %s", request.getUri()));
      } catch (InvocationTargetException e) {
        LOG.error("Error processing path {} {}", request.getUri(), e);
        responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                            String.format("Error in executing path: %s", request.getUri()));
      } catch (Exception e){
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
   * Filter results by HttpMethod that is requested to be handled.
   * @param resourceModels List of ResourceModels
   * @param method HttpMethod
   * @return HttpResourceModel filtered by httpMethod that needs to be handled. null if there are no matches.
   */
  private HttpResourceModel filterResults(List<HttpResourceModel> resourceModels, HttpMethod method){
    for (HttpResourceModel resourceModel : resourceModels){
      if (method.getName().toLowerCase().equals(resourceModel.getHttpMethod().toLowerCase())){
        return resourceModel;
      }
    }
    return null;
  }

  /**
   * Builder for HttpResourceHandler.
   */
  public static final class Builder {
    private List<Object> handlers;

    public Builder(){
      handlers = Lists.newArrayList();
    }

    /**
     * Add HttpHandler - HttpHandler consists of methods that handle http requests with jersey annotations.
     * @param handler instance of Handler.
     * @return instance of Builder
     */
    public Builder addHandler(Object handler){
      handlers.add(handler);
      return this;
    }

    /**
     * Build HttpResourceHandler.
     * @return instance of {@code HttpResourceHandler}
     */
    public HttpResourceHandler build(){
      //TODO: Check for path collision
      return new HttpResourceHandler(handlers);
    }
  }

  /**
   * @return instance of {@code Builder}
   */
  public static Builder builder(){
    return new Builder();
  }
}
