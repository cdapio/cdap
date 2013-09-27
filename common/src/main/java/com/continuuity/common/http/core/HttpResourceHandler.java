/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.http.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
import java.lang.reflect.Modifier;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

// TODO: Check path collision
/**
 * HttpResourceHandler handles the http request. HttpResourceHandler looks up all Jax-rs annotations in classes
 * and dispatches to appropriate method on receiving requests.
 */
public final class HttpResourceHandler implements HttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HttpResourceHandler.class);
  private final PatternPathRouterWithGroups<HttpResourceModel> patternRouter =
    new PatternPathRouterWithGroups<HttpResourceModel>();
  private final Iterable<HttpHandler> handlers;
  private final Iterable<HandlerHook> handlerHooks;

  /**
   * Construct HttpResourceHandler. Reads all annotations from all the handler classes and methods passed in, constructs
   * patternPathRouter which is routable by path to {@code HttpResourceModel} as destination of the route.
   *
   * @param handlers Iterable of HttpHandler.
   * @param handlerHooks Iterable of HandlerHook.
   */
  public HttpResourceHandler(Iterable<HttpHandler> handlers, Iterable<HandlerHook> handlerHooks){
    //Store the handlers to call init and destroy on all handlers.
    this.handlers = ImmutableList.copyOf(handlers);
    this.handlerHooks = ImmutableList.copyOf(handlerHooks);

    for (HttpHandler handler : handlers){
      String basePath = "";
      if (handler.getClass().isAnnotationPresent(Path.class)){
        basePath =  handler.getClass().getAnnotation(Path.class).value();
      }

      for (Method method:  handler.getClass().getDeclaredMethods()){
        if (method.getParameterTypes().length >= 2 &&
          method.getParameterTypes()[0].isAssignableFrom(HttpRequest.class) &&
          method.getParameterTypes()[1].isAssignableFrom(HttpResponder.class) &&
          Modifier.isPublic(method.getModifiers())) {

          String relativePath = "";
          if (method.getAnnotation(Path.class) != null) {
            relativePath = method.getAnnotation(Path.class).value();
          }
          String absolutePath = String.format("%s/%s", basePath, relativePath);
          Set<HttpMethod> httpMethods = getHttpMethods(method);
          Preconditions.checkArgument(httpMethods.size() >= 1,
                                      String.format("No HttpMethod found for method: %s", method.getName()));
          patternRouter.add(absolutePath, new HttpResourceModel(httpMethods, method, handler));
        } else {
          LOG.trace("Not adding method {}({}) to path routing like. HTTP calls will not be routed to this method",
                   method.getName(), method.getParameterTypes());
        }
      }
    }
  }

  /**
   * Fetches the HttpMethod from annotations and returns String representation of HttpMethod.
   * Return emptyString if not present.
   *
   * @param method Method handling the http request.
   * @return String representation of HttpMethod from annotations or emptyString as a default.
   */
  private Set<HttpMethod> getHttpMethods(Method method){
    Set<HttpMethod> httpMethods = Sets.newHashSet();

    if (method.isAnnotationPresent(GET.class)){
      httpMethods.add(HttpMethod.GET);
    }
    if (method.isAnnotationPresent(PUT.class)){
      httpMethods.add(HttpMethod.PUT);
    }
    if (method.isAnnotationPresent(POST.class)){
      httpMethods.add(HttpMethod.POST);
    }
    if (method.isAnnotationPresent(DELETE.class)){
      httpMethods.add(HttpMethod.DELETE);
    }

    return ImmutableSet.copyOf(httpMethods);
  }

  /**
   * Call the appropriate handler for handling the httprequest. 404 if path is not found. 405 if path is found but
   * httpMethod does not match what's configured.
   *
   * @param request instance of {@code HttpRequest}
   * @param responder instance of {@code HttpResponder} to handle the request.
   */
  public void handle(HttpRequest request, HttpResponder responder){

    Map<String, String> groupValues = Maps.newHashMap();
    String path = URI.create(request.getUri()).getPath();

    List<HttpResourceModel> resourceModels = patternRouter.getDestinations(path, groupValues);

    HttpResourceModel httpResourceModel = getMatchedResourceModel(resourceModels, request.getMethod());

    try {
      if (httpResourceModel != null) {
        //Found a httpresource route to it.

        // Call preCall method of handler hooks.
        boolean terminated = false;
        HandlerInfo info = new HandlerInfo(httpResourceModel.getMethod().getDeclaringClass().getName(),
                                           httpResourceModel.getMethod().getName());
        for (HandlerHook hook : handlerHooks) {
          if (!hook.preCall(request, responder, info)) {
            // Terminate further request processing if preCall returns false.
            terminated = true;
            break;
          }
        }

        // Call httpresource method
        if (!terminated) {
          // Wrap responder to make post hook calls.
          responder = new WrappedHttpResponder(responder, handlerHooks, request, info);
          httpResourceModel.handle(request, responder, groupValues);
        }
      } else if (resourceModels.size() > 0)  {
        //Found a matching resource but could not find the right HttpMethod so return 405
        responder.sendError(HttpResponseStatus.METHOD_NOT_ALLOWED,
                            String.format("Problem accessing: %s. Reason: Method Not Allowed", request.getUri()));
      } else {
        responder.sendError(HttpResponseStatus.NOT_FOUND, String.format("Problem accessing: %s. Reason: Not Found",
                                                                        request.getUri()));
      }
    } catch (Throwable t){
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                          String.format("Caught exception processing request. Reason: %s",
                                        t.getMessage()));
    }
  }

  /**
   * Get HttpResourceModel which matches the HttpMethod of the request.
   *
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

  @Override
  public void init(HandlerContext context) {
    for (HttpHandler handler : handlers){
      handler.init(context);
    }
  }

  @Override
  public void destroy(HandlerContext context) {
    for (HttpHandler handler : handlers){
      handler.destroy(context);
    }
  }
}
