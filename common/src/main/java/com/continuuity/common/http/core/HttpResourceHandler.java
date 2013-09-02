/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.http.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelFutureProgressListener;
import org.jboss.netty.channel.DefaultFileRegion;
import org.jboss.netty.channel.FileRegion;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.handler.stream.ChunkedFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.activation.MimetypesFileTypeMap;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CACHE_CONTROL;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.DATE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.EXPIRES;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.IF_MODIFIED_SINCE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.LAST_MODIFIED;
import static org.jboss.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static org.jboss.netty.handler.codec.http.HttpHeaders.setContentLength;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

// TODO: Check path collision
/**
 * HttpResourceHandler handles the http request. HttpResourceHandler looks up all Jax-rs annotations in classes
 * and dispatches to appropriate method on receiving requests.
 */
public final class HttpResourceHandler implements HttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HttpResourceHandler.class);
  private PatternPathRouterWithGroups<HttpResourceModel> patternRouter =
    new PatternPathRouterWithGroups<HttpResourceModel>();
  private List<HttpHandler> handlers;
  private final File documentRoot;


  public HttpResourceHandler(Iterable<HttpHandler> handlers, File documentRoot) {
    this.documentRoot = documentRoot;
    //Store the handlers to call init and destroy on all handlers.
    this.handlers = ImmutableList.copyOf(handlers);
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
          LOG.warn("Not adding method {}({}) to path routing like. HTTP calls will not be routed to this method",
                   method.getName(), method.getParameterTypes());
        }
      }
    }
  }

  /**
   * Construct HttpResourceHandler. Reads all annotations from all the handler classes and methods passed in, constructs
   * patternPathRouter which is routable by path to {@code HttpResourceModel} as destination of the route.
   *
   * @param handlers Iterable of HttpHandler.
   */
  public HttpResourceHandler(Iterable <HttpHandler> handlers){
    this(handlers, new File(System.getProperty("user.dir")));
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
      if (httpResourceModel != null){
        // Found a httpresource route to it.
        httpResourceModel.handle(request, responder, groupValues);
      } else if (resourceModels.size() > 0)  {
        // Found a matching resource but could not find the right HttpMethod so return 405
        responder.sendError(HttpResponseStatus.METHOD_NOT_ALLOWED,
                            String.format("Problem accessing: %s. Reason: Method Not Allowed", request.getUri()));
      } else {
        // if there are no matches, we will try to find it there is static content to be served.
        // if it's not GET request, we are here because we didn't find a route.
        if (request.getMethod() != HttpMethod.GET) {
          responder.sendStatus(NOT_FOUND);
          return;
        }
        serveStaticContent(request, responder);
      }
    } catch (Throwable t){
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                          String.format("Caught exception processing request. Reason: %s",
                                         t.getMessage()));
    }
  }

  private void serveStaticContent(HttpRequest request, HttpResponder responder) throws Exception {

    final String path = sanitizeUri(request.getUri());
    if (path == null) {
      responder.sendStatus(FORBIDDEN);
      return;
    }

    File file = new File(path);
    if (file.isHidden() || !file.exists()) {
      responder.sendStatus(NOT_FOUND);
      return;
    }

    if (!file.isFile()) {
      responder.sendStatus(FORBIDDEN);
      return;
    }

    // Cache Validation
    String ifModifiedSince = request.getHeader(IF_MODIFIED_SINCE);
    if (ifModifiedSince != null && ifModifiedSince.length() != 0) {
      SimpleDateFormat dateFormatter = new SimpleDateFormat(HttpResponder.HTTP_DATE_FORMAT, Locale.US);
      Date ifModifiedSinceDate = dateFormatter.parse(ifModifiedSince);

      // Only compare up to the second because the datetime format we send to the client does
      // not have milliseconds
      long ifModifiedSinceDateSeconds = ifModifiedSinceDate.getTime() / 1000;
      long fileLastModifiedSeconds = file.lastModified() / 1000;
      if (ifModifiedSinceDateSeconds == fileLastModifiedSeconds) {
        responder.sendNotModifiedHeader();
        return;
      }
    }
    responder.sendFile(path, file);
  }

  private String sanitizeUri(String uri) {
    // Decode the path.
    try {
      uri = URLDecoder.decode(uri, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      try {
        uri = URLDecoder.decode(uri, "ISO-8859-1");
      } catch (UnsupportedEncodingException e1) {
        throw new Error();
      }
    }

    // Convert file separators.
    uri = uri.replace('/', File.separatorChar);

    // Simplistic dumb security check.
    // You will have to do something serious in the production environment.
    if (uri.contains(File.separator + '.') ||
      uri.contains('.' + File.separator) ||
      uri.startsWith(".") || uri.endsWith(".")) {
      return null;
    }

    // Convert to absolute path.
    return new File(documentRoot.getAbsolutePath(), uri).getAbsolutePath();
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
