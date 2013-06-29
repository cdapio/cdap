/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

/**
 * Service and components to build Netty based Http web service.
 * {@code NettyHttpService} sets up the necessary pipeline and manages starting, stopping,
 * state-management of the web service.
 *
 * In-order to handle http requests, {@code HttpHandler} must be implemented. The methods
 * in the classes implemented from {@code HttpHandler} must be annotated with Jersey annotations to
 * specify http uri paths and http methods.
 * Note: Only supports the following annotations: Path, PathParams, GET, PUT, POST, DELETE.
 * Note: Doesn't support getting Annotations from base class if the HttpHandler implements also extends
 * a class with annotation.
 *
 * Sample usage Handlers and Netty service setup.
 *
 * //Setup Handlers
 *
 * @Path("/common/v1/")
 * public class ApiHandler implements HttpHandler{
 *   @Path("widgets")
 *   @GET
 *   public void widgetHandler(HttpRequest request, HttpResponder responder){
 *      responder.sendJson(HttpResponseStatus.OK, "{\"key\": \"value\"}");
 *   }
 *
 *   @Override
 *   public void init(HandlerContext context){
 *    //Perform bootstrap operations before any of the handlers in this class gets called.
 *   }
 *
 *   @Override
 *   public void destroy(HandlerContext context){
 *    //Perform teardown operations the server shuts down.
 *  }
 *}
 * //Set up and build Netty pipeline
 *
 * List<ApiHandler> handlers = Lists.newArrayList();
 * handlers.add(new Handler());
 *
 * NettyHttpService.Builder builder = NettyHttpService.builder();
 * builder.addHttpHandlers(handlers);
 * builder.setPort(8989);
 *
 * Channel service = builder.build();
 * service.startAndWait();
 *
 * //Stop the web-service
 * service.shutdown();
 */
package com.continuuity.common.http.core;

