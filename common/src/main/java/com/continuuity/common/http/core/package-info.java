/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

/**
 * HTTP dispatchers and handlers libraries designed to be used with netty.
 * Sample code for Handler.
 *
 * @Path("/common/v1/")
 * @Singleton
 * public class FooHandler{
 *   @Path("foo")
 *   @GET
 *   public void MethodFoo(HttpRequest request, HttpResponder responder){
 *      responder.sendJson(HttpResponseStatus.OK, "{\"set\": \"foo\"}");
 *   }
 * }
 *
 * Sample code for netty pipeline using dispatcher.
 *
 * public ChannelPipeline setPipeline(ExecutionHandler handler) {
 *   List<Object> handlers = Lists.newArrayList();
 *   handlers.add(new FooHandler());
 *
 *   ChannelPipeline pipeline = Channels.pipeline();
 *   pipeline.addLast("decoder", new HttpRequestDecoder());
 *   pipeline.addLast("encoder", new HttpResponseEncoder());
 *   pipeline.addLast("compressor", new HttpContentCompressor());
 *   pipeline.addLast("executor", executionHandler);
 *   pipeline.addLast("dispatcher", new HttpDispatcher(new HttpMethodHandler()));
 *   return pipeline;
 * }
 *
 */
package com.continuuity.common.http.core;

