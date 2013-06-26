/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

/**
 * HTTP dispatchers and handlers libraries designed to be used with netty. Uses Jax-RS annotations to get the
 * Paths and HttpMethods.
 * Note: Only supports the following annotations: Path, GET, PUT, POST, DELETE.
 * Note: Doesn't support getting Annotations used in base class.
 *
 * Sample usage Handlers and Netty service setup.
 *
 * Handler code:
 *
 * @Path("/common/v1/")
 * public class ApiHandler{
 *   @Path("widgets")
 *   @GET
 *   public void widgetHandler(HttpResponder responder){
 *      responder.sendJson(HttpResponseStatus.OK, "{\"key\": \"value\"}");
 *   }
 * }
 *
 * Netty Services:
 *
 * //Instantiate ServerBootstrap
 * ServerBootstrap bootstrap = new ServerBootstrap( new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
 *                                                                                  Executors.newCachedThreadPool()));
 *
 * //Setup Resource handlers
 * HttpResourceHandler.Builder builder = HttpResourceHandler.builder();
 * builder.addHandler(new ApiHandler());
 * HttpResourceHandler resourceHandler = builder.build();
 *
 * //Setup pipeline
 * ChannelPipeline pipeline = bootstrap.getPipeline();
 * pipeline.addLast("decoder", new HttpRequestDecoder());
 * pipeline.addLast("encoder", new HttpResponseEncoder());
 * pipeline.addLast("compressor", new HttpContentCompressor());
 * pipeline.addLast("executor", new ExecutionHandler(new OrderedMemoryAwareThreadPoolExecutor(16, 1048576, 1048576)));
 * pipeline.addLast("dispatcher", new HttpDispatcher(resourceHandler));
 *
 * //bind to address
 * InetSocketAddress address = new InetSocketAddress(9090);
 * Channel serverChannel = bootstrap.bind(address);
 *
 */
package com.continuuity.common.http.core;

