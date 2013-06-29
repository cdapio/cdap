/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.http.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Webservice implemented using the netty framework. Implements Guava's Service interface to manage the states
 * of the webservice.
 */
public final class NettyHttpService extends AbstractIdleService {

  private static final Logger LOG  = LoggerFactory.getLogger(NettyHttpService.class);

  private ServerBootstrap bootstrap;
  private Channel channel;
  private int port;

  private int servicePort;
  private final int threadPoolSize;
  private final long threadKeepAliveSecs;
  private final Set<HttpHandler> httpHandlers;
  private final HandlerContext handlerContext;

  private HttpResourceHandler resourceHandler;


  /**
   * Initialize NettyHttpService.
   * @param port port to run the service on.
   * @param threadPoolSize Size of the thread pool for the executor.
   * @param threadKeepAliveSecs  maximum time that excess idle threads will wait for new tasks before terminating.
   * @param httpHandlers HttpHandlers to handle the calls.
   */
  public NettyHttpService(int port, int threadPoolSize, long threadKeepAliveSecs, Iterable<HttpHandler> httpHandlers){
    this.port = port;
    this.threadPoolSize = threadPoolSize;
    this.threadKeepAliveSecs = threadKeepAliveSecs;
    this.httpHandlers = ImmutableSet.copyOf(httpHandlers);
    this.handlerContext = new DummyHandlerContext();
  }

  /**
   * Create Execution handlers with threadPoolExecutor.
   * @param threadPoolSize size of threadPool
   * @param threadKeepAliveSecs  maximum time that excess idle threads will wait for new tasks before terminating.
   * @return instance of {@code ExecutionHandler}.
   */
  private ExecutionHandler createExecutionHandler(int threadPoolSize, long threadKeepAliveSecs){

    ThreadFactory threadFactory = new ThreadFactory() {
      private final ThreadGroup threadGroup = new ThreadGroup("executor-thread");
      private final AtomicLong count = new AtomicLong(0);

      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(threadGroup, r, String.format("executor-%d", count.getAndIncrement()));
        t.setDaemon(true);
        return t;
      }
    };

    //Create ExecutionHandler
    return new ExecutionHandler(new ThreadPoolExecutor(0, threadPoolSize, threadKeepAliveSecs, TimeUnit.SECONDS,
                                                       new SynchronousQueue<Runnable>(),
                                                       threadFactory,
                                                       new ThreadPoolExecutor.AbortPolicy()));
  }

  /**
   * Bootstrap the pipeline.
   * <ul>
   *   <li>Create Execution handler</li>
   *   <li>Setup Http resource handler</li>
   *   <li>Setup the netty pipeline</li>
   * </ul>
   * @param threadPoolSize Size of threadpool in threadpoolExecutor
   * @param threadKeepAliveSecs  maximum time that excess idle threads will wait for new tasks before terminating.
   * @param httpHandlers Handlers for httpRequests.
   */
  private void bootStrap(int threadPoolSize, long threadKeepAliveSecs, Iterable<HttpHandler> httpHandlers){

    ExecutionHandler executionHandler = createExecutionHandler(threadPoolSize, threadKeepAliveSecs);

    Executor bossExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                                                                .setDaemon(true)
                                                                .setNameFormat("boss-thread")
                                                                .build());

    Executor workerExecutor = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                                                              .setDaemon(true)
                                                              .setNameFormat("worker-thread")
                                                              .build());

    //Server bootstrap with default worker threads (2 * number of cores)
    bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(bossExecutor, workerExecutor));

    resourceHandler = new HttpResourceHandler(httpHandlers);
    resourceHandler.init(handlerContext);

    ChannelPipeline pipeline = bootstrap.getPipeline();
    pipeline.addLast("decoder", new HttpRequestDecoder());
    pipeline.addLast("encoder", new HttpResponseEncoder());
    pipeline.addLast("compressor", new HttpContentCompressor());
    pipeline.addLast("executor", executionHandler);
    //TODO: Add chunker
    pipeline.addLast("dispatcher", new HttpDispatcher(resourceHandler));
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting service on port {}", port);
    bootStrap(threadPoolSize, threadKeepAliveSecs, httpHandlers);
    InetSocketAddress address = new InetSocketAddress(port);
    channel = bootstrap.bind(address);
    servicePort = ((InetSocketAddress) channel.getLocalAddress()).getPort();

  }

  /**
   * @return port where the service is running.
   */
  public int getServicePort() {
    return servicePort;
  }

  @Override
  protected void shutDown() throws Exception {
    resourceHandler.destroy(handlerContext);
    LOG.info("Stopping service on port {}", port);
    channel.close();
  }

  /**
   * Builder to help create the NettyHttpService.
   */
  public static class Builder {

    private static final int DEFAULT_THREAD_POOL_SIZE = 60;
    private static final long DEFAULT_THREAD_KEEP_ALIVE_TIME_SECS = 60L;

    //Private constructor to prevent instantiating Builder instance directly.
    private Builder(){
      threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
      threadKeepAliveSecs = DEFAULT_THREAD_KEEP_ALIVE_TIME_SECS;
      port = 0;
    }

    private Iterable<HttpHandler> handlers;
    private int threadPoolSize;
    private int port;
    private long threadKeepAliveSecs;

    /**
     * Add HttpHandlers that service the request.
     * @param handlers Iterable of HttpHandlers.
     * @return instance of {@code Builder}.
     */
    public Builder addHttpHandlers(Iterable<HttpHandler> handlers){
      this.handlers = handlers;
      return this;
    }

    public Builder setThreadPoolSize(int threadPoolSize){
      this.threadPoolSize = threadPoolSize;
      return this;
    }

    public Builder setThreadKeepAliveSeconds(long threadKeepAliveSecs){
      this.threadKeepAliveSecs = threadKeepAliveSecs;
      return this;
    }

    /**
     * Set the port on which the service should listen to.
     * By default the service will run on a random port.
     * @param port port on which the service should listen to.
     * @return instance of {@code Builder}.
     */
    public Builder setPort(int port){
      this.port = port;
      return this;
    }

    public NettyHttpService build(){
      return new NettyHttpService(port, threadPoolSize, threadKeepAliveSecs, handlers);
    }


  }
}
