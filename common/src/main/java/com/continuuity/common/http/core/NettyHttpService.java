/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.http.core;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
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
  private static final int MAX_INPUT_SIZE = 128 * 1024;

  private ServerBootstrap bootstrap;
  private Channel channel;
  private InetSocketAddress bindAddress;

  private final int threadPoolSize;
  private final long threadKeepAliveSecs;

  private final Set<HttpHandler> httpHandlers;
  private final HandlerContext handlerContext;
  private final ChannelGroup channelGroup;

  private static final int CLOSE_CHANNEL_TIMEOUT = 5;
  private static final int WORKER_THREAD_POOL_SIZE = 10;

  private HttpResourceHandler resourceHandler;


  /**
   * Initialize NettyHttpService.
   *
   * @param bindAddress Address for the service to bind to.
   * @param threadPoolSize Size of the thread pool for the executor.
   * @param threadKeepAliveSecs  maximum time that excess idle threads will wait for new tasks before terminating.
   * @param httpHandlers HttpHandlers to handle the calls.
   */
  private NettyHttpService(InetSocketAddress bindAddress, int threadPoolSize,
                           long threadKeepAliveSecs, Iterable<HttpHandler> httpHandlers){
    this.bindAddress = bindAddress;
    this.threadPoolSize = threadPoolSize;
    this.threadKeepAliveSecs = threadKeepAliveSecs;
    this.httpHandlers = ImmutableSet.copyOf(httpHandlers);
    this.handlerContext = new DummyHandlerContext();
    this.channelGroup = new DefaultChannelGroup();
  }

  /**
   * Create Execution handlers with threadPoolExecutor.
   *
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
   *
   * @param threadPoolSize Size of threadpool in threadpoolExecutor
   * @param threadKeepAliveSecs  maximum time that excess idle threads will wait for new tasks before terminating.
   * @param httpHandlers Handlers for httpRequests.
   */
  private void bootStrap(int threadPoolSize, long threadKeepAliveSecs, Iterable<HttpHandler> httpHandlers){

    final ExecutionHandler executionHandler = createExecutionHandler(threadPoolSize, threadKeepAliveSecs);

    Executor bossExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                                                                .setDaemon(true)
                                                                .setNameFormat("boss-thread")
                                                                .build());

    Executor workerExecutor = Executors.newFixedThreadPool(WORKER_THREAD_POOL_SIZE, new ThreadFactoryBuilder()
                                                                                        .setDaemon(true)
                                                                                        .setNameFormat("worker-thread")
                                                                                        .build());

    //Server bootstrap with default worker threads (2 * number of cores)
    bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(bossExecutor, workerExecutor));

    resourceHandler = new HttpResourceHandler(httpHandlers);
    resourceHandler.init(handlerContext);

    final ChannelUpstreamHandler connectionTracker =  new SimpleChannelUpstreamHandler() {
                                                  @Override
                                                  public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e)
                                                    throws Exception {
                                                    channelGroup.add(e.getChannel());
                                                    super.handleUpstream(ctx, e);
                                                  }
                                                };

    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();

        pipeline.addLast("tracker", connectionTracker);
        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("aggregator", new HttpChunkAggregator(MAX_INPUT_SIZE));
        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("compressor", new HttpContentCompressor());
        pipeline.addLast("executor", executionHandler);
        //TODO: Add chunker
        pipeline.addLast("dispatcher", new HttpDispatcher(resourceHandler));

        return pipeline;
      }
    });
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting service on address {}", bindAddress);
    bootStrap(threadPoolSize, threadKeepAliveSecs, httpHandlers);
    channel = bootstrap.bind(bindAddress);
    channelGroup.add(channel);
    bindAddress = ((InetSocketAddress) channel.getLocalAddress());

  }

  /**
   * @return port where the service is running.
   */
  public InetSocketAddress getBindAddress() {
    return bindAddress;
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping service on address {}", bindAddress);
    try {
      if (!channelGroup.close().await(CLOSE_CHANNEL_TIMEOUT, TimeUnit.SECONDS)) {
        LOG.warn("Timeout when closing all channels.");
      }
    } finally {
      resourceHandler.destroy(handlerContext);
      bootstrap.releaseExternalResources();
    }
  }

  /**
   * Builder to help create the NettyHttpService.
   */
  public static class Builder {

    private static final int DEFAULT_THREAD_POOL_SIZE = 60;
    private static final long DEFAULT_THREAD_KEEP_ALIVE_TIME_SECS = 60L;

    //Private constructor to prevent instantiating Builder instance directly.
    private Builder() {
      threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
      threadKeepAliveSecs = DEFAULT_THREAD_KEEP_ALIVE_TIME_SECS;
      port = 0;
    }

    private Iterable<HttpHandler> handlers;
    private int threadPoolSize;
    private String host;
    private int port;
    private long threadKeepAliveSecs;

    /**
     * Add HttpHandlers that service the request.
     * @param handlers Iterable of HttpHandlers.
     * @return instance of {@code Builder}.
     */
    public Builder addHttpHandlers(Iterable<HttpHandler> handlers) {
      this.handlers = handlers;
      return this;
    }

    public Builder setThreadPoolSize(int threadPoolSize) {
      this.threadPoolSize = threadPoolSize;
      return this;
    }

    public Builder setThreadKeepAliveSeconds(long threadKeepAliveSecs) {
      this.threadKeepAliveSecs = threadKeepAliveSecs;
      return this;
    }

    /**
     * Set the port on which the service should listen to.
     * By default the service will run on a random port.
     * @param port port on which the service should listen to.
     * @return instance of {@code Builder}.
     */
    public Builder setPort(int port) {
      this.port = port;
      return this;
    }

    public Builder setHost(String host) {
      this.host = host;
      return this;
    }

    public NettyHttpService build() {
      InetSocketAddress bindAddress;
      if (host == null) {
        bindAddress = new InetSocketAddress("localhost", port);
      } else {
        bindAddress = new InetSocketAddress(host, port);
      }

      return new NettyHttpService(bindAddress, threadPoolSize, threadKeepAliveSecs, handlers);
    }
  }
}
