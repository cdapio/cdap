/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.http.core;

import com.google.common.collect.ImmutableList;
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
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
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
  private static final int MAX_INPUT_SIZE = 1024 * 1024 * 1024;

  private ServerBootstrap bootstrap;
  private int bossThreadPoolSize;
  private int workerThreadPoolSize;
  private int connectionBacklog;
  private final int execThreadPoolSize;
  private final long execThreadKeepAliveSecs;
  private final RejectedExecutionHandler rejectedExecutionHandler;
  private InetSocketAddress bindAddress;

  private final Set<HttpHandler> httpHandlers;
  private final List<HandlerHook> handlerHooks;
  private final HandlerContext handlerContext;
  private final ChannelGroup channelGroup;

  private static final int CLOSE_CHANNEL_TIMEOUT = 5;

  private HttpResourceHandler resourceHandler;


  /**
   * Initialize NettyHttpService.
   *
   * @param bindAddress Address for the service to bind to.
   * @param bossThreadPoolSize Size of the boss thread pool.
   * @param workerThreadPoolSize Size of the worker thread pool.
   * @param connectionBacklog Max concurrent connections that can be queued.
   * @param execThreadPoolSize Size of the thread pool for the executor.
   * @param execThreadKeepAliveSecs  maximum time that excess idle threads will wait for new tasks before terminating.
   * @param rejectedExecutionHandler rejection policy for executor.
   * @param httpHandlers HttpHandlers to handle the calls.
   */
  public NettyHttpService(InetSocketAddress bindAddress, int bossThreadPoolSize, int workerThreadPoolSize,
                          int connectionBacklog,
                          int execThreadPoolSize, long execThreadKeepAliveSecs,
                          RejectedExecutionHandler rejectedExecutionHandler,
                          Iterable<? extends HttpHandler> httpHandlers,
                          Iterable<? extends HandlerHook> handlerHooks){
    this.bindAddress = bindAddress;
    this.bossThreadPoolSize = bossThreadPoolSize;
    this.workerThreadPoolSize = workerThreadPoolSize;
    this.connectionBacklog = connectionBacklog;
    this.execThreadPoolSize = execThreadPoolSize;
    this.execThreadKeepAliveSecs = execThreadKeepAliveSecs;
    this.rejectedExecutionHandler = rejectedExecutionHandler;
    this.httpHandlers = ImmutableSet.copyOf(httpHandlers);
    this.handlerContext = new DummyHandlerContext();
    this.channelGroup = new DefaultChannelGroup();
    this.handlerHooks = ImmutableList.copyOf(handlerHooks);
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
    ThreadPoolExecutor threadPoolExecutor =
      new OrderedMemoryAwareThreadPoolExecutor(threadPoolSize, 0, 0, threadKeepAliveSecs, TimeUnit.SECONDS,
                                               threadFactory);
    threadPoolExecutor.setRejectedExecutionHandler(rejectedExecutionHandler);
    return new ExecutionHandler(threadPoolExecutor);
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

    Executor bossExecutor = Executors.newFixedThreadPool(bossThreadPoolSize,
                                                         new ThreadFactoryBuilder()
                                                           .setDaemon(true)
                                                           .setNameFormat("boss-thread")
                                                           .build());

    Executor workerExecutor = Executors.newFixedThreadPool(workerThreadPoolSize, new ThreadFactoryBuilder()
                                                                                        .setDaemon(true)
                                                                                        .setNameFormat("worker-thread")
                                                                                        .build());

    //Server bootstrap with default worker threads (2 * number of cores)
    bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(bossExecutor, bossThreadPoolSize,
                                                                      workerExecutor, workerThreadPoolSize));
    bootstrap.setOption("backlog", connectionBacklog);

    resourceHandler = new HttpResourceHandler(httpHandlers, handlerHooks);
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
    LOG.info("Starting service on address {}...", bindAddress);
    bootStrap(execThreadPoolSize, execThreadKeepAliveSecs, httpHandlers);
    Channel channel = bootstrap.bind(bindAddress);
    channelGroup.add(channel);
    bindAddress = ((InetSocketAddress) channel.getLocalAddress());
    LOG.info("Started service on address {}", bindAddress);
  }

  /**
   * @return port where the service is running.
   */
  public InetSocketAddress getBindAddress() {
    return bindAddress;
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping service on address {}...", bindAddress);
    bootstrap.shutdown();
    try {
      if (!channelGroup.close().await(CLOSE_CHANNEL_TIMEOUT, TimeUnit.SECONDS)) {
        LOG.warn("Timeout when closing all channels.");
      }
    } finally {
      resourceHandler.destroy(handlerContext);
      bootstrap.releaseExternalResources();
    }
    LOG.info("Done stopping service on address {}", bindAddress);
  }

  /**
   * Builder to help create the NettyHttpService.
   */
  public static class Builder {

    private static final int DEFAULT_BOSS_THREAD_POOL_SIZE = 1;
    private static final int DEFAULT_WORKER_THREAD_POOL_SIZE = 10;
    private static final int DEFAULT_CONNECTION_BACKLOG = 1000;
    private static final int DEFAULT_EXEC_HANDLER_THREAD_POOL_SIZE = 60;
    private static final long DEFAULT_EXEC_HANDLER_THREAD_KEEP_ALIVE_TIME_SECS = 60L;
    private static final RejectedExecutionHandler DEFAULT_REJECTED_EXECUTION_HANDLER =
      new ThreadPoolExecutor.CallerRunsPolicy();

    //Private constructor to prevent instantiating Builder instance directly.
    private Builder(){
      bossThreadPoolSize = DEFAULT_BOSS_THREAD_POOL_SIZE;
      workerThreadPoolSize = DEFAULT_WORKER_THREAD_POOL_SIZE;
      connectionBacklog = DEFAULT_CONNECTION_BACKLOG;
      execThreadPoolSize = DEFAULT_EXEC_HANDLER_THREAD_POOL_SIZE;
      execThreadKeepAliveSecs = DEFAULT_EXEC_HANDLER_THREAD_KEEP_ALIVE_TIME_SECS;
      rejectedExecutionHandler = DEFAULT_REJECTED_EXECUTION_HANDLER;
      port = 0;
    }

    private Iterable<? extends HttpHandler> handlers;
    private Iterable<? extends HandlerHook> handlerHooks = ImmutableList.of();
    private int bossThreadPoolSize;
    private int workerThreadPoolSize;
    private int connectionBacklog;
    private int execThreadPoolSize;
    private String host;
    private int port;
    private long execThreadKeepAliveSecs;
    private RejectedExecutionHandler rejectedExecutionHandler;

    /**
     * Add HttpHandlers that service the request.
     * @param handlers Iterable of HttpHandlers.
     * @return instance of {@code Builder}.
     */
    public Builder addHttpHandlers(Iterable<? extends HttpHandler> handlers) {
      this.handlers = handlers;
      return this;
    }

    /**
     * Set HandlerHooks to be executed pre and post handler calls. They are executed in the same order as specified
     * by the iterable.
     * @param handlerHooks Iterable of HandlerHooks.
     * @return instance of {@code Builder}.
     */
    public Builder setHandlerHooks(Iterable<? extends HandlerHook> handlerHooks) {
      this.handlerHooks = handlerHooks;
      return this;
    }

    public Builder setBossThreadPoolSize(int bossThreadPoolSize) {
      this.bossThreadPoolSize = bossThreadPoolSize;
      return this;
    }

    public Builder setWorkerThreadPoolSize(int workerThreadPoolSize) {
      this.workerThreadPoolSize = workerThreadPoolSize;
      return this;
    }

    public Builder setConnectionBacklog(int connectionBacklog) {
      this.connectionBacklog = connectionBacklog;
      return this;
    }

    public Builder setExecThreadPoolSize(int execThreadPoolSize){
      this.execThreadPoolSize = execThreadPoolSize;
      return this;
    }

    public Builder setExecThreadKeepAliveSeconds(long threadKeepAliveSecs){
      this.execThreadKeepAliveSecs = threadKeepAliveSecs;
      return this;
    }

    public Builder setRejectedExecutionHandler(RejectedExecutionHandler rejectedExecutionHandler) {
      this.rejectedExecutionHandler = rejectedExecutionHandler;
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

      return new NettyHttpService(bindAddress, bossThreadPoolSize, workerThreadPoolSize, connectionBacklog,
                                  execThreadPoolSize, execThreadKeepAliveSecs, rejectedExecutionHandler,
                                  handlers, handlerHooks);
    }
  }
}
