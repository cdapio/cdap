package com.continuuity.gateway.router;

import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.gateway.router.handlers.InboundHandler;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.jboss.netty.bootstrap.ClientBootstrap;
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
import org.jboss.netty.channel.socket.nio.NioClientBossPool;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioWorker;
import org.jboss.netty.channel.socket.nio.NioWorkerPool;
import org.jboss.netty.channel.socket.nio.ShareableWorkerPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Proxies request to a set of servers.
 */
public class NettyRouter extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(NettyRouter.class);
  private static final int CLOSE_CHANNEL_TIMEOUT_SECS = 10;

  private final int serverBossThreadPoolSize;
  private final int serverWorkerThreadPoolSize;
  private final int serverConnectionBacklog;

  private final int clientBossThreadPoolSize;
  private final int clientWorkerThreadPoolSize;
  private final int clientConnectionBacklog;
  private final InetSocketAddress bindAddress;
  private final EndpointStrategy discoverableEndpoints;

  private final ChannelGroup channelGroup = new DefaultChannelGroup("server channels");

  private volatile ServerBootstrap serverBootstrap;
  private volatile ExecutorService serverBossExecutor;
  private volatile ExecutorService serverWorkerExecutor;

  private volatile ClientBootstrap clientBootstrap;
  private volatile ExecutorService clientBossExecutor;
  private volatile ExecutorService clientWorkerExecutor;
  private volatile InetSocketAddress boundAddress;

  public NettyRouter(int serverBossThreadPoolSize, int serverWorkerThreadPoolSize, int serverConnectionBacklog,
                     int clientBossThreadPoolSize, int clientWorkerThreadPoolSize, int clientConnectionBacklog,
                     InetSocketAddress bindAddress, EndpointStrategy discoverableEndpoints) {
    this.serverBossThreadPoolSize = serverBossThreadPoolSize;
    this.serverWorkerThreadPoolSize = serverWorkerThreadPoolSize;
    this.serverConnectionBacklog = serverConnectionBacklog;
    this.clientBossThreadPoolSize = clientBossThreadPoolSize;
    this.clientWorkerThreadPoolSize = clientWorkerThreadPoolSize;
    this.clientConnectionBacklog = clientConnectionBacklog;
    this.bindAddress = bindAddress;
    this.discoverableEndpoints = discoverableEndpoints;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting Netty Router on address {}...", bindAddress);

    serverBossExecutor = createExecutorService(serverBossThreadPoolSize, "router-server-boss-thread-%d");
    serverWorkerExecutor = createExecutorService(serverWorkerThreadPoolSize, "router-server-worker-thread-%d");
    clientBossExecutor = createExecutorService(clientBossThreadPoolSize, "router-client-boss-thread-%d");
    clientWorkerExecutor = createExecutorService(clientWorkerThreadPoolSize, "router-client-worker-thread-%d");

    serverBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(serverBossExecutor, serverWorkerExecutor));
    serverBootstrap.setOption("backlog", serverConnectionBacklog);

    clientBootstrap = new ClientBootstrap(
      new NioClientSocketChannelFactory(
        new NioClientBossPool(clientBossExecutor, clientBossThreadPoolSize),
        new ShareableWorkerPool<NioWorker>(new NioWorkerPool(clientWorkerExecutor, clientWorkerThreadPoolSize))));
    serverBootstrap.setOption("backlog", clientConnectionBacklog);

    final ChannelUpstreamHandler connectionTracker =  new SimpleChannelUpstreamHandler() {
      @Override
      public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e)
        throws Exception {
        channelGroup.add(e.getChannel());
        super.handleUpstream(ctx, e);
      }
    };

    serverBootstrap.setPipelineFactory(
      new ChannelPipelineFactory() {
        @Override
        public ChannelPipeline getPipeline() throws Exception {
          ChannelPipeline pipeline = Channels.pipeline();
          pipeline.addLast("tracker", connectionTracker);
          pipeline.addLast("inbound-handler", new InboundHandler(clientBootstrap, discoverableEndpoints));
          return pipeline;
        }
      }
    );

    clientBootstrap.setPipelineFactory(
      new ChannelPipelineFactory() {
        @Override
        public ChannelPipeline getPipeline() throws Exception {
          ChannelPipeline pipeline = Channels.pipeline();
          pipeline.addLast("tracker", connectionTracker);
          return pipeline;
        }
      }
    );

    Channel channel = serverBootstrap.bind(bindAddress);
    channelGroup.add(channel);

    boundAddress = (InetSocketAddress) channel.getLocalAddress();
    LOG.info("Started Netty Router on address {}.", boundAddress);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping Netty Router...");

    serverBootstrap.shutdown();
    try {
      if (!channelGroup.close().await(CLOSE_CHANNEL_TIMEOUT_SECS, TimeUnit.SECONDS)) {
        LOG.warn("Timeout when closing all channels.");
      }
    } finally {
      clientBootstrap.shutdown();
      clientBootstrap.releaseExternalResources();
      serverBootstrap.releaseExternalResources();

      serverBossExecutor.shutdown();
      serverWorkerExecutor.shutdown();

      clientBossExecutor.shutdown();
      clientWorkerExecutor.shutdown();
    }

    LOG.info("Stopped Netty Router.");
  }

  public int getPort() {
    return boundAddress.getPort();
  }

  private ExecutorService createExecutorService(int threadPoolSize, String name) {
    return Executors.newFixedThreadPool(threadPoolSize,
                                        new ThreadFactoryBuilder()
                                          .setDaemon(true)
                                          .setNameFormat(name)
                                          .build());
  }
}
