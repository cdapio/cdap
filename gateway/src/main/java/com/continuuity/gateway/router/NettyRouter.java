package com.continuuity.gateway.router;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.gateway.router.handlers.InboundHandler;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.DirectChannelBufferFactory;
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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Proxies request to a set of servers. Experimental.
 */
public class NettyRouter extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(NettyRouter.class);
  private static final int CLOSE_CHANNEL_TIMEOUT_SECS = 10;

  private final int serverBossThreadPoolSize;
  private final int serverWorkerThreadPoolSize;
  private final int serverConnectionBacklog;

  private final int clientBossThreadPoolSize;
  private final int clientWorkerThreadPoolSize;
  private final InetAddress hostname;
  private final int port;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final String destinationServiceName;

  private final ChannelGroup channelGroup = new DefaultChannelGroup("server channels");

  private ServerBootstrap serverBootstrap;
  private ClientBootstrap clientBootstrap;
  private InetSocketAddress boundAddress;

  @Inject
  public NettyRouter(CConfiguration cConf, @Named(Constants.Router.ADDRESS) InetAddress hostname,
                     DiscoveryServiceClient discoveryServiceClient) {
    this.serverBossThreadPoolSize = cConf.getInt(Constants.Router.SERVER_BOSS_THREADS,
                                                 Constants.Router.DEFAULT_SERVER_BOSS_THREADS);
    this.serverWorkerThreadPoolSize = cConf.getInt(Constants.Router.SERVER_WORKER_THREADS,
                                                   Constants.Router.DEFAULT_SERVER_WORKER_THREADS);
    this.serverConnectionBacklog = cConf.getInt(Constants.Router.BACKLOG_CONNECTIONS,
                                                Constants.Router.DEFAULT_BACKLOG);

    this.clientBossThreadPoolSize = cConf.getInt(Constants.Router.CLIENT_BOSS_THREADS,
                                                 Constants.Router.DEFAULT_CLIENT_BOSS_THREADS);
    this.clientWorkerThreadPoolSize = cConf.getInt(Constants.Router.CLIENT_WORKER_THREADS,
                                                   Constants.Router.DEFAULT_CLIENT_WORKER_THREADS);

    this.hostname = hostname;
    this.port = cConf.getInt(Constants.Router.PORT, Constants.Router.DEFAULT_PORT);

    this.discoveryServiceClient = discoveryServiceClient;

    this.destinationServiceName = cConf.get(Constants.Router.DEST_SERVICE_NAME,
                                            Constants.Router.DEFAULT_DEST_SERVICE_NAME);
  }

  @Override
  protected void startUp() throws Exception {
    InetAddress address = hostname;
    if (address.isAnyLocalAddress()) {
      try {
        address = InetAddress.getLocalHost();
      } catch (UnknownHostException e) {
        throw Throwables.propagate(e);
      }
    }

    InetSocketAddress bindAddress = new InetSocketAddress(address.getCanonicalHostName(), port);
    LOG.info("Starting Netty Router for service {} on address {}...", destinationServiceName, bindAddress);

    ExecutorService serverBossExecutor = createExecutorService(serverBossThreadPoolSize,
                                                               "router-server-boss-thread-%d");
    ExecutorService serverWorkerExecutor = createExecutorService(serverWorkerThreadPoolSize,
                                                                 "router-server-worker-thread-%d");
    ExecutorService clientBossExecutor = createExecutorService(clientBossThreadPoolSize,
                                                               "router-client-boss-thread-%d");
    ExecutorService clientWorkerExecutor = createExecutorService(clientWorkerThreadPoolSize,
                                                                 "router-client-worker-thread-%d");

    final EndpointStrategy discoverableEndpoints = new RandomEndpointStrategy(
      discoveryServiceClient.discover(destinationServiceName));

    serverBootstrap = new ServerBootstrap(
      new NioServerSocketChannelFactory(serverBossExecutor, serverWorkerExecutor));
    serverBootstrap.setOption("backlog", serverConnectionBacklog);

    clientBootstrap = new ClientBootstrap(
      new NioClientSocketChannelFactory(
        new NioClientBossPool(clientBossExecutor, clientBossThreadPoolSize),
        new ShareableWorkerPool<NioWorker>(new NioWorkerPool(clientWorkerExecutor, clientWorkerThreadPoolSize))));

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
          pipeline.addLast("inbound-handler",
                           new InboundHandler(clientBootstrap, discoverableEndpoints, destinationServiceName));
          return pipeline;
        }
      }
    );

    serverBootstrap.setOption("bufferFactory", new DirectChannelBufferFactory());

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

    clientBootstrap.setOption("bufferFactory", new DirectChannelBufferFactory());

    Channel channel = serverBootstrap.bind(bindAddress);
    channelGroup.add(channel);

    boundAddress = (InetSocketAddress) channel.getLocalAddress();
    LOG.info("Started Netty Router for service {} on address {}.", destinationServiceName, boundAddress);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping Netty Router for service {} on address {}...", destinationServiceName, boundAddress);

    try {
      if (!channelGroup.close().await(CLOSE_CHANNEL_TIMEOUT_SECS, TimeUnit.SECONDS)) {
        LOG.warn("Timeout when closing all channels.");
      }
    } finally {
      serverBootstrap.shutdown();
      clientBootstrap.shutdown();
      clientBootstrap.releaseExternalResources();
      serverBootstrap.releaseExternalResources();
    }

    LOG.info("Stopped Netty Router for service {} on address {}.", destinationServiceName, boundAddress);
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
