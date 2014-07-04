package com.continuuity.gateway.router;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.gateway.router.handlers.HttpRequestHandler;
import com.continuuity.gateway.router.handlers.SecurityAuthenticationHttpHandler;
import com.continuuity.security.auth.AccessTokenTransformer;
import com.continuuity.security.auth.TokenValidator;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.discovery.DiscoveryServiceClient;
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
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpRequestEncoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Set;
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
  private final Set<String> forwards; // format port:service

  private final ChannelGroup channelGroup = new DefaultChannelGroup("server channels");
  private final RouterServiceLookup serviceLookup;

  private final boolean securityEnabled;
  private final TokenValidator tokenValidator;
  private final AccessTokenTransformer accessTokenTransformer;
  private final CConfiguration configuration;
  private final String realm;

  private ServerBootstrap serverBootstrap;
  private ClientBootstrap clientBootstrap;

  private DiscoveryServiceClient discoveryServiceClient;

  @Inject
  public NettyRouter(CConfiguration cConf, @Named(Constants.Router.ADDRESS) InetAddress hostname,
                     RouterServiceLookup serviceLookup, TokenValidator tokenValidator,
                     AccessTokenTransformer accessTokenTransformer,
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
    this.forwards = Sets.newHashSet(cConf.getStrings(Constants.Router.FORWARD, Constants.Router.DEFAULT_FORWARD));
    Preconditions.checkState(!this.forwards.isEmpty(), "Require at least one forward rule for router to start");
    LOG.info("Forwards - {}", this.forwards);

    this.serviceLookup = serviceLookup;
    this.securityEnabled = cConf.getBoolean(Constants.Security.CFG_SECURITY_ENABLED, false);
    this.realm = cConf.get(Constants.Security.CFG_REALM);
    this.tokenValidator = tokenValidator;
    this.accessTokenTransformer = accessTokenTransformer;
    this.discoveryServiceClient = discoveryServiceClient;
    this.configuration = cConf;
  }

  @Override
  protected void startUp() throws Exception {
    ChannelUpstreamHandler connectionTracker =  new SimpleChannelUpstreamHandler() {
      @Override
      public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e)
        throws Exception {
        channelGroup.add(e.getChannel());
        super.handleUpstream(ctx, e);
      }
    };

    tokenValidator.startAndWait();
    bootstrapClient(connectionTracker);

    bootstrapServer(connectionTracker);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping Netty Router...");

    try {
      if (!channelGroup.close().await(CLOSE_CHANNEL_TIMEOUT_SECS, TimeUnit.SECONDS)) {
        LOG.warn("Timeout when closing all channels.");
      }
    } finally {
      serverBootstrap.shutdown();
      clientBootstrap.shutdown();
      clientBootstrap.releaseExternalResources();
      serverBootstrap.releaseExternalResources();
      tokenValidator.stopAndWait();
    }

    LOG.info("Stopped Netty Router.");
  }

  public RouterServiceLookup getServiceLookup() {
    return serviceLookup;
  }

  private ExecutorService createExecutorService(int threadPoolSize, String name) {
    return Executors.newFixedThreadPool(threadPoolSize,
                                        new ThreadFactoryBuilder()
                                          .setDaemon(true)
                                          .setNameFormat(name)
                                          .build());
  }

  private void bootstrapServer(final ChannelUpstreamHandler connectionTracker) {
    ExecutorService serverBossExecutor = createExecutorService(serverBossThreadPoolSize,
                                                               "router-server-boss-thread-%d");
    ExecutorService serverWorkerExecutor = createExecutorService(serverWorkerThreadPoolSize,
                                                                 "router-server-worker-thread-%d");
    serverBootstrap = new ServerBootstrap(
      new NioServerSocketChannelFactory(serverBossExecutor, serverWorkerExecutor));
    serverBootstrap.setOption("backlog", serverConnectionBacklog);
    serverBootstrap.setOption("child.bufferFactory", new DirectChannelBufferFactory());

    // Setup the pipeline factory
    serverBootstrap.setPipelineFactory(
      new ChannelPipelineFactory() {
        @Override
        public ChannelPipeline getPipeline() throws Exception {
          ChannelPipeline pipeline = Channels.pipeline();
          pipeline.addLast("tracker", connectionTracker);
          pipeline.addLast("http-response-encoder", new HttpResponseEncoder());
          pipeline.addLast("http-decoder", new HttpRequestDecoder());
          if (securityEnabled) {
            pipeline.addLast("access-token-authenticator", new SecurityAuthenticationHttpHandler(realm, tokenValidator,
                                                                                    configuration,
                                                                                    accessTokenTransformer,
                                                                                    discoveryServiceClient));
          }
          // for now there's only one hardcoded rule, but if there will be more, we may want it generic and configurable
          pipeline.addLast("http-request-handler",
                           new HttpRequestHandler(clientBootstrap, serviceLookup,
                                                  ImmutableList.<ProxyRule>of(new DatasetsProxyRule(configuration))));
          return pipeline;
        }
      }
    );

    InetAddress address = hostname;
    if (address.isAnyLocalAddress()) {
      try {
        address = InetAddress.getLocalHost();
      } catch (UnknownHostException e) {
        throw Throwables.propagate(e);
      }
    }

    // Start listening on ports.
    ImmutableMap.Builder<Integer, String> serviceMapBuilder = ImmutableMap.builder();
    for (String forward : forwards) {
      int ind = forward.indexOf(':');
      int port = Integer.parseInt(forward.substring(0, ind));
      String service = forward.substring(ind + 1);

      String boundService = serviceLookup.getService(port);
      if (boundService != null) {
        LOG.warn("Port {} is already configured to service {}, ignoring forward for service {}",
                 port, boundService, service);
        continue;
      }

      InetSocketAddress bindAddress = new InetSocketAddress(address.getCanonicalHostName(), port);
      LOG.info("Starting Netty Router for service {} on address {}...", service, bindAddress);
      Channel channel = serverBootstrap.bind(bindAddress);
      InetSocketAddress boundAddress = (InetSocketAddress) channel.getLocalAddress();
      serviceMapBuilder.put(boundAddress.getPort(), service);
      channelGroup.add(channel);

      // Update service map
      serviceLookup.updateServiceMap(serviceMapBuilder.build());

      LOG.info("Started Netty Router for service {} on address {}.", service, boundAddress);
    }
  }

  private void bootstrapClient(final ChannelUpstreamHandler connectionTracker) {
    ExecutorService clientBossExecutor = createExecutorService(clientBossThreadPoolSize,
                                                               "router-client-boss-thread-%d");
    ExecutorService clientWorkerExecutor = createExecutorService(clientWorkerThreadPoolSize,
                                                                 "router-client-worker-thread-%d");

    clientBootstrap = new ClientBootstrap(
      new NioClientSocketChannelFactory(
        new NioClientBossPool(clientBossExecutor, clientBossThreadPoolSize),
        new ShareableWorkerPool<NioWorker>(new NioWorkerPool(clientWorkerExecutor, clientWorkerThreadPoolSize))));

    clientBootstrap.setPipelineFactory(
      new ChannelPipelineFactory() {
        @Override
        public ChannelPipeline getPipeline() throws Exception {
          ChannelPipeline pipeline = Channels.pipeline();
          pipeline.addLast("tracker", connectionTracker);
          pipeline.addLast("request-encoder", new HttpRequestEncoder());
          return pipeline;
        }
      }
    );

    clientBootstrap.setOption("bufferFactory", new DirectChannelBufferFactory());
  }
}
