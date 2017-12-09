/*
 * Copyright © 2016-2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.gateway.router;

import co.cask.cdap.common.ServiceBindException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.gateway.router.handlers.AuditLogHandler;
import co.cask.cdap.gateway.router.handlers.AuthenticationHandler;
import co.cask.cdap.gateway.router.handlers.HttpRequestRouter;
import co.cask.cdap.gateway.router.handlers.HttpStatusRequestHandler;
import co.cask.cdap.security.auth.AccessTokenTransformer;
import co.cask.cdap.security.auth.TokenValidator;
import co.cask.http.SSLConfig;
import co.cask.http.SSLHandlerFactory;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Proxies request to a set of servers. Experimental.
 */
public class NettyRouter extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(NettyRouter.class);

  private final int serverBossThreadPoolSize;
  private final int serverWorkerThreadPoolSize;
  private final int serverConnectionBacklog;
  private final InetAddress hostname;
  private final Map<String, Integer> serviceToPortMap;
  private final RouterServiceLookup serviceLookup;
  private final boolean securityEnabled;
  private final TokenValidator tokenValidator;
  private final AccessTokenTransformer accessTokenTransformer;
  private final CConfiguration cConf;
  private final boolean sslEnabled;
  private final SSLHandlerFactory sslHandlerFactory;

  private DiscoveryServiceClient discoveryServiceClient;
  private Cancellable serverCancellable;

  @Inject
  public NettyRouter(CConfiguration cConf, SConfiguration sConf, @Named(Constants.Router.ADDRESS) InetAddress hostname,
                     RouterServiceLookup serviceLookup, TokenValidator tokenValidator,
                     AccessTokenTransformer accessTokenTransformer,
                     DiscoveryServiceClient discoveryServiceClient) {
    this.cConf = cConf;
    this.serverBossThreadPoolSize = cConf.getInt(Constants.Router.SERVER_BOSS_THREADS);
    this.serverWorkerThreadPoolSize = cConf.getInt(Constants.Router.SERVER_WORKER_THREADS);
    this.serverConnectionBacklog = cConf.getInt(Constants.Router.BACKLOG_CONNECTIONS);
    this.hostname = hostname;
    this.serviceToPortMap = new HashMap<>();
    this.serviceLookup = serviceLookup;
    this.securityEnabled = cConf.getBoolean(Constants.Security.ENABLED, false);
    this.tokenValidator = tokenValidator;
    this.accessTokenTransformer = accessTokenTransformer;
    this.discoveryServiceClient = discoveryServiceClient;
    this.sslEnabled = cConf.getBoolean(Constants.Security.SSL.EXTERNAL_ENABLED);
    if (isSSLEnabled()) {
      this.serviceToPortMap.put(Constants.Router.GATEWAY_DISCOVERY_NAME,
                                cConf.getInt(Constants.Router.ROUTER_SSL_PORT));

      File keystore;
      try {
        keystore = new File(sConf.get(Constants.Security.Router.SSL_KEYSTORE_PATH));
      } catch (Throwable e) {
        throw new RuntimeException("SSL is enabled but the keystore file could not be read. Please verify that the " +
                                     "keystore file exists and the path is set correctly : "
                                     + sConf.get(Constants.Security.Router.SSL_KEYSTORE_PATH));
      }
      SSLConfig sslConfig = SSLConfig.builder(keystore, sConf.get(Constants.Security.Router.SSL_KEYSTORE_PASSWORD))
        .setCertificatePassword(sConf.get(Constants.Security.Router.SSL_KEYPASSWORD))
        .build();

      this.sslHandlerFactory = new SSLHandlerFactory(sslConfig);
    } else {
      this.serviceToPortMap.put(Constants.Router.GATEWAY_DISCOVERY_NAME, cConf.getInt(Constants.Router.ROUTER_PORT));
      this.sslHandlerFactory = null;
    }
    LOG.info("Service to Port Mapping - {}", this.serviceToPortMap);
  }

  @Override
  protected void startUp() throws Exception {
    tokenValidator.startAndWait();
    ChannelGroup channelGroup = new DefaultChannelGroup(ImmediateEventExecutor.INSTANCE);
    serverCancellable = startServer(createServerBootstrap(channelGroup), channelGroup);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping Netty Router...");

    serverCancellable.cancel();
    tokenValidator.stopAndWait();

    LOG.info("Stopped Netty Router.");
  }

  /** @noinspection NullableProblems */
  @Override
  protected Executor executor(final State state) {
    final AtomicInteger id = new AtomicInteger();
    return new Executor() {
      @Override
      public void execute(Runnable runnable) {
        Thread t = new Thread(runnable, String.format("NettyRouter-%d", id.incrementAndGet()));
        t.start();
      }
    };
  }

  public RouterServiceLookup getServiceLookup() {
    return serviceLookup;
  }

  private EventLoopGroup createEventLoopGroup(int size, String nameFormat) {
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(nameFormat).build();
    return new NioEventLoopGroup(size, threadFactory);
  }

  private ServerBootstrap createServerBootstrap(final ChannelGroup channelGroup) throws ServiceBindException {
    EventLoopGroup bossGroup = createEventLoopGroup(serverBossThreadPoolSize, "router-server-boss-thread-%d");
    EventLoopGroup workerGroup = createEventLoopGroup(serverWorkerThreadPoolSize, "router-server-worker-thread-%d");

    return new ServerBootstrap()
      .group(bossGroup, workerGroup)
      .channel(NioServerSocketChannel.class)
      .option(ChannelOption.SO_BACKLOG, serverConnectionBacklog)
      .childHandler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
          channelGroup.add(ch);
          ChannelPipeline pipeline = ch.pipeline();
          if (isSSLEnabled()) {
            pipeline.addLast("ssl", sslHandlerFactory.create(ch.alloc()));
          }
          pipeline.addLast("http-codec", new HttpServerCodec());
          pipeline.addLast("http-status-request-handler", new HttpStatusRequestHandler());
          if (securityEnabled) {
            pipeline.addLast("access-token-authenticator",
                             new AuthenticationHandler(cConf, tokenValidator,
                                                       discoveryServiceClient, accessTokenTransformer));
          }
          if (cConf.getBoolean(Constants.Router.ROUTER_AUDIT_LOG_ENABLED)) {
            pipeline.addLast("audit-log", new AuditLogHandler());
          }
          // Always let the client to continue sending the request body after the authentication passed
          pipeline.addLast("expect-continue", new HttpServerExpectContinueHandler());
          // for now there's only one hardcoded rule, but if there will be more, we may want it generic and configurable
          pipeline.addLast("http-request-handler", new HttpRequestRouter(cConf, serviceLookup));
        }
      });
  }

  private Cancellable startServer(final ServerBootstrap serverBootstrap,
                                  final ChannelGroup channelGroup) throws Exception {
    // Start listening on ports.
    Map<Integer, String> serviceMap = new HashMap<>();
    for (Map.Entry<String, Integer> forward : serviceToPortMap.entrySet()) {
      int port = forward.getValue();
      String service = forward.getKey();

      String boundService = serviceLookup.getService(port);
      if (boundService != null) {
        LOG.warn("Port {} is already configured to service {}, ignoring forward for service {}",
                 port, boundService, service);
        continue;
      }

      InetSocketAddress bindAddress = new InetSocketAddress(hostname, port);
      LOG.info("Starting Netty Router for service {} on address {}...", service, bindAddress);

      try {
        Channel channel = serverBootstrap.bind(bindAddress).sync().channel();
        channelGroup.add(channel);
        InetSocketAddress boundAddress = (InetSocketAddress) channel.localAddress();
        serviceMap.put(boundAddress.getPort(), service);

        // Update service map
        serviceLookup.updateServiceMap(serviceMap);

        LOG.info("Started Netty Router for service {} on address {}.", service, boundAddress);
      } catch (Exception e) {
        if ((Throwables.getRootCause(e) instanceof BindException)) {
          throw new ServiceBindException("Router", hostname.getCanonicalHostName(), port, e);
        }

        throw e;
      }
    }

    return new Cancellable() {
      @Override
      public void cancel() {
        List<Future<?>> futures = new ArrayList<>();
        futures.add(channelGroup.close());
        futures.add(serverBootstrap.config().group().shutdownGracefully(0, 5, TimeUnit.SECONDS));
        futures.add(serverBootstrap.config().childGroup().shutdownGracefully(0, 5, TimeUnit.SECONDS));

        for (Future<?> future : futures) {
          future.awaitUninterruptibly();
        }
      }
    };
  }

  private boolean isSSLEnabled() {
    return sslEnabled;
  }
}
