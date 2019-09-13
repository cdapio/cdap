/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.gateway.router;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.common.ServiceBindException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.cdap.common.security.KeyStores;
import io.cdap.cdap.gateway.router.handlers.AuditLogHandler;
import io.cdap.cdap.gateway.router.handlers.AuthenticationHandler;
import io.cdap.cdap.gateway.router.handlers.HttpRequestRouter;
import io.cdap.cdap.gateway.router.handlers.HttpStatusRequestHandler;
import io.cdap.cdap.security.auth.AccessTokenTransformer;
import io.cdap.cdap.security.auth.TokenValidator;
import io.cdap.http.SSLConfig;
import io.cdap.http.SSLHandlerFactory;
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
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Proxies request to a set of servers. Experimental.
 */
public class NettyRouter extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(NettyRouter.class);

  private final CConfiguration cConf;
  private final SConfiguration sConf;
  private final int serverBossThreadPoolSize;
  private final int serverWorkerThreadPoolSize;
  private final int serverConnectionBacklog;
  private final InetAddress hostname;
  private final int port;
  private final RouterServiceLookup serviceLookup;
  private final boolean securityEnabled;
  private final TokenValidator tokenValidator;
  private final AccessTokenTransformer accessTokenTransformer;
  private final boolean sslEnabled;
  private InetSocketAddress boundAddress;

  private DiscoveryServiceClient discoveryServiceClient;
  private Cancellable serverCancellable;

  @Inject
  public NettyRouter(CConfiguration cConf, SConfiguration sConf, @Named(Constants.Router.ADDRESS) InetAddress hostname,
                     RouterServiceLookup serviceLookup, TokenValidator tokenValidator,
                     AccessTokenTransformer accessTokenTransformer,
                     DiscoveryServiceClient discoveryServiceClient) {
    this.cConf = cConf;
    this.sConf = sConf;
    this.serverBossThreadPoolSize = cConf.getInt(Constants.Router.SERVER_BOSS_THREADS);
    this.serverWorkerThreadPoolSize = cConf.getInt(Constants.Router.SERVER_WORKER_THREADS);
    this.serverConnectionBacklog = cConf.getInt(Constants.Router.BACKLOG_CONNECTIONS);
    this.hostname = hostname;
    this.serviceLookup = serviceLookup;
    this.securityEnabled = cConf.getBoolean(Constants.Security.ENABLED, false);
    this.tokenValidator = tokenValidator;
    this.accessTokenTransformer = accessTokenTransformer;
    this.discoveryServiceClient = discoveryServiceClient;
    this.sslEnabled = cConf.getBoolean(Constants.Security.SSL.EXTERNAL_ENABLED);
    this.port = sslEnabled
      ? cConf.getInt(Constants.Router.ROUTER_SSL_PORT)
      : cConf.getInt(Constants.Router.ROUTER_PORT);
  }

  /**
   * Returns an {@link Optional} {@link InetSocketAddress} that this router is bound to. If the router is not
   * running, an empty {@link Optional} will be returned.
   */
  public Optional<InetSocketAddress> getBoundAddress() {
    return Optional.ofNullable(boundAddress);
  }

  @Override
  protected void startUp() throws Exception {
    tokenValidator.startAndWait();
    ChannelGroup channelGroup = new DefaultChannelGroup(ImmediateEventExecutor.INSTANCE);
    serverCancellable = startServer(createServerBootstrap(channelGroup), channelGroup);
  }

  @Override
  protected void shutDown() {
    boundAddress = null;
    LOG.info("Stopping Netty Router...");

    serverCancellable.cancel();
    tokenValidator.stopAndWait();

    LOG.info("Stopped Netty Router.");
  }

  @Override
  protected Executor executor(final State state) {
    final AtomicInteger id = new AtomicInteger();
    return runnable -> {
      Thread t = new Thread(runnable, String.format("NettyRouter-%d", id.incrementAndGet()));
      t.start();
    };
  }

  private EventLoopGroup createEventLoopGroup(int size, String nameFormat) {
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(nameFormat).build();
    return new NioEventLoopGroup(size, threadFactory);
  }

  private ServerBootstrap createServerBootstrap(final ChannelGroup channelGroup) {
    EventLoopGroup bossGroup = createEventLoopGroup(serverBossThreadPoolSize, "router-server-boss-thread-%d");
    EventLoopGroup workerGroup = createEventLoopGroup(serverWorkerThreadPoolSize, "router-server-worker-thread-%d");

    SSLHandlerFactory sslHandlerFactory = null;
    if (sslEnabled) {
      // We support both JKS keystore format and PEM cert file.
      String keyStorePath = sConf.get(Constants.Security.Router.SSL_KEYSTORE_PATH);
      String certFilePath = cConf.get(Constants.Security.Router.SSL_CERT_PATH);

      if (!Strings.isNullOrEmpty(keyStorePath)) {
        SSLConfig sslConfig = SSLConfig
          .builder(new File(keyStorePath), sConf.get(Constants.Security.Router.SSL_KEYSTORE_PASSWORD))
          .setCertificatePassword(sConf.get(Constants.Security.Router.SSL_KEYPASSWORD))
          .build();
        sslHandlerFactory = new SSLHandlerFactory(sslConfig);
      } else if (!Strings.isNullOrEmpty(certFilePath)) {
        String password = sConf.get(Constants.Security.Router.SSL_CERT_PASSWORD, "");
        KeyStore keyStore = KeyStores.createKeyStore(Paths.get(certFilePath), password);
        sslHandlerFactory = new HttpsEnabler().setKeyStore(keyStore, password::toCharArray).createSSLHandlerFactory();
      }

      if (sslHandlerFactory == null) {
        throw new RuntimeException("SSL is enabled but there is no keystore file nor certificate file being " +
                                     "configured. Please ensure either '" + Constants.Security.Router.SSL_KEYSTORE_PATH
                                     + "' is set in cdap-security.xml or '" + Constants.Security.Router.SSL_CERT_PATH
                                     + "' is set in cdap-site.xml file.");
      }
    }

    SSLHandlerFactory finalSSLHandlerFactory = sslHandlerFactory;
    return new ServerBootstrap()
      .group(bossGroup, workerGroup)
      .channel(NioServerSocketChannel.class)
      .option(ChannelOption.SO_BACKLOG, serverConnectionBacklog)
      .childHandler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) {
          channelGroup.add(ch);
          ChannelPipeline pipeline = ch.pipeline();
          if (finalSSLHandlerFactory != null) {
            pipeline.addLast("ssl", finalSSLHandlerFactory.create(ch.alloc()));
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
    InetSocketAddress bindAddress = new InetSocketAddress(hostname, port);
    LOG.info("Starting Netty Router on address {}...", bindAddress);

    try {
      Channel channel = serverBootstrap.bind(bindAddress).sync().channel();
      channelGroup.add(channel);
      boundAddress = (InetSocketAddress) channel.localAddress();
      LOG.info("Started Netty Router on address {}.", boundAddress);
    } catch (Exception e) {
      if ((Throwables.getRootCause(e) instanceof BindException)) {
        throw new ServiceBindException("Router", hostname.getCanonicalHostName(), port, e);
      }

      throw e;
    }

    return () -> {
      List<Future<?>> futures = new ArrayList<>();
      futures.add(channelGroup.close());
      futures.add(serverBootstrap.config().group().shutdownGracefully(0, 5, TimeUnit.SECONDS));
      futures.add(serverBootstrap.config().childGroup().shutdownGracefully(0, 5, TimeUnit.SECONDS));

      for (Future<?> future : futures) {
        future.awaitUninterruptibly();
      }
    };
  }
}
