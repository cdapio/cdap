/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.common.internal.remote;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.http.SSLHandlerFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.util.concurrent.Future;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Netty proxy that routes task requests to task worker pods leased per namespace, so user code
 * from different namespaces never shares a worker. Requests are streamed, not aggregated, to keep
 * memory flat.
 */
public class TaskWorkerManagerService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(TaskWorkerManagerService.class);

  private final CConfiguration cConf;
  private final SConfiguration sConf;
  private final String bindAddress;
  private final int bindPort;
  private final int bossThreads;
  private final int workerThreads;
  private final boolean internalSslEnabled;
  private final PodLeaseManager podLeaseManager;
  private final DiscoveryService discoveryService;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final ProxyDrainState drainState;
  private final long drainTimeoutMillis;

  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private Channel serverChannel;
  private Cancellable cancelDiscovery;

  @Inject
  TaskWorkerManagerService(CConfiguration cConf, SConfiguration sConf,
      DiscoveryService discoveryService, DiscoveryServiceClient discoveryServiceClient) {
    this(cConf, sConf, discoveryService, discoveryServiceClient, new ProxyDrainState());
  }

  @VisibleForTesting
  TaskWorkerManagerService(CConfiguration cConf, DiscoveryService discoveryService,
      DiscoveryServiceClient discoveryServiceClient, ProxyDrainState drainState) {
    this(cConf, SConfiguration.create(), discoveryService, discoveryServiceClient, drainState);
  }

  @VisibleForTesting
  TaskWorkerManagerService(CConfiguration cConf, SConfiguration sConf,
      DiscoveryService discoveryService, DiscoveryServiceClient discoveryServiceClient,
      ProxyDrainState drainState) {
    this.cConf = cConf;
    this.sConf = sConf;
    this.bindAddress = cConf.get(Constants.TaskWorkerManager.ADDRESS);
    this.bindPort = cConf.getInt(Constants.TaskWorkerManager.PORT);
    this.bossThreads = cConf.getInt(Constants.TaskWorkerManager.BOSS_THREADS);
    this.workerThreads = cConf.getInt(Constants.TaskWorkerManager.WORKER_THREADS);
    this.internalSslEnabled = cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED);
    this.podLeaseManager = new PodLeaseManager(cConf);
    this.discoveryService = discoveryService;
    this.discoveryServiceClient = discoveryServiceClient;
    this.drainState = drainState;
    this.drainTimeoutMillis =
        TimeUnit.SECONDS.toMillis(cConf.getLong(Constants.TaskWorkerManager.DRAIN_TIMEOUT_SECS));
  }

  @Override
  protected void startUp() throws Exception {
    LOG.debug("Starting TaskWorkerManagerService on {}:{} (internalSslEnabled={})",
        bindAddress, bindPort, internalSslEnabled);

    // Warm the discovery cache so the first request doesn't see an empty pod list.
    discoveryServiceClient.discover(Constants.Service.TASK_WORKER);

    // Same keystore setup as TaskWorkerService: when ssl.internal.cert.path is unset in the proxy
    // pod, HttpsEnabler generates a self-signed certificate in memory.
    final SSLHandlerFactory sslHandlerFactory = internalSslEnabled
        ? new HttpsEnabler().configureKeyStore(cConf, sConf).createSSLHandlerFactory()
        : null;

    bossGroup = new NioEventLoopGroup(bossThreads,
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("task-worker-manager-boss-%d").build());
    workerGroup = new NioEventLoopGroup(workerThreads,
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("task-worker-manager-worker-%d").build());

    ServerBootstrap bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) {
            ChannelPipeline pipeline = ch.pipeline();
            if (sslHandlerFactory != null) {
              pipeline.addLast("ssl", sslHandlerFactory.create(ch.alloc()));
            }
            pipeline.addLast(new HttpServerCodec());
            // No HttpObjectAggregator here on purpose: see the class javadoc.
            pipeline.addLast(new ProxyFrontendHandler(
                podLeaseManager, discoveryServiceClient, drainState, internalSslEnabled));
          }
        });

    serverChannel = bootstrap.bind(bindAddress, bindPort).sync().channel();

    // Register only once listening, using the bound address so port 0 works.
    InetSocketAddress boundAddress = (InetSocketAddress) serverChannel.localAddress();
    URIScheme uriScheme = internalSslEnabled ? URIScheme.HTTPS : URIScheme.HTTP;
    cancelDiscovery = discoveryService.register(ResolvingDiscoverable.of(
        uriScheme.createDiscoverable(Constants.Service.TASK_WORKER_MANAGER, boundAddress)));

    LOG.info("TaskWorkerManagerService netty proxy started on {} ({})", boundAddress, uriScheme);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.debug("Stopping TaskWorkerManagerService");

    // Deregister first so App Fabric stops resolving this pod.
    if (cancelDiscovery != null) {
      cancelDiscovery.cancel();
    }
    // Stop accepting connections. Accepted connections stay open, so responses already on their way
    // back from a task worker can still be delivered.
    if (serverChannel != null) {
      serverChannel.close().awaitUninterruptibly();
    }

    // New task requests are answered 503 from here on; App Fabric retries them on the replacement.
    // This thread then waits while the event loops deliver the in-flight responses.
    drainState.startDraining();
    if (drainState.awaitDrained(drainTimeoutMillis)) {
      LOG.info("All in-flight task worker requests finished; stopping the proxy");
    } else {
      LOG.warn("{} task worker request(s) still in flight after {} seconds; closing them",
          drainState.getInFlight(), TimeUnit.MILLISECONDS.toSeconds(drainTimeoutMillis));
    }

    List<Future<?>> futures = new ArrayList<>();
    if (bossGroup != null) {
      futures.add(bossGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS));
    }
    if (workerGroup != null) {
      futures.add(workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS));
    }

    for (Future<?> future : futures) {
      future.awaitUninterruptibly();
    }

    LOG.debug("Stopping TaskWorkerManagerService has completed");
  }
}
