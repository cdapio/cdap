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

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
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
 * Runs the centralized netty proxy that fronts the task worker pool.
 *
 * <p>On an RBAC instance, user code from different namespaces must never share a task worker JVM.
 * This service sits between App Fabric and the task workers, reads the
 * {@link Constants.Gateway#HEADER_CDAP_NAMESPACE} header off each inbound request, and asks
 * {@link PodLeaseManager} for a pod that is either already warm for that namespace or safe to claim.
 * It then streams the request straight through to that pod.
 *
 * <p>The pipeline is deliberately only {@link HttpServerCodec} plus {@link ProxyFrontendHandler}.
 * There is no {@code HttpObjectAggregator}: task requests carry serialized pipeline specs that can
 * run to many megabytes, and aggregating them would mean buffering every in-flight request body on
 * the proxy heap. Without it, {@link io.netty.buffer.ByteBuf} chunks are forwarded as they arrive
 * and the proxy's memory stays flat regardless of payload size.
 */
public class TaskWorkerManagerService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(TaskWorkerManagerService.class);

  private final String bindAddress;
  private final int bindPort;
  private final int bossThreads;
  private final int workerThreads;
  private final PodLeaseManager podLeaseManager;
  private final DiscoveryService discoveryService;
  private final DiscoveryServiceClient discoveryServiceClient;

  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private Channel serverChannel;
  private Cancellable cancelDiscovery;

  @Inject
  TaskWorkerManagerService(CConfiguration cConf, DiscoveryService discoveryService,
      DiscoveryServiceClient discoveryServiceClient) {
    this.bindAddress = cConf.get(Constants.TaskWorkerManager.ADDRESS);
    this.bindPort = cConf.getInt(Constants.TaskWorkerManager.PORT);
    this.bossThreads = cConf.getInt(Constants.TaskWorkerManager.BOSS_THREADS);
    this.workerThreads = cConf.getInt(Constants.TaskWorkerManager.WORKER_THREADS);
    this.podLeaseManager = new PodLeaseManager(cConf);
    this.discoveryService = discoveryService;
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.debug("Starting TaskWorkerManagerService on {}:{}", bindAddress, bindPort);

    // Warm the task worker discovery cache before we accept traffic. Twill populates its
    // discoverables asynchronously, so touching it here means the first request through the proxy
    // sees a populated pod registry instead of an empty one and an immediate HTTP 429.
    discoveryServiceClient.discover(Constants.Service.TASK_WORKER);

    bossGroup = new NioEventLoopGroup(bossThreads,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("task-worker-manager-boss-%d").build());
    workerGroup = new NioEventLoopGroup(workerThreads,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("task-worker-manager-worker-%d").build());

    ServerBootstrap bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast(new HttpServerCodec());
            // No HttpObjectAggregator here on purpose: see the class javadoc.
            pipeline.addLast(new ProxyFrontendHandler(podLeaseManager, discoveryServiceClient));
          }
        });

    serverChannel = bootstrap.bind(bindAddress, bindPort).sync().channel();

    // Announce only after the socket is listening, so nothing can be routed here before we can
    // serve it. Registering the channel's own local address rather than the configured one picks up
    // an ephemeral port when the configured port is 0.
    InetSocketAddress boundAddress = (InetSocketAddress) serverChannel.localAddress();
    cancelDiscovery = discoveryService.register(ResolvingDiscoverable.of(
        URIScheme.HTTP.createDiscoverable(Constants.Service.TASK_WORKER_MANAGER, boundAddress)));

    LOG.info("TaskWorkerManagerService netty proxy started on {}", boundAddress);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.debug("Stopping TaskWorkerManagerService");

    // Deregister first so App Fabric stops resolving this pod, then drain and close channels.
    if (cancelDiscovery != null) {
      cancelDiscovery.cancel();
    }

    List<Future<?>> futures = new ArrayList<>();
    if (serverChannel != null) {
      futures.add(serverChannel.close());
    }
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
