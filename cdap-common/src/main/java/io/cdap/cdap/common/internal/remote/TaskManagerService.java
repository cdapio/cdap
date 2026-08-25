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

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;

/**
 * TaskManagerService runs the Centralized Netty Proxy server inside the Task Manager pod.
 *
 * <p>Architectural Role:
 * <ul>
 *   <li>Binds an internal TCP port (default {@code 11025}) to receive Studio validation
 *       and pipeline deployment requests from AppFabric.</li>
 *   <li>Sets up a raw Netty 4 {@link io.netty.bootstrap.ServerBootstrap} with dedicated Boss
 *       (acceptor) and Worker (I/O) event loop groups.</li>
 *   <li>Configures the {@link io.netty.channel.ChannelPipeline} with {@link io.netty.handler.codec.http.HttpServerCodec}
 *       and {@link ProxyFrontendHandler}.</li>
 *   <li>NOTE: This service intentionally does NOT install {@code HttpObjectAggregator}.
 *       Omitting the aggregator enables zero-copy, streaming {@link io.netty.buffer.ByteBuf} forwarding
 *       directly between AppFabric and the target Task Worker pod without JVM heap copies.</li>
 * </ul>
 */
public class TaskManagerService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(TaskManagerService.class);

  private final int port;
  private final String address;
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private ChannelFuture channelFuture;

  // In-memory routing registry mapping pod IP:Port -> PodState (leased namespace and occupancy count)
  private final Map<String, PodState> podRegistry = new ConcurrentHashMap<>();

  private final DiscoveryServiceClient discoveryServiceClient;
  private final DiscoveryService discoveryService;
  private Cancellable cancellable;

  @Inject
  TaskManagerService(CConfiguration cConf, DiscoveryServiceClient discoveryServiceClient,
                     DiscoveryService discoveryService) {
    this.port = cConf.getInt("task.manager.bind.port", 11025);
    this.address = cConf.get("task.manager.bind.address", "0.0.0.0");
    this.discoveryServiceClient = discoveryServiceClient;
    this.discoveryService = discoveryService;

    LOG.info("shruzard - Initializing TaskManagerService (Netty Proxy POC) on {}:{}", address, port);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("shruzard - Starting TaskManagerService Proxy HTTP server...");

    // Enable live Kubernetes Endpoints streaming if supported by the discovery client,
    // guaranteeing we get direct Pod IPs (V1Endpoints) instead of load-balanced ClusterIPs (V1Service).
    try {
      java.lang.reflect.Method method = discoveryServiceClient.getClass().getMethod("enableEndpointsWatcher");
      method.invoke(discoveryServiceClient);
      LOG.info("shruzard - Successfully enabled Kubernetes Endpoints watcher on discovery service {}",
               discoveryServiceClient.getClass().getSimpleName());
    } catch (NoSuchMethodException ignored) {
      // Normal for discovery services that do not support endpoints watching (e.g. In-memory, ZK)
    } catch (Exception e) {
      LOG.warn("shruzard - Failed to invoke enableEndpointsWatcher on discovery service", e);
    }

    // Pre-warm the Twill discovery cache asynchronously to ensure K8s Watchers 
    // are fully hydrated before the first HTTP request hits the proxy.
    discoveryServiceClient.discover(Constants.Service.TASK_WORKER);

    bossGroup = new NioEventLoopGroup(1,
        new com.google.common.util.concurrent.ThreadFactoryBuilder()
            .setNameFormat("taskmanager-boss-thread-%d").build());
    workerGroup = new NioEventLoopGroup(0,
        new com.google.common.util.concurrent.ThreadFactoryBuilder()
            .setNameFormat("taskmanager-worker-thread-%d").build());

    ServerBootstrap b = new ServerBootstrap();
    b.group(bossGroup, workerGroup)
     .channel(NioServerSocketChannel.class)
     .childHandler(new ChannelInitializer<SocketChannel>() {
         @Override
         protected void initChannel(SocketChannel ch) {
             ChannelPipeline p = ch.pipeline();
             p.addLast(new HttpServerCodec());
             // NOTICE: NO HttpObjectAggregator here!
             p.addLast(new ProxyFrontendHandler(podRegistry, discoveryServiceClient));
         }
     });

    channelFuture = b.bind(address, port).sync();
    
    // Announce via DiscoveryService so Kubernetes Discovery can provision K8s Service/Endpoints dynamically
    InetSocketAddress socketAddress = new InetSocketAddress(address, port);
    this.cancellable = discoveryService.register(
        ResolvingDiscoverable.of(URIScheme.HTTP.createDiscoverable(Constants.Service.TASK_MANAGER, socketAddress))
    );
    
    LOG.info("shruzard - TaskManagerService Proxy HTTP server started successfully at {}:{}", address, port);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("shruzard - Stopping TaskManagerService Proxy HTTP server...");
    if (this.cancellable != null) {
      this.cancellable.cancel();
    }
    if (channelFuture != null) {
      channelFuture.channel().close().sync();
    }
    if (bossGroup != null) {
      bossGroup.shutdownGracefully();
    }
    if (workerGroup != null) {
      workerGroup.shutdownGracefully();
    }
    LOG.info("shruzard - TaskManagerService Proxy HTTP server stopped.");
  }
}
