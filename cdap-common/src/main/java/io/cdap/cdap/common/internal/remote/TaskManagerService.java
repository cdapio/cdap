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
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import io.cdap.cdap.common.conf.Constants;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Guice-managed service that runs the Centralized Task Manager HTTP Server (Netty Proxy POC).
 */
public class TaskManagerService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(TaskManagerService.class);

  private final int port;
  private final String address;
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private ChannelFuture channelFuture;

  private final Map<String, PodState> podRegistry = new ConcurrentHashMap<>();

  private final DiscoveryServiceClient discoveryServiceClient;

  @Inject
  TaskManagerService(CConfiguration cConf, DiscoveryServiceClient discoveryServiceClient) {
    this.port = cConf.getInt("task.manager.port", 11025);
    this.address = cConf.get("task.manager.address", "0.0.0.0");
    this.discoveryServiceClient = discoveryServiceClient;

    LOG.info("shruzard - Initializing TaskManagerService (Netty Proxy POC) on {}:{}", address, port);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("shruzard - Starting TaskManagerService Proxy HTTP server...");

    bossGroup = new NioEventLoopGroup(1,
        new com.google.common.util.concurrent.ThreadFactoryBuilder().setNameFormat("taskmanager-boss-thread-%d").build());
    workerGroup = new NioEventLoopGroup(0,
        new com.google.common.util.concurrent.ThreadFactoryBuilder().setNameFormat("taskmanager-worker-thread-%d").build());

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
    LOG.info("shruzard - TaskManagerService Proxy HTTP server started successfully at {}:{}", address, port);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("shruzard - Stopping TaskManagerService Proxy HTTP server...");
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
