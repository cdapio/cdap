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

  private final Map<Integer, String> workerPartitions = new ConcurrentHashMap<>();

  @Inject
  TaskManagerService(CConfiguration cConf) {
    this.port = cConf.getInt("task.manager.port", 11025);
    this.address = cConf.get("task.manager.address", "0.0.0.0");

    // Mocking worker partitions for POC
    workerPartitions.put(1, "127.0.0.1:8081");
    workerPartitions.put(2, "127.0.0.1:8082");

    LOG.info("sidhdirenge - Initializing TaskManagerService (Netty Proxy POC) on {}:{}", address, port);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("sidhdirenge - Starting TaskManagerService Proxy HTTP server...");

    bossGroup = new NioEventLoopGroup(1);
    workerGroup = new NioEventLoopGroup();

    ServerBootstrap b = new ServerBootstrap();
    b.group(bossGroup, workerGroup)
     .channel(NioServerSocketChannel.class)
     .childHandler(new ChannelInitializer<SocketChannel>() {
         @Override
         protected void initChannel(SocketChannel ch) {
             ChannelPipeline p = ch.pipeline();
             p.addLast(new HttpServerCodec());
             // NOTICE: NO HttpObjectAggregator here!
             p.addLast(new ProxyFrontendHandler(workerPartitions));
         }
     });

    channelFuture = b.bind(address, port).sync();
    LOG.info("sidhdirenge - TaskManagerService Proxy HTTP server started successfully at {}:{}", address, port);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("sidhdirenge - Stopping TaskManagerService Proxy HTTP server...");
    if (channelFuture != null) {
      channelFuture.channel().close().sync();
    }
    if (bossGroup != null) {
      bossGroup.shutdownGracefully();
    }
    if (workerGroup != null) {
      workerGroup.shutdownGracefully();
    }
    LOG.info("sidhdirenge - TaskManagerService Proxy HTTP server stopped.");
  }
}
