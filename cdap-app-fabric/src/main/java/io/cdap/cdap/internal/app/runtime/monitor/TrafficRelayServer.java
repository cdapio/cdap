/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.util.concurrent.AbstractIdleService;
import io.cdap.cdap.internal.app.runtime.monitor.proxy.SimpleRelayChannelHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.apache.twill.common.Threads;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A netty based traffic relay server that passes network traffic between two endpoints.
 */
public final class TrafficRelayServer extends AbstractIdleService {

  private final InetAddress bindHost;
  private final Supplier<InetSocketAddress> targetAddressSupplier;
  private final ChannelGroup serverChannelGroup;
  private final ChannelGroup clientChannelGroup;

  private EventLoopGroup eventLoopGroup;
  private InetSocketAddress bindAddress;

  public TrafficRelayServer(InetAddress bindHost, Supplier<InetSocketAddress> targetAddressSupplier) {
    this.bindHost = bindHost;
    this.targetAddressSupplier = targetAddressSupplier;
    this.serverChannelGroup = new DefaultChannelGroup(ImmediateEventExecutor.INSTANCE);
    this.clientChannelGroup = new DefaultChannelGroup(ImmediateEventExecutor.INSTANCE);
  }

  public InetSocketAddress getBindAddress() {
    InetSocketAddress addr = this.bindAddress;
    if (addr == null) {
      throw new IllegalStateException("Traffic relay server hasn't been started");
    }
    return addr;
  }

  @Override
  protected void startUp() throws Exception {
    // We only do IO relying, hence doesn't need large amount of threads.
    eventLoopGroup = new NioEventLoopGroup(10, Threads.createDaemonThreadFactory("traffic-relay-%d"));

    Bootstrap clientBootstrap = new Bootstrap()
      .group(eventLoopGroup)
      .channel(NioSocketChannel.class)
      .option(ChannelOption.SO_KEEPALIVE, true)
      .handler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) {
          clientChannelGroup.add(ch);
        }
      });

    ServerBootstrap serverBootstrap = new ServerBootstrap()
      .group(eventLoopGroup)
      .channel(NioServerSocketChannel.class)
      .childHandler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) {
          serverChannelGroup.add(ch);
          ch.pipeline().addLast("connector", new ChannelInboundHandlerAdapter() {
            @Override
            public void channelActive(ChannelHandlerContext ctx) {
              InetSocketAddress targetAddr = targetAddressSupplier.get();

              // If target address is not available, just close the connection.
              Channel serverChannel = ctx.channel();
              if (targetAddr == null) {
                serverChannel.close();
                return;
              }

              // Disable reading until the relay connection is opened
              serverChannel.config().setAutoRead(false);
              clientBootstrap.connect(targetAddr).addListener((ChannelFutureListener) future -> {
                Channel clientChannel = future.channel();

                serverChannel.closeFuture()
                  .addListener((ChannelFutureListener) f -> clientChannel.close());
                clientChannel.closeFuture()
                  .addListener((ChannelFutureListener) f -> serverChannel.close());

                if (future.isSuccess()) {
                  serverChannel.pipeline().remove("connector");
                  serverChannel.pipeline().addLast(new SimpleRelayChannelHandler(clientChannel));
                  clientChannel.pipeline().addLast(new SimpleRelayChannelHandler(serverChannel));
                  serverChannel.config().setAutoRead(true);
                } else {
                  serverChannel.close();
                }
              });
            }
          });
        }
      });

    Channel serverChannel = serverBootstrap.bind(bindHost, 0).sync().channel();
    bindAddress = (InetSocketAddress) serverChannel.localAddress();

    serverChannelGroup.add(serverChannel);
  }

  @Override
  protected void shutDown() {
    serverChannelGroup.close().awaitUninterruptibly();
    clientChannelGroup.close().awaitUninterruptibly();
    bindAddress = null;
    eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
  }
}
