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

package io.cdap.cdap.internal.app.runtime.monitor.proxy;

import com.google.common.util.concurrent.AbstractIdleService;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.socksx.SocksPortUnificationServerHandler;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * A SOCKS proxy service for proxying calls from remote runtime to CDAP services.
 */
public class ServiceSocksProxy extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceSocksProxy.class);

  private final DiscoveryServiceClient discoveryServiceClient;
  private final ServiceSocksProxyAuthenticator authenticator;
  private volatile InetSocketAddress bindAddress;
  private ChannelGroup channelGroup;
  private EventLoopGroup eventLoopGroup;

  public ServiceSocksProxy(DiscoveryServiceClient discoveryServiceClient,
                           ServiceSocksProxyAuthenticator authenticator) {
    this.discoveryServiceClient = discoveryServiceClient;
    this.authenticator = authenticator;
  }

  /**
   * Returns the address that this SOCKS proxy bind to.
   */
  public InetSocketAddress getBindAddress() {
    InetSocketAddress addr = this.bindAddress;
    if (addr == null) {
      throw new IllegalStateException("Proxy server hasn't been started");
    }
    return addr;
  }

  @Override
  protected void startUp() throws Exception {
    ServerBootstrap bootstrap = new ServerBootstrap();

    // We don't perform any blocking task in the proxy, only IO relying, hence doesn't need large amount of threads.
    eventLoopGroup = new NioEventLoopGroup(10, Threads.createDaemonThreadFactory("service-socks-proxy-%d"));
    bootstrap
      .group(eventLoopGroup)
      .channel(NioServerSocketChannel.class)
      .childHandler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) {
          channelGroup.add(ch);

          ch.pipeline()
            .addLast(new SocksPortUnificationServerHandler())
            .addLast(new ServiceSocksServerHandler(discoveryServiceClient, authenticator));
        }
      });

    Channel serverChannel = bootstrap.bind(InetAddress.getLoopbackAddress(), 0).sync().channel();
    bindAddress = (InetSocketAddress) serverChannel.localAddress();

    channelGroup = new DefaultChannelGroup(ImmediateEventExecutor.INSTANCE);
    channelGroup.add(serverChannel);

    LOG.info("Runtime service socks proxy started on {}", bindAddress);
  }

  @Override
  protected void shutDown() throws Exception {
    channelGroup.close().awaitUninterruptibly();
    bindAddress = null;
    eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
  }
}
