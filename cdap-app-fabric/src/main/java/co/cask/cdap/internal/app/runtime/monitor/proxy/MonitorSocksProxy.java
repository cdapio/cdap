/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.monitor.proxy;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * The SOCKS proxy server for forwarding runtime monitor https calls. It uses SSH port forwarding (tunneling)
 * to relay data between client and server. Unlike regular SSH dynamic tunneling, this proxy server
 * always forward network traffic to the {@code localhost} of the destination SSH host as specified from the request.
 */
public class MonitorSocksProxy extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(MonitorSocksProxy.class);

  private final CConfiguration cConf;
  private final PortForwardingProvider portForwardingProvider;
  private volatile InetSocketAddress bindAddress;
  private ChannelGroup channelGroup;
  private EventLoopGroup eventLoopGroup;

  public MonitorSocksProxy(CConfiguration cConf, PortForwardingProvider portForwardingProvider) {
    this.cConf = cConf;
    this.portForwardingProvider = portForwardingProvider;
  }

  public InetSocketAddress getBindAddress() {
    InetSocketAddress addr = this.bindAddress;
    if (addr == null) {
      throw new IllegalStateException("Monitor proxy server hasn't been started");
    }
    return addr;
  }

  @Override
  protected void startUp() throws Exception {
    ServerBootstrap bootstrap = new ServerBootstrap();

    // Use a thread pool of size runtime monitor threads + 1. There can be at most that many threads making
    // call to this socks proxy, plus 1 for the boss thread.
    eventLoopGroup = new NioEventLoopGroup(cConf.getInt(Constants.RuntimeMonitor.THREADS) + 1,
                                           Threads.createDaemonThreadFactory("monitor-socks-proxy-%d"));
    bootstrap
      .group(eventLoopGroup)
      .channel(NioServerSocketChannel.class)
      .childHandler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) {
          channelGroup.add(ch);

          ch.pipeline()
            .addLast(new SocksPortUnificationServerHandler())
            .addLast(new MonitorSocksServerHandler(portForwardingProvider));
        }
      });

    Channel serverChannel = bootstrap.bind(InetAddress.getLoopbackAddress(), 0).sync().channel();
    bindAddress = (InetSocketAddress) serverChannel.localAddress();

    channelGroup = new DefaultChannelGroup(ImmediateEventExecutor.INSTANCE);
    channelGroup.add(serverChannel);

    LOG.info("Runtime monitor socks proxy started on {}", bindAddress);
  }

  @Override
  protected void shutDown() throws Exception {
    channelGroup.close().awaitUninterruptibly();
    bindAddress = null;
    eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
  }
}
