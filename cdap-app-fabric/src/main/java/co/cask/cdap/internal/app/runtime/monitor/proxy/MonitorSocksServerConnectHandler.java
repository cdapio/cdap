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

import co.cask.cdap.common.http.Channels;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.runtime.spi.ssh.PortForwarding;
import com.google.common.io.Closeables;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * The Netty handler for handling socks connect request and relaying data.
 * Connections are established based on known set of allowed SSH local port forwarding.
 */
final class MonitorSocksServerConnectHandler extends AbstractSocksServerConnectHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MonitorSocksServerHandler.class);
  private static final Logger OUTAGE_LOG = Loggers.sampling(
    LOG, LogSamplers.perMessage(() -> LogSamplers.limitRate(TimeUnit.MINUTES.toMillis(1))));

  private final PortForwardingProvider portForwardingProvider;

  MonitorSocksServerConnectHandler(PortForwardingProvider portForwardingProvider) {
    this.portForwardingProvider = portForwardingProvider;
  }

  @Override
  protected Future<RelayChannelHandler> createForwardingChannelHandler(Channel inboundChannel,
                                                                       String destAddress, int destPort) {
    // Creates a SSH tunnel to the destination host and relays all traffic to the "localhost" of the
    // destination host.
    Promise<RelayChannelHandler> promise = new DefaultPromise<>(inboundChannel.eventLoop());
    InetSocketAddress destSocketAddress = new InetSocketAddress(destAddress, destPort);
    try {
      PortForwarding portForwarding = portForwardingProvider
        .createPortForwarding(destSocketAddress, new PortForwarding.DataConsumer() {
          @Override
          public void received(ByteBuffer buffer) {
            // Copy and write the buffer to the channel.
            // It needs to be copied because writing to channel is asynchronous
            inboundChannel.write(Unpooled.wrappedBuffer(buffer).copy());
          }

          @Override
          public void flushed() {
            inboundChannel.flush();
          }

          @Override
          public void finished() {
            Channels.closeOnFlush(inboundChannel);
          }
        });

      // Close the port forwarding channel when the connect get closed
      inboundChannel.closeFuture().addListener(future -> Closeables.closeQuietly(portForwarding));

      // Creates a handler that forward everything to the port forwarding channel
      promise.setSuccess(new PortForwardingRelayChannelHandler(portForwarding, destSocketAddress));

    } catch (IOException e) {
      promise.setFailure(e);
    }

    return promise;
  }

  /**
   * A {@link RelayChannelHandler} for relaying traffic through SSH local port forwarding.
   */
  private static final class PortForwardingRelayChannelHandler
    extends ChannelInboundHandlerAdapter implements RelayChannelHandler {

    private final PortForwarding portForwarding;
    private final InetSocketAddress relayAddress;

    PortForwardingRelayChannelHandler(PortForwarding portForwarding, InetSocketAddress relayAddress) {
      this.portForwarding = portForwarding;
      this.relayAddress = relayAddress;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      ByteBuf buf = (ByteBuf) msg;
      try {
        portForwarding.write(buf.nioBuffer());
      } finally {
        buf.release();
      }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
      portForwarding.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      // If there is exception, just close the channel
      OUTAGE_LOG.warn("Exception raised when relaying messages", cause);
      ctx.close();
    }

    @Override
    public SocketAddress getRelayAddress() {
      return relayAddress;
    }
  }
}
