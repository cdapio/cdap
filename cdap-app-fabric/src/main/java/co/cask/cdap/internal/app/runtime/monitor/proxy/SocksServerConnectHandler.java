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
import co.cask.cdap.internal.app.runtime.monitor.SSHSessionProvider;
import co.cask.cdap.runtime.spi.ssh.PortForwarding;
import co.cask.cdap.runtime.spi.ssh.SSHSession;
import com.google.common.io.Closeables;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.socksx.SocksMessage;
import io.netty.handler.codec.socksx.v4.DefaultSocks4CommandResponse;
import io.netty.handler.codec.socksx.v4.Socks4CommandRequest;
import io.netty.handler.codec.socksx.v4.Socks4CommandStatus;
import io.netty.handler.codec.socksx.v4.Socks4CommandType;
import io.netty.handler.codec.socksx.v5.DefaultSocks5CommandResponse;
import io.netty.handler.codec.socksx.v5.Socks5CommandRequest;
import io.netty.handler.codec.socksx.v5.Socks5CommandStatus;
import io.netty.handler.codec.socksx.v5.Socks5CommandType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * The Netty handler for handling socks connect request and relaying data.
 */
final class SocksServerConnectHandler extends SimpleChannelInboundHandler<SocksMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(SocksServerHandler.class);
  private static final Logger OUTAGE_LOG = Loggers.sampling(
    LOG, LogSamplers.perMessage(() -> LogSamplers.limitRate(TimeUnit.MINUTES.toMillis(1))));

  private final SSHSessionProvider sshSessionProvider;

  SocksServerConnectHandler(SSHSessionProvider sshSessionProvider) {
    this.sshSessionProvider = sshSessionProvider;
  }

  @Override
  public void channelRead0(final ChannelHandlerContext ctx, final SocksMessage message) throws SocksException {
    // Socks4
    if (message instanceof Socks4CommandRequest) {
      Socks4CommandRequest request = (Socks4CommandRequest) message;

      // We only support CONNECT command
      if (request.type().equals(Socks4CommandType.CONNECT)) {
        try {
          handleConnectRequest(ctx, request.dstAddr(), request.dstPort(),
                               new DefaultSocks4CommandResponse(Socks4CommandStatus.SUCCESS,
                                                                request.dstAddr(), request.dstPort()));
        } catch (Exception e) {
          throw new SocksException(new DefaultSocks4CommandResponse(Socks4CommandStatus.REJECTED_OR_FAILED), e);
        }
      } else {
        sendAndClose(ctx, new DefaultSocks4CommandResponse(Socks4CommandStatus.REJECTED_OR_FAILED));
      }
      return;
    }

    // Socks5
    if (message instanceof Socks5CommandRequest) {
      Socks5CommandRequest request = (Socks5CommandRequest) message;

      // We only support CONNECT command
      if (request.type().equals(Socks5CommandType.CONNECT)) {
        try {
          handleConnectRequest(ctx, request.dstAddr(), request.dstPort(),
                               new DefaultSocks5CommandResponse(Socks5CommandStatus.SUCCESS, request.dstAddrType(),
                                                                request.dstAddr(), request.dstPort()));
        } catch (Exception e) {
          throw new SocksException(new DefaultSocks5CommandResponse(Socks5CommandStatus.FAILURE,
                                                                    request.dstAddrType()), e);
        }
      } else {
        sendAndClose(ctx, new DefaultSocks5CommandResponse(Socks5CommandStatus.FAILURE, request.dstAddrType()));
      }
      return;
    }

    // Unsupported command, just close the channel.
    Channels.closeOnFlush(ctx.channel());
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    OUTAGE_LOG.warn("Failed to tunnel traffic for channel {}", ctx.channel(), cause);

    if (cause instanceof SocksException) {
      // This means we cannot create port forwarding channel, response with SOCKS failure message.
      sendAndClose(ctx, ((SocksException) cause).getResponse());
    } else {
      // For other exceptions, just close the channel
      ctx.close();
    }
  }

  /**
   * Handles the CONNECT socks request.
   *
   * @param ctx the context object for the channel
   * @param destAddress the destination address
   * @param destPort the destination port that the remote {@code localhost} is listening to
   * @param successResponse a {@link SocksMessage} to send back to client once the port forwarding channel has been
   *                        established
   * @throws IOException if failed to open the port forwarding channel
   */
  private void handleConnectRequest(ChannelHandlerContext ctx, String destAddress, int destPort,
                                    SocksMessage successResponse) throws IOException {
    // Before sending back the success response,
    // creates the forwarding handler first, which will open the port forwarding channel.
    ChannelHandler forwardingHandler = createForwardingChannelHandler(ctx.channel(), destAddress, destPort);
    ctx.channel().write(successResponse).addListener((ChannelFutureListener) future -> {
      if (future.isSuccess()) {
        ctx.pipeline().remove(SocksServerConnectHandler.this);
        ctx.pipeline().addLast(forwardingHandler);
      } else {
        Channels.closeOnFlush(ctx.channel());
      }
    });
  }

  /**
   * Sends a message and close the channel once the sending is completed.
   *
   * @param ctx the {@link ChannelHandlerContext} for sending the message
   * @param message the message to send.
   */
  private void sendAndClose(ChannelHandlerContext ctx, Object message) {
    ctx.writeAndFlush(message).addListener(ChannelFutureListener.CLOSE);
  }

  /**
   * Creates a {@link ChannelHandler} for relaying future traffic between the client and the destination server.
   * It creates a SSH tunnel to the destination host and relays all traffic to the {@code localhost} of the
   * destination host.
   *
   * @param channel the {@link Channel} for reading and writing data
   * @param destAddress the destination address
   * @param destPort the destination port that the remote {@code localhost} is listening to
   * @return a {@link ChannelHandler} ready to be added to the {@link ChannelPipeline} for relaying
   * @throws IOException if failed to open the port forwarding channel
   */
  private ChannelHandler createForwardingChannelHandler(Channel channel,
                                                        String destAddress, int destPort) throws IOException {
    SSHSession sshSession = sshSessionProvider.getSession(destAddress, destPort);
    PortForwarding portForwarding = sshSession.createLocalPortForward("localhost", destPort,
                                                                      destPort, new PortForwarding.DataConsumer() {
      @Override
      public void received(ByteBuffer buffer) {
        channel.write(Unpooled.wrappedBuffer(buffer));
      }

      @Override
      public void flushed() {
        channel.flush();
      }

      @Override
      public void finished() {
        Channels.closeOnFlush(channel);
      }
    });

    // Close the port forwarding channel when the connect get closed
    channel.closeFuture().addListener(future -> Closeables.closeQuietly(portForwarding));

    // Creates a handler that forward everything to the port forwarding channel
    return new ChannelInboundHandlerAdapter() {

      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf buf = (ByteBuf) msg;
        portForwarding.write(buf.nioBuffer());
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
    };
  }

  /**
   * An exception that wraps a cause and a {@link SocksMessage} for responding back to client.
   */
  private static final class SocksException extends Exception {

    private final SocksMessage response;

    private SocksException(SocksMessage response, Throwable cause) {
      super(cause);
      this.response = response;
    }

    SocksMessage getResponse() {
      return response;
    }
  }
}
