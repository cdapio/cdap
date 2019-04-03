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

import io.cdap.cdap.common.http.Channels;
import io.cdap.cdap.common.logging.LogSamplers;
import io.cdap.cdap.common.logging.Loggers;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.socksx.SocksMessage;
import io.netty.handler.codec.socksx.v4.DefaultSocks4CommandResponse;
import io.netty.handler.codec.socksx.v4.Socks4CommandRequest;
import io.netty.handler.codec.socksx.v4.Socks4CommandStatus;
import io.netty.handler.codec.socksx.v4.Socks4CommandType;
import io.netty.handler.codec.socksx.v5.DefaultSocks5CommandResponse;
import io.netty.handler.codec.socksx.v5.Socks5AddressType;
import io.netty.handler.codec.socksx.v5.Socks5CommandRequest;
import io.netty.handler.codec.socksx.v5.Socks5CommandStatus;
import io.netty.handler.codec.socksx.v5.Socks5CommandType;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Abstract base class for handling SOCKS proxy connect requests.
 */
abstract class AbstractSocksServerConnectHandler extends SimpleChannelInboundHandler<SocksMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSocksServerConnectHandler.class);
  private static final Logger OUTAGE_LOG = Loggers.sampling(
    LOG, LogSamplers.perMessage(() -> LogSamplers.limitRate(TimeUnit.MINUTES.toMillis(1))));

  /**
   * Creates a {@link Future} that returns a {@link RelayChannelHandler} for relaying future traffic between
   * the client and the destination server when it is completed.
   *
   * @param inboundChannel the {@link Channel} for the incoming proxy request
   * @param destAddress the destination address
   * @param destPort the destination port that the remote {@code localhost} is listening to
   * @return a {@link Future} of {@link ChannelHandler} that will be added to the {@link ChannelPipeline} for relaying
   *         when the Future is completed
   */
  protected abstract Future<RelayChannelHandler> createForwardingChannelHandler(Channel inboundChannel,
                                                                                String destAddress, int destPort);

  @Override
  public void channelRead0(final ChannelHandlerContext ctx, final SocksMessage message) throws SocksException {
    // Socks4
    if (message instanceof Socks4CommandRequest) {
      Socks4CommandRequest request = (Socks4CommandRequest) message;

      // We only support CONNECT command
      if (request.type().equals(Socks4CommandType.CONNECT)) {
        try {
          handleConnectRequest(ctx, request.dstAddr(), request.dstPort(),
                               addr -> new DefaultSocks4CommandResponse(Socks4CommandStatus.SUCCESS,
                                                                        addr.getAddress().getHostAddress(),
                                                                        addr.getPort()),
                               () -> new DefaultSocks4CommandResponse(Socks4CommandStatus.REJECTED_OR_FAILED));
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
                               addr -> new DefaultSocks5CommandResponse(Socks5CommandStatus.SUCCESS,
                                                                        addr.getAddress() instanceof Inet4Address
                                                                          ? Socks5AddressType.IPv4
                                                                          : Socks5AddressType.IPv6,
                                                                        addr.getAddress().getHostAddress(),
                                                                        addr.getPort()),
                               () -> new DefaultSocks5CommandResponse(Socks5CommandStatus.FAILURE,
                                                                      request.dstAddrType()));
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
   * @param responseFunc a {@link Function} for creating a {@link SocksMessage} to send back to client
   *                     once the relay channel has been established
   */
  private void handleConnectRequest(ChannelHandlerContext ctx, String destAddress,
                                    int destPort, Function<InetSocketAddress, SocksMessage> responseFunc,
                                    Supplier<SocksMessage> failureResponseSupplier) {
    // Create a forwarding channel handler, which returns a Future.
    // When that handler future completed successfully, responds with a Socks success response.
    // After the success response completed successfully, add the relay handler to the pipeline.
    Channel inboundChannel = ctx.channel();

    createForwardingChannelHandler(inboundChannel, destAddress, destPort)
      .addListener((GenericFutureListener<Future<RelayChannelHandler>>) handlerFuture -> {
        if (handlerFuture.isSuccess()) {
          RelayChannelHandler handler = handlerFuture.get();
          SocketAddress relayAddress = handler.getRelayAddress();
          if (!(relayAddress instanceof InetSocketAddress)) {
            // This shouldn't happen, as the address must be a InetSocketAddress type
            // If it does, log and response with error for the Socks connection
            LOG.warn("Relay address is not InetSocketAddress: {} {}", relayAddress.getClass(), relayAddress);
            inboundChannel.writeAndFlush(failureResponseSupplier.get())
              .addListener(channelFuture -> Channels.closeOnFlush(inboundChannel));
          } else {
            inboundChannel.writeAndFlush(responseFunc.apply((InetSocketAddress) relayAddress))
              .addListener(channelFuture -> {
                if (channelFuture.isSuccess()) {
                  ctx.pipeline().remove(AbstractSocksServerConnectHandler.this);
                  ctx.pipeline().addLast(handler);
                } else {
                  Channels.closeOnFlush(inboundChannel);
                }
              });
          }
        } else {
          Channels.closeOnFlush(inboundChannel);
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
