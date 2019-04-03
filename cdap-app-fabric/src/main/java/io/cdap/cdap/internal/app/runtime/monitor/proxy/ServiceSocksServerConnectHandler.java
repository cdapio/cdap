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

import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * The Netty handler for handling socks connect request and relaying data to CDAP services.
 * The connection is established based on service discovery.
 */
final class ServiceSocksServerConnectHandler extends AbstractSocksServerConnectHandler {

  private final DiscoveryServiceClient discoveryServiceClient;

  ServiceSocksServerConnectHandler(DiscoveryServiceClient discoveryServiceClient) {
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @Override
  protected Future<RelayChannelHandler> createForwardingChannelHandler(Channel inboundChannel,
                                                                       String destAddress, int destPort) {
    Promise<RelayChannelHandler> promise = new DefaultPromise<>(inboundChannel.eventLoop());

    // Creates a bootstrap for connecting to the target service
    ChannelGroup channels = new DefaultChannelGroup(inboundChannel.eventLoop());
    Bootstrap bootstrap = new Bootstrap()
      .group(inboundChannel.eventLoop())
      .channel(NioSocketChannel.class)
      .option(ChannelOption.SO_KEEPALIVE, true)
      .handler(new ChannelInboundHandlerAdapter() {
        @Override
        public void channelActive(ChannelHandlerContext ctx) {
          channels.add(ctx.channel());
          // When the outbound connection is active, adds the relay channel handler for the current pipeline,
          // which is for relaying traffic coming back from outbound connection.
          // Also complete the relay channel handler future, which is for relaying traffic from inbound to outbound.
          ctx.pipeline().addLast(new SimpleRelayChannelHandler(inboundChannel));
          promise.setSuccess(new SimpleRelayChannelHandler(ctx.channel()));
        }
      });

    // Discover the target address
    Promise<Discoverable> discoverablePromise = new DefaultPromise<>(inboundChannel.eventLoop());
    Cancellable cancellable = discoveryServiceClient.discover(destAddress).watchChanges(serviceDiscovered -> {
      // If it is discovered, make a connection and complete the channel handler future
      Discoverable discoverable = new RandomEndpointStrategy(() -> serviceDiscovered).pick();
      if (discoverable != null) {
        discoverablePromise.setSuccess(discoverable);
      }
    }, inboundChannel.eventLoop());

    // When discovery completed successfully, connect to the destination
    discoverablePromise.addListener((GenericFutureListener<Future<Discoverable>>) discoverableFuture -> {
      // Cancel the watch as it is no longer needed
      cancellable.cancel();

      if (discoverableFuture.isSuccess()) {
        Discoverable discoverable = discoverableFuture.get();
        bootstrap.connect(discoverable.getSocketAddress()).addListener((ChannelFutureListener) channelFuture -> {
          // If failed to establish the outbound connection, fail the relay channel handler future
          if (!channelFuture.isSuccess()) {
            promise.setFailure(channelFuture.cause());
          }
        });
      } else {
        promise.setFailure(discoverableFuture.cause());
      }
    });

    // On inbound channel close, close all outbound channels.
    // Also cancel the watch since it is no longer needed.
    // This is to handle case where discovery never return an endpoint before client connection timeout
    inboundChannel.closeFuture().addListener((ChannelFutureListener) future -> {
      cancellable.cancel();
      channels.close();
    });

    return promise;
  }
}
