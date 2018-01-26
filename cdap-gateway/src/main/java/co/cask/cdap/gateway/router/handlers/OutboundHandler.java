/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package co.cask.cdap.gateway.router.handlers;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ChannelDuplexHandler} for forwarding requests/responses between the router and the internal service.
 * It also handle idle state event for closing idled internal connections.
 */
public class OutboundHandler extends ChannelDuplexHandler {
  private static final Logger LOG = LoggerFactory.getLogger(OutboundHandler.class);

  private final Channel inboundChannel;
  private boolean requestInProgress;
  private boolean keepAlive;

  public OutboundHandler(Channel inboundChannel) {
    this.inboundChannel = inboundChannel;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    // One receiving messages from the internal service, forward it to the inbound channel
    inboundChannel.write(msg);

    if (msg instanceof HttpResponse) {
      keepAlive = HttpUtil.isKeepAlive((HttpResponse) msg);
    }

    // A response is completed by receiving the last http content
    if (msg instanceof LastHttpContent) {
      requestInProgress = false;
    }
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    inboundChannel.flush();
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    // A request starts with a HttpRequest
    if (msg instanceof HttpRequest) {
      requestInProgress = true;
      keepAlive = HttpUtil.isKeepAlive((HttpRequest) msg);
    }
    ctx.write(msg, promise);
  }

  @Override
  public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
    if (requestInProgress) {
      final Channel channel = ctx.channel();
      ctx.executor().execute(() -> {
        // If outboundChannel is not saturated anymore, continue accepting
        // the incoming traffic from the inboundChannel.
        if (channel.isWritable()) {
          LOG.trace("Setting inboundChannel readable.");
          inboundChannel.config().setAutoRead(true);
        } else {
          // If outboundChannel is saturated, do not read inboundChannel
          LOG.trace("Setting inboundChannel non-readable.");
          inboundChannel.config().setAutoRead(false);
        }
      });
    }
    ctx.fireChannelWritabilityChanged();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    // Close the inbound channel if there is request in progress, or the last request/response has keep-alive == false
    if (requestInProgress || !keepAlive) {
      Channels.closeOnFlush(inboundChannel);
    }
    ctx.fireChannelInactive();
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (!(evt instanceof IdleStateEvent)) {
      ctx.fireUserEventTriggered(evt);
      return;
    }

    if (IdleState.ALL_IDLE == ((IdleStateEvent) evt).state()) {
      if (requestInProgress) {
        LOG.trace("Request is in progress, so not closing channel.");
      } else {
        // No data has been sent or received for a while. Close channel.
        Channel channel = ctx.channel();
        channel.close();
        LOG.trace("No data has been sent or received for channel '{}' for more than the configured idle timeout. " +
                    "Closing the channel. Local Address: {}, Remote Address: {}",
                  channel, channel.localAddress(), channel.remoteAddress());
      }
    }
  }
}
