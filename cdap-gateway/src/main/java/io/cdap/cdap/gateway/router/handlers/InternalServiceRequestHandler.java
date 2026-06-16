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

package io.cdap.cdap.gateway.router.handlers;

import io.cdap.cdap.common.http.Channels;
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
 * A {@link ChannelDuplexHandler} for forwarding requests/responses between the router and the
 * internal service. It also handle idle state event for closing idled internal connections.
 */
public class InternalServiceRequestHandler extends ChannelDuplexHandler {

  private static final Logger LOG = LoggerFactory.getLogger(InternalServiceRequestHandler.class);

  private final Channel httpRequestChannel;
  private boolean requestInProgress;
  private boolean keepAlive;

  public InternalServiceRequestHandler(Channel httpRequestChannel) {
    this.httpRequestChannel = httpRequestChannel;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    String clientChId = httpRequestChannel.id().asShortText();
    if (msg instanceof HttpResponse) {
      HttpResponse response = (HttpResponse) msg;
      keepAlive = HttpUtil.isKeepAlive(response);
      LOG.info("Router [ClientCh:{}]: Received response headers from service: status={}", clientChId, response.status());
    }

    // One receiving messages from the internal service, forward it to the httpRequestChannel
    io.netty.channel.ChannelFuture writeFuture = httpRequestChannel.write(msg);

    // A response is completed by receiving the last http content
    if (msg instanceof LastHttpContent) {
      requestInProgress = false;
      LOG.info("Router [ClientCh:{}]: Received response completion (LastHttpContent) from service. Writing to client socket...", clientChId);
      writeFuture.addListener(new io.netty.channel.ChannelFutureListener() {
        @Override
        public void operationComplete(io.netty.channel.ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
            LOG.info("Router [ClientCh:{}]: Response bytes successfully written to client TCP socket.", clientChId);
          } else {
            LOG.error("Router [ClientCh:{}]: Failed to write response bytes to client TCP socket! Error: {}", clientChId, future.cause().getMessage());
          }
        }
      });
    }
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    LOG.info("Router [ClientCh:{}]: Flushing response packets back to client.", httpRequestChannel.id().asShortText());
    httpRequestChannel.flush();
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
    // A request starts with a HttpRequest
    if (msg instanceof HttpRequest) {
      HttpRequest request = (HttpRequest) msg;
      requestInProgress = true;
      keepAlive = HttpUtil.isKeepAlive(request);
      LOG.info("Router [ClientCh:{}]: Forwarding request to service endpoint: {} {}", httpRequestChannel.id().asShortText(), request.method(), request.uri());
    }
    ctx.write(msg, promise);
  }

  @Override
  public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
    if (requestInProgress) {
      final Channel internalServiceChannel = ctx.channel();
      ctx.executor().execute(() -> {
        // If internalServiceChannel is not saturated anymore, continue accepting
        // the incoming traffic from the httpRequestChannel.
        if (internalServiceChannel.isWritable()) {
          LOG.trace("Setting httpRequestChannel readable.");
          httpRequestChannel.config().setAutoRead(true);
        } else {
          // If internalServiceChannel is saturated, do not read httpRequestChannel
          LOG.trace("Setting httpRequestChannel non-readable.");
          httpRequestChannel.config().setAutoRead(false);
        }
      });
    }
    ctx.fireChannelWritabilityChanged();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    // Close the httpRequestChannel if there is request in progress, or the last request/response has
    // keep-alive == false
    if (requestInProgress || !keepAlive) {
      Channels.closeOnFlush(httpRequestChannel);
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
        LOG.trace(
            "No data has been sent or received for channel '{}' for more than the configured idle timeout. "

                + "Closing the channel. Local Address: {}, Remote Address: {}",
            channel, channel.localAddress(), channel.remoteAddress());
      }
    }
  }
}
