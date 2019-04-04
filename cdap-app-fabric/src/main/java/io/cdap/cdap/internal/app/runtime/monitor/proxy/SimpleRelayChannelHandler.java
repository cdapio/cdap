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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * A {@link RelayChannelHandler} that relay traffic from one {@link Channel} to another.
 */
public final class SimpleRelayChannelHandler extends ChannelInboundHandlerAdapter implements RelayChannelHandler {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleRelayChannelHandler.class);
  private static final Logger OUTAGE_LOG = Loggers.sampling(
    LOG, LogSamplers.perMessage(() -> LogSamplers.limitRate(TimeUnit.MINUTES.toMillis(1))));

  private final Channel outboundChannel;

  public SimpleRelayChannelHandler(Channel outboundChannel) {
    this.outboundChannel = outboundChannel;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (outboundChannel.isActive()) {
      outboundChannel.write(msg);
    } else {
      ReferenceCountUtil.release(msg);
    }
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    outboundChannel.flush();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    if (outboundChannel.isActive()) {
      Channels.closeOnFlush(outboundChannel);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    // If there is exception, just close the channel
    OUTAGE_LOG.warn("Exception raised when relaying messages", cause);
    ctx.close();
  }

  @Override
  public SocketAddress getRelayAddress() {
    return outboundChannel.remoteAddress();
  }
}
