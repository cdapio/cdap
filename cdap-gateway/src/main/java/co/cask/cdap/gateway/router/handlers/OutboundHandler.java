/*
 * Copyright © 2014 Cask Data, Inc.
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

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles requests to and from a discoverable endpoint.
 */
public class OutboundHandler extends SimpleChannelUpstreamHandler {
  private static final Logger LOG = LoggerFactory.getLogger(OutboundHandler.class);

  private final Channel inboundChannel;

  public OutboundHandler(Channel inboundChannel) {
    this.inboundChannel = inboundChannel;
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, final MessageEvent e) throws Exception {
    ChannelBuffer msg = (ChannelBuffer) e.getMessage();
    inboundChannel.write(msg);
  }

  @Override
  public void channelInterestChanged(ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception {
    inboundChannel.getPipeline().execute(new Runnable() {
      @Override
      public void run() {
        // If outboundChannel is not saturated anymore, continue accepting
        // the incoming traffic from the inboundChannel.
        if (e.getChannel().isWritable()) {
          LOG.trace("Setting inboundChannel readable.");
          inboundChannel.setReadable(true);
        } else {
          // If outboundChannel is saturated, do not read inboundChannel
          LOG.trace("Setting inboundChannel non-readable.");
          inboundChannel.setReadable(false);
        }
      }
    });
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    LOG.error("Got exception {}", ctx.getChannel(), e.getCause());
    HttpRequestHandler.closeOnFlush(e.getChannel());
  }
}
