package com.continuuity.gateway.router.handlers;

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
  public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    LOG.trace("Channel closed {}", ctx.getChannel().getId());
    HttpRequestHandler.closeOnFlush(inboundChannel);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    LOG.error("Got exception {} {}", ctx.getChannel().getId(), e.getCause());
    HttpRequestHandler.closeOnFlush(e.getChannel());
  }
}
