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
  public void channelInterestChanged(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    // If outboundChannel is not saturated anymore, continue accepting
    // the incoming traffic from the inboundChannel.
    if (e.getChannel().isWritable()) {
      inboundChannel.setReadable(true);
    }

    // If outboundChannel is saturated, do not read inboundChannel
    if (!e.getChannel().isWritable()) {
      inboundChannel.setReadable(false);
    }
  }

  @Override
  public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    InboundHandler.closeOnFlush(inboundChannel);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    LOG.error("Got exception", e);
    InboundHandler.closeOnFlush(e.getChannel());
  }
}
