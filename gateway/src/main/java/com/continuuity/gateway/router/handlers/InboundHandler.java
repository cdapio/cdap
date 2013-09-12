package com.continuuity.gateway.router.handlers;

import com.continuuity.common.discovery.EndpointStrategy;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * Proxies incoming requests to a discoverable endpoint.
 */
public class InboundHandler extends SimpleChannelUpstreamHandler {
  private static final Logger LOG = LoggerFactory.getLogger(InboundHandler.class);

  private final ClientBootstrap clientBootstrap;
  private final EndpointStrategy discoverableEndpoints;

  // This lock guards against the race condition that overrides the
  // OP_READ flag incorrectly.
  // See the related discussion: http://markmail.org/message/x7jc6mqx6ripynqf
  private final Object trafficLock = new Object();
  private volatile Channel outboundChannel;

  public InboundHandler(ClientBootstrap clientBootstrap, EndpointStrategy discoverableEndpoints) {
    this.clientBootstrap = clientBootstrap;
    this.discoverableEndpoints = discoverableEndpoints;
  }

  @Override
  public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    // Suspend incoming traffic until connected to the outbound service.
    final Channel inboundChannel = e.getChannel();
    inboundChannel.setReadable(false);

    // Connect to outbound service.
    InetSocketAddress address = discoverableEndpoints.pick().getSocketAddress();
    LOG.trace("Opening connection from {} to {}", inboundChannel.getLocalAddress(), address);
    ChannelFuture outFuture = clientBootstrap.connect(address);

    outboundChannel = outFuture.getChannel();
    outFuture.addListener(new ChannelFutureListener() {
      public void operationComplete(ChannelFuture future) throws Exception {
        if (future.isSuccess()) {
          outboundChannel.getPipeline().addLast("outbound-handler", new OutboundHandler(inboundChannel, trafficLock));

          // Connection attempt succeeded:
          // Begin to accept incoming traffic.
          inboundChannel.setReadable(true);
        } else {
          // Close the connection if the connection attempt has failed.
          inboundChannel.close();
        }
      }
    });
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, final MessageEvent e) throws Exception {
    ChannelBuffer msg = (ChannelBuffer) e.getMessage();

    synchronized (trafficLock) {
      outboundChannel.write(msg);
      // If outboundChannel is saturated, do not read until notified in
      // OutboundHandler.channelInterestChanged().
      if (!outboundChannel.isWritable()) {
        e.getChannel().setReadable(false);
      }
    }
  }

  @Override
  public void channelInterestChanged(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    // If inboundChannel is not saturated anymore, continue accepting
    // the incoming traffic from the outboundChannel.
    synchronized (trafficLock) {
      if (e.getChannel().isWritable()) {
        if (outboundChannel != null) {
          outboundChannel.setReadable(true);
        }
      }
    }
  }

  @Override
  public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    if (outboundChannel != null) {
      closeOnFlush(outboundChannel);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    LOG.error("Got exception", e);
    closeOnFlush(e.getChannel());
  }

  /**
   * Closes the specified channel after all queued write requests are flushed.
   */
  static void closeOnFlush(Channel ch) {
    if (ch.isConnected()) {
      ch.write(ChannelBuffers.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }
  }
}
