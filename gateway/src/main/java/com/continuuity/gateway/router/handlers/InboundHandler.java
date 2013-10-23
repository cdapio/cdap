package com.continuuity.gateway.router.handlers;

import com.continuuity.gateway.router.HeaderDecoder;
import com.continuuity.gateway.router.RouterServiceLookup;
import com.continuuity.weave.discovery.Discoverable;
import com.google.common.base.Supplier;
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
  private final RouterServiceLookup serviceLookup;

  private volatile Channel outboundChannel;

  public InboundHandler(ClientBootstrap clientBootstrap, final RouterServiceLookup serviceLookup) {
    this.clientBootstrap = clientBootstrap;
    this.serviceLookup = serviceLookup;
  }

  private void openOutboundAndWrite(MessageEvent e) throws Exception {
    final ChannelBuffer msg = (ChannelBuffer) e.getMessage();
    msg.markReaderIndex();

    // Suspend incoming traffic until connected to the outbound service.
    final Channel inboundChannel = e.getChannel();
    inboundChannel.setReadable(false);

    // Discover endpoint.
    int inboundPort = ((InetSocketAddress) inboundChannel.getLocalAddress()).getPort();
    Discoverable discoverable = serviceLookup.getDiscoverable(inboundPort, new Supplier<HeaderDecoder.HeaderInfo>() {
      @Override
      public HeaderDecoder.HeaderInfo get() {
        return HeaderDecoder.decodeHeader(msg);
      }
    });

    if (discoverable == null) {
      inboundChannel.close();
      return;
    }

    // Connect to outbound service.
    final InetSocketAddress address = discoverable.getSocketAddress();
    LOG.trace("Opening connection from {} to {} for {}",
              inboundChannel.getLocalAddress(), address, inboundChannel.getRemoteAddress());
    ChannelFuture outFuture = clientBootstrap.connect(address);

    outboundChannel = outFuture.getChannel();
    outFuture.addListener(new ChannelFutureListener() {
      public void operationComplete(ChannelFuture future) throws Exception {
        if (future.isSuccess()) {
          outboundChannel.getPipeline().addLast("outbound-handler", new OutboundHandler(inboundChannel));

          // Connection attempt succeeded.

          // Write the message to outBoundChannel.
          msg.resetReaderIndex();
          outboundChannel.write(msg);

          // Begin to accept incoming traffic.
          inboundChannel.setReadable(true);
          LOG.trace("Connection opened from {} to {} for {}",
                    inboundChannel.getLocalAddress(), address, inboundChannel.getRemoteAddress());
        } else {
          // Close the connection if the connection attempt has failed.
          inboundChannel.close();
          LOG.trace("Failed to open connection from {} to {} for {}",
                    inboundChannel.getLocalAddress(), address, inboundChannel.getRemoteAddress(), future.getCause());
        }
      }
    });
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    if (outboundChannel == null) {
      openOutboundAndWrite(e);
      return;
    }

    ChannelBuffer msg = (ChannelBuffer) e.getMessage();
    outboundChannel.write(msg);
  }

  @Override
  public void channelInterestChanged(ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception {
    if (outboundChannel != null) {
      outboundChannel.getPipeline().execute(new Runnable() {
        @Override
        public void run() {
          // If inboundChannel is not saturated anymore, continue accepting
          // the incoming traffic from the outboundChannel.
          if (e.getChannel().isWritable()) {
            LOG.trace("Setting outboundChannel readable.");
            outboundChannel.setReadable(true);
          } else {
            // If inboundChannel is saturated, do not read from outboundChannel
            LOG.trace("Setting outboundChannel non-readable.");
            outboundChannel.setReadable(false);
          }
        }
      });
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
    LOG.error("Got exception", e.getCause());
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
