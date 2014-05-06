package com.continuuity.gateway.router.handlers;

import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.gateway.router.RouterServiceLookup;
import com.google.common.collect.Maps;
import org.apache.twill.discovery.Discoverable;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Handler that handles HTTP requests and forwards to appropriate services. The service discovery is
 * performed using Discovery service for forwarding.
 */
public class HttpRequestHandler extends SimpleChannelUpstreamHandler {

  private final ClientBootstrap clientBootstrap;
  private final RouterServiceLookup serviceLookup;
  // Data structure is used to clean up the channel futures on connection close.
  private final Map<WrappedDiscoverable, ChannelFuture> discoveryLookup;
  private ChannelFuture chunkFuture;

  public HttpRequestHandler(ClientBootstrap clientBootstrap,
                            RouterServiceLookup serviceLookup) {
    this.clientBootstrap = clientBootstrap;
    this.serviceLookup = serviceLookup;
    this.discoveryLookup = Maps.newHashMap();
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx,
                              MessageEvent event) throws Exception {

    Channel inboundChannel = event.getChannel();
    Object msg = event.getMessage();

    if (msg instanceof HttpChunk) {
      // This case below should never happen this would mean we get Chunks before HTTPMessage.
      raiseExceptionIfNull(chunkFuture, HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           "Chunk received and event sender is null");
      writeMessage(chunkFuture, msg);

    } else if (msg instanceof HttpRequest) {
      // Discover and forward event.
      HttpRequest request = (HttpRequest) msg;

      // Suspend incoming traffic until connected to the outbound service.
      inboundChannel.setReadable(false);
      WrappedDiscoverable discoverable = getDiscoverable(request,
                                                         (InetSocketAddress) inboundChannel.getLocalAddress());

      // If no event sender, make new connection, otherwise reuse existing one.
      ChannelFuture future =  discoveryLookup.get(discoverable);
      if (future == null) {
        InetSocketAddress address = discoverable.getSocketAddress();

        future = clientBootstrap.connect(address);
        Channel outboundChannel = future.getChannel();
        outboundChannel.getPipeline().addAfter("request-encoder",
                                               "outbound-handler", new OutboundHandler(inboundChannel));
        discoveryLookup.put(discoverable, future);
      }

      // Send the message.
      writeMessage(future, request);
      inboundChannel.setReadable(true);

      //Save the channelFuture for subsequent chunks
      if (request.isChunked()) {
        chunkFuture = future;
      }

    } else {
      super.messageReceived(ctx, event);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    Throwable cause = e.getCause();
    if (cause instanceof HandlerException) {
      ctx.getChannel().write(((HandlerException) cause).createFailureResponse())
        .addListener(ChannelFutureListener.CLOSE);
    } else {
      HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                                                      HttpResponseStatus.INTERNAL_SERVER_ERROR);
      ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
    }
  }

  @Override
  public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    // Close all event sender
    for (ChannelFuture future : discoveryLookup.values()) {
      closeOnFlush(future.getChannel());
    }
    super.channelClosed(ctx, e);
  }

  /**
   * Closes the specified channel after all queued write requests are flushed.
   */
  static void closeOnFlush(Channel ch) {
    if (ch.isConnected()) {
      ch.write(ChannelBuffers.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }
  }

  private static <T> T raiseExceptionIfNull(T reference, HttpResponseStatus status, String message) {
    if (reference == null) {
      throw new HandlerException(status, message);
    }
    return reference;
  }

  private WrappedDiscoverable getDiscoverable(final HttpRequest httpRequest,
                                              final InetSocketAddress address) {
    EndpointStrategy strategy = serviceLookup.getDiscoverable(address.getPort(), httpRequest);
    raiseExceptionIfNull(strategy, HttpResponseStatus.SERVICE_UNAVAILABLE,
                         "Router cannot forward this request to any service");
    Discoverable discoverable = strategy.pick();
    raiseExceptionIfNull(discoverable, HttpResponseStatus.SERVICE_UNAVAILABLE,
                         "Router cannot forward this request to any service");

    return new WrappedDiscoverable(discoverable);
  }

  /**
   * Sends a message to a channel, carried inside the given {@link ChannelFuture}. This method will block
   * until the given future is completed.
   *
   * @param future Future to block on and also carry the channel to write to.
   * @param o The message.
   * @throws InterruptedException if there is thread interruption while waiting for the future to complete.
   */
  private void writeMessage(ChannelFuture future, Object o) throws InterruptedException {
    if (!future.isSuccess()) {
      future.sync();
    }
    Channels.write(future.getChannel(), o);
  }
}

