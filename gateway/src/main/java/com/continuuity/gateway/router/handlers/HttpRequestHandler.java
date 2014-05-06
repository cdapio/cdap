package com.continuuity.gateway.router.handlers;

import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.gateway.router.RouterServiceLookup;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handler that handles HTTP requests and forwards to appropriate services. The service discovery is
 * performed using Discovery service for forwarding.
 */
public class HttpRequestHandler extends SimpleChannelUpstreamHandler {

  private final ClientBootstrap clientBootstrap;
  private final RouterServiceLookup serviceLookup;
  // Data structure is used to clean up the channel futures on connection close.
  private final Map<WrappedDiscoverable, EventSender> discoveryLookup;
  private EventSender chunkEventSender;

  public HttpRequestHandler(ClientBootstrap clientBootstrap,
                            RouterServiceLookup serviceLookup) {
    this.clientBootstrap = clientBootstrap;
    this.serviceLookup = serviceLookup;
    this.discoveryLookup = Maps.newHashMap();
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx,
                              MessageEvent event) throws Exception {

    final Channel inboundChannel = event.getChannel();
    if (event.getMessage() instanceof HttpChunk) {
      // This case below should never happen this would mean we get Chunks before HTTPMessage.
      raiseExceptionIfNull(chunkEventSender, HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           "Chunk received and event sender is null");
      chunkEventSender.sendMessage(((HttpChunk) event.getMessage()));
    } else if (event.getMessage() instanceof HttpRequest) {
      // Discover and forward event.
      HttpRequest request = (HttpRequest) event.getMessage();

      // Suspend incoming traffic until connected to the outbound service.
      inboundChannel.setReadable(false);
      WrappedDiscoverable discoverable = getDiscoverable(request,
                                                         (InetSocketAddress) inboundChannel.getLocalAddress());

      // If no event sender, make new connection, otherwise reuse existing one.
      EventSender eventSender =  discoveryLookup.get(discoverable);
      if (eventSender == null) {
        InetSocketAddress address = discoverable.getSocketAddress();

        ChannelFuture outFuture = clientBootstrap.connect(address);
        Channel outboundChannel = outFuture.getChannel();
        outboundChannel.getPipeline().addAfter("request-encoder",
                                               "outbound-handler", new OutboundHandler(inboundChannel));

        eventSender = new EventSender(outFuture);
        discoveryLookup.put(discoverable, eventSender);
      }

      // Send the message.
      eventSender.sendMessage(request);
      inboundChannel.setReadable(true);
      //Save the channelFuture for subsequent chunks
      if (request.isChunked()) {
        chunkEventSender = eventSender;
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
    for (EventSender sender : discoveryLookup.values()) {
      Closeables.closeQuietly(sender);
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
   * EventSender uses connection future or event future to send the message.
   * ConnectionFuture is used for the first event and event future for subsequent events.
   * Reason to use event future is to send chunked requests in the same channel.
   */
  private static final class EventSender implements Closeable {
    private final ChannelFuture connectionFuture;
    private final ChannelFuture eventFuture;
    private final AtomicBoolean first;

    private EventSender(ChannelFuture connectionFuture) {
      this.first = new AtomicBoolean(true);
      this.connectionFuture = connectionFuture;
      this.eventFuture = Channels.future(connectionFuture.getChannel());
    }

    void sendMessage(final Object o) {
      if (first.compareAndSet(true, false)) {
        connectionFuture.addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            Channels.write(future.getChannel(), o);
            eventFuture.setSuccess();
          }
        });
      } else {
        eventFuture.addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            Channels.write(future.getChannel(), o);
          }
        });
      }
    }

    @Override
    public void close() throws IOException {
      closeOnFlush(connectionFuture.getChannel());
    }
  }
}

