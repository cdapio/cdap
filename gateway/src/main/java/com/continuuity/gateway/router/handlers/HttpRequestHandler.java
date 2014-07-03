package com.continuuity.gateway.router.handlers;

import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.exception.HandlerException;
import com.continuuity.gateway.router.ProxyRule;
import com.continuuity.gateway.router.RouterServiceLookup;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handler that handles HTTP requests and forwards to appropriate services. The service discovery is
 * performed using Discovery service for forwarding.
 */
public class HttpRequestHandler extends SimpleChannelUpstreamHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HttpRequestHandler.class);

  private final ClientBootstrap clientBootstrap;
  private final RouterServiceLookup serviceLookup;
  // Data structure is used to clean up the channel futures on connection close.
  private final Map<WrappedDiscoverable, MessageSender> discoveryLookup;
  private final List<ProxyRule> proxyRules;

  private MessageSender chunkSender;
  private AtomicBoolean channelClosed = new AtomicBoolean(false);

  public HttpRequestHandler(ClientBootstrap clientBootstrap,
                            RouterServiceLookup serviceLookup,
                            List<ProxyRule> proxyRules) {
    this.clientBootstrap = clientBootstrap;
    this.serviceLookup = serviceLookup;
    this.discoveryLookup = Maps.newHashMap();
    this.proxyRules = proxyRules;
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx,
                              MessageEvent event) throws Exception {

    if (channelClosed.get()) {
      return;
    }
    Channel inboundChannel = event.getChannel();
    Object msg = event.getMessage();

    if (msg instanceof HttpChunk) {
      // This case below should never happen this would mean we get Chunks before HTTPMessage.
      raiseExceptionIfNull(chunkSender, HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           "Chunk received and event sender is null");
      chunkSender.send(msg);

    } else if (msg instanceof HttpRequest) {
      // Discover and forward event.
      HttpRequest request = (HttpRequest) msg;
      request = applyProxyRules(request);

      // Suspend incoming traffic until connected to the outbound service.
      inboundChannel.setReadable(false);
      WrappedDiscoverable discoverable = getDiscoverable(request,
                                                         (InetSocketAddress) inboundChannel.getLocalAddress());

      // If no event sender, make new connection, otherwise reuse existing one.
      MessageSender sender =  discoveryLookup.get(discoverable);
      if (sender == null) {
        InetSocketAddress address = discoverable.getSocketAddress();

        ChannelFuture future = clientBootstrap.connect(address);
        Channel outboundChannel = future.getChannel();
        outboundChannel.getPipeline().addAfter("request-encoder",
                                               "outbound-handler", new OutboundHandler(inboundChannel));
        sender = new MessageSender(inboundChannel, future);
        discoveryLookup.put(discoverable, sender);
      }

      // Send the message.
      sender.send(request);
      inboundChannel.setReadable(true);

      //Save the channelFuture for subsequent chunks
      if (request.isChunked()) {
        chunkSender = sender;
      }

    } else {
      super.messageReceived(ctx, event);
    }
  }

  private HttpRequest applyProxyRules(HttpRequest request) {
    for (ProxyRule rule : proxyRules) {
      request = rule.apply(request);
    }

    return request;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)  {
    Throwable cause = e.getCause();

    LOG.error("Exception raised in Request Handler {}", ctx.getChannel().getId(), cause);
    if (ctx.getChannel().isConnected() && !channelClosed.get()) {
      HttpResponse response = (cause instanceof HandlerException) ?
                              ((HandlerException) cause).createFailureResponse() :
                              new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                                                      HttpResponseStatus.INTERNAL_SERVER_ERROR);
        Channels.write(ctx, e.getFuture(), response);
        e.getFuture().addListener(ChannelFutureListener.CLOSE);
    }
  }

  @Override
  public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    // Close all event sender
    LOG.trace("Channel closed {}", ctx.getChannel().getId());
    for (Closeable c : discoveryLookup.values()) {
      Closeables.closeQuietly(c);
    }
    channelClosed.compareAndSet(false, true);
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
   * For sending messages to outbound channel while maintaining the order of messages according to
   * the order that {@link #send(Object)} method is called.
   *
   * It uses a lock-free algorithm similar to the one
   * in {@link com.continuuity.data.stream.service.ConcurrentStreamWriter} to do the write through the
   * channel callback.
   */
  private static final class MessageSender implements Closeable {
    private final Channel inBoundChannel;
    private final ChannelFuture channelFuture;
    private final Queue<OutboundMessage> messages;
    private final AtomicBoolean writer;

    private MessageSender(Channel inBoundChannel, ChannelFuture channelFuture) {
      this.inBoundChannel = inBoundChannel;
      this.channelFuture = channelFuture;
      this.messages = Queues.newConcurrentLinkedQueue();
      this.writer = new AtomicBoolean(false);
    }

    private void send(Object msg) {
      final OutboundMessage message = new OutboundMessage(msg);
      messages.add(message);
      if (channelFuture.isSuccess()) {
        flushUntilCompleted(channelFuture.getChannel(), message);
      } else {
        channelFuture.addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            if (!future.isSuccess()) {
              closeOnFlush(inBoundChannel);
              return;
            }

            flushUntilCompleted(future.getChannel(), message);
          }
        });
      }
    }

    /**
     * Writes queued messages to the given channel and keep doing it until the given message is written.
     */
    private void flushUntilCompleted(Channel channel, OutboundMessage message) {
      // Retry until the message is sent.
      while (!message.isCompleted()) {
        // If lose to be write, just yield for other threads and recheck if the message is sent.
        if (!writer.compareAndSet(false, true)) {
          Thread.yield();
          continue;
        }

        // Otherwise, send every messages in the queue and notify others by setting the completed flag
        // The visibility of the flag is guaranteed by the setting of the atomic boolean.
        try {
          OutboundMessage m = messages.poll();
          while (m != null) {
            m.write(channel);
            m.completed();
            m = messages.poll();
          }
        } finally {
          writer.set(false);
        }
      }
    }

    @Override
    public void close() throws IOException {
      closeOnFlush(channelFuture.getChannel());
    }
  }


  private static final class OutboundMessage {
    private final Object message;
    private boolean completed;

    private OutboundMessage(Object message) {
      this.message = message;
    }

    private boolean isCompleted() {
      return completed;
    }

    private void completed() {
      completed = true;
    }

    private void write(Channel channel) {
      channel.write(message);
    }
  }
}

