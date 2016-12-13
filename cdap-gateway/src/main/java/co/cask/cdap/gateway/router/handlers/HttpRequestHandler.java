/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.common.HandlerException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.gateway.router.ProxyRule;
import co.cask.cdap.gateway.router.RouterServiceLookup;
import co.cask.cdap.security.tools.PermissiveTrustManagerFactory;
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
import org.jboss.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

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

  private final AtomicInteger exceptionsHandled = new AtomicInteger(0);
  private MessageSender chunkSender;
  private volatile boolean channelClosed;

  public HttpRequestHandler(ClientBootstrap clientBootstrap,
                            RouterServiceLookup serviceLookup,
                            List<ProxyRule> proxyRules) {
    this.clientBootstrap = clientBootstrap;
    this.serviceLookup = serviceLookup;
    this.discoveryLookup = new HashMap<>();
    this.proxyRules = proxyRules;
  }

  @Override
  public void messageReceived(final ChannelHandlerContext ctx,
                              MessageEvent event) throws Exception {

    if (channelClosed) {
      return;
    }
    final Channel inboundChannel = event.getChannel();
    Object msg = event.getMessage();

    if (msg instanceof HttpChunk) {
      // This case below should never happen this would mean we get Chunks before HTTPMessage.
      if (chunkSender == null) {
        throw new HandlerException(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                   "Chunk received and event sender is null");
      }
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
      if (sender == null || !sender.isConnected()) {
        InetSocketAddress address = discoverable.getSocketAddress();

        ChannelFuture future = clientBootstrap.connect(address);
        final Channel outboundChannel = future.getChannel();
        outboundChannel.getPipeline().addAfter("request-encoder",
                                               "outbound-handler", new OutboundHandler(inboundChannel));
        if (Arrays.equals(Constants.Security.SSL_URI_SCHEME.getBytes(), discoverable.getPayload())) {
          SSLContext clientContext = null;
          try {
            clientContext = SSLContext.getInstance("TLS");
            clientContext.init(null, PermissiveTrustManagerFactory.getTrustManagers(), null);
          } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new RuntimeException("SSL is enabled for app-fabric but failed to create SSLContext in the router " +
                                         "client.", e);
          }
          SSLEngine engine = clientContext.createSSLEngine();
          engine.setUseClientMode(true);
          engine.setEnabledProtocols(new String[] {"TLSv1.2", "TLSv1.1", "TLSv1"});
          outboundChannel.getPipeline().addFirst("ssl", new SslHandler(engine));
          LOG.trace("Adding ssl handler to the pipeline.");
        }
        sender = new MessageSender(inboundChannel, future);
        discoveryLookup.put(discoverable, sender);

        // Remember the in-flight outbound channel
        inboundChannel.setAttachment(outboundChannel);
        outboundChannel.getCloseFuture().addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            inboundChannel.getPipeline().execute(new Runnable() {
              @Override
              public void run() {
                // When the outbound channel closed,
                // close the inbound channel as well if it carries the in-flight request
                if (outboundChannel.equals(inboundChannel.getAttachment())) {
                  closeOnFlush(inboundChannel);
                }
              }
            });
          }
        });
      } else {
        Channel outboundChannel = (Channel) inboundChannel.getAttachment();
        if (outboundChannel != null) {
          // Set outbound channel to be readable in case previous request has set it as non-readable
          outboundChannel.setReadable(true);
        }
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
  public void channelInterestChanged(ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception {
    final Channel outboundChannel = (Channel) e.getChannel().getAttachment();
    if (outboundChannel != null) {
      outboundChannel.getPipeline().execute(new Runnable() {
        @Override
        public void run() {
          // If inboundChannel is not saturated anymore, continue accepting
          // the outbound traffic from the outboundChannel.
          if (e.getChannel().isWritable()) {
            LOG.trace("Setting outboundChannel readable.");
            outboundChannel.setReadable(true);
          } else {
            // If inboundChannel is saturated, do not read outboundChannel
            LOG.trace("Setting outboundChannel non-readable.");
            outboundChannel.setReadable(false);
          }
        }
      });
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)  {
    Throwable cause = e.getCause();

    // avoid handling exception more than once from a handler, to avoid a possible infinite recursion
    switch (exceptionsHandled.incrementAndGet()) {
      case 1:
        // if this is the first error, break and handle the error normally (below)
        break;
      case 2:
        // if its the second time, log and return
        LOG.error("Not handling exception due to already having handled an exception in Request Handler {}",
                  ctx.getChannel(), cause);
        // fall through
      default:
        // if its the 3rd time or more, simply return. don't log, since even logging can result
        // in an exception and cause recursion
        return;
    }

    LOG.error("Exception raised in Request Handler {}", ctx.getChannel(), cause);
    if (ctx.getChannel().isConnected() && !channelClosed) {
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
    LOG.trace("Channel closed {}", ctx.getChannel());
    for (Closeable c : discoveryLookup.values()) {
      Closeables.closeQuietly(c);
    }
    channelClosed = true;
    super.channelClosed(ctx, e);
  }

  /**
   * Closes the specified channel after all queued write requests are flushed.
   */
  static void closeOnFlush(Channel ch) {
    // We need to check for both connected state and the close future.
    // This is because depending on who initiate the close, the state may be reflected in different order.
    if (ch.isConnected() && !ch.getCloseFuture().isDone()) {
      ch.write(ChannelBuffers.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }
  }

  private WrappedDiscoverable getDiscoverable(final HttpRequest httpRequest,
                                              final InetSocketAddress address) {
    EndpointStrategy strategy = serviceLookup.getDiscoverable(address.getPort(), httpRequest);
    if (strategy == null) {
      throw  new HandlerException(HttpResponseStatus.SERVICE_UNAVAILABLE,
                                  String.format("No endpoint strategy found for request : %s",
                                  httpRequest.getUri()));
    }
    Discoverable discoverable = strategy.pick();
    if (discoverable == null) {
      throw  new HandlerException(HttpResponseStatus.SERVICE_UNAVAILABLE,
                                  String.format("No discoverable found for request : %s",
                                                httpRequest.getUri()));
    }
    return new WrappedDiscoverable(discoverable);
  }

  /**
   * For sending messages to outbound channel while maintaining the order of messages according to
   * the order that {@link #send(Object)} method is called.
   *
   * It uses a lock-free algorithm similar to the one
   * in {@link co.cask.cdap.data.stream.service.ConcurrentStreamWriter} to do the write through the
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

    private boolean isConnected() {
      return channelFuture.getChannel().isConnected();
    }

    private void send(Object msg) {
      // Attach the outbound channel to the inbound to indicate the in-flight request outbound.
      inBoundChannel.setAttachment(channelFuture.getChannel());

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

