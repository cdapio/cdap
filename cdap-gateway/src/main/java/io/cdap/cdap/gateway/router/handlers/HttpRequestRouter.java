/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.cdap.gateway.router.handlers;

import io.cdap.cdap.common.HandlerException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.EndpointStrategy;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.Channels;
import io.cdap.cdap.gateway.router.RouterServiceLookup;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.ReferenceCountUtil;
import java.io.Closeable;
import java.io.Flushable;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.net.ssl.SSLException;
import org.apache.twill.discovery.Discoverable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ChannelInboundHandler} for forwarding incoming request to appropriate CDAP service
 * endpoint based on the request. This class doesn't need to be thread safe as Netty will make sure
 * there is no concurrent calls to ChannelHandler and each call always have a happens-before
 * relationship to the previous call.
 */
public class HttpRequestRouter extends ChannelDuplexHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HttpRequestRouter.class);

  private final CConfiguration cConf;
  private final RouterServiceLookup serviceLookup;
  private final Map<Discoverable, Queue<MessageSender>> messageSenders;
  private int inflightRequests;
  private MessageSender currentMessageSender;
  private ChannelFutureListener failureResponseListener;

  public HttpRequestRouter(CConfiguration cConf, RouterServiceLookup serviceLookup) {
    this.cConf = cConf;
    this.serviceLookup = serviceLookup;
    this.messageSenders = new HashMap<>();
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    try {
      final Channel httpRequestChannel = ctx.channel();
      ChannelFutureListener writeCompletedListener = getFailureResponseListener(httpRequestChannel);

      if (msg instanceof HttpRequest) {
        HttpRequest request = (HttpRequest) msg;

        // For "/" request, response with 200. This is for load balancer health check
        if ("/".equals(request.uri())) {
          HttpResponse response = new DefaultFullHttpResponse(request.protocolVersion(),
              HttpResponseStatus.OK);
          HttpUtil.setContentLength(response, 0L);
          httpRequestChannel.writeAndFlush(response);
          return;
        }

        inflightRequests++;
        if (inflightRequests != 1) {
          // This means there is concurrent request via HTTP pipelining.
          // Simply return
          // At the end of the first response, we'll respond to all the other requests as well
          return;
        }

        // Disable read until sending of this request object is completed successfully
        // This is for handling the initial connection delay
        httpRequestChannel.config().setAutoRead(false);
        writeCompletedListener = future -> {
          if (future.isSuccess()) {
            httpRequestChannel.config().setAutoRead(true);
          } else {
            getFailureResponseListener(httpRequestChannel).operationComplete(future);
          }
        };

        currentMessageSender = getMessageSender(
            httpRequestChannel, getDiscoverable(request)
        );
      }

      if (inflightRequests == 1 && currentMessageSender != null) {
        ReferenceCountUtil.retain(msg);
        currentMessageSender.send(msg, writeCompletedListener);
      }
    } finally {
      ReferenceCountUtil.release(msg);
    }
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    if (currentMessageSender != null) {
      currentMessageSender.flush();
    }
    ctx.fireChannelReadComplete();
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    ctx.writeAndFlush(msg, promise);

    // When the response for the first request is completed, write N failure responses for pipelining requests (if any).
    if (msg instanceof LastHttpContent) {
      for (int i = 0; i < inflightRequests - 1; i++) {
        ctx.writeAndFlush(createPipeliningNotSupported());
      }
      inflightRequests = 0;

      // Recycle the message sender
      if (currentMessageSender != null) {
        messageSenders.get(currentMessageSender.getDiscoverable()).add(currentMessageSender);
      }
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    HttpResponse response = cause instanceof HandlerException
        ? ((HandlerException) cause).createFailureResponse()
        : createErrorResponse(cause);
    HttpUtil.setKeepAlive(response, false);
    ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    if (currentMessageSender != null) {
      currentMessageSender.close();
    }
    for (Map.Entry<Discoverable, Queue<MessageSender>> entry : messageSenders.entrySet()) {
      for (MessageSender sender : entry.getValue()) {
        sender.close();
      }
    }
    ctx.fireChannelInactive();
  }

  /**
   * [CDAP-21071] Handles the case by stopping the internalServiceChannel of Service -> Router when
   * the response from Service -> Router is faster than the response from Router to the Client.
   */
  @Override
  public void channelWritabilityChanged(ChannelHandlerContext ctx) {
    if (inflightRequests > 0 && currentMessageSender != null) {
      final Channel httpRequestChannel = ctx.channel();
      ctx.executor().execute(() -> {
        // If httpRequestChannel is not saturated anymore, continue accepting
        // the incoming traffic from the internalServiceChannel for service<>router.
        // If httpRequestChannel is saturated, do not read internalServiceChannel
        currentMessageSender.setAutoRead(httpRequestChannel.isWritable());
      });
    }
    ctx.fireChannelWritabilityChanged();
  }

  private ChannelFutureListener getFailureResponseListener(final Channel httpRequestChannel) {
    if (failureResponseListener == null) {
      failureResponseListener = future -> {
        if (!future.isSuccess()) {
          HttpResponse response = createErrorResponse(future.cause());
          HttpUtil.setKeepAlive(response, false);
          httpRequestChannel.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
      };
    }
    return failureResponseListener;
  }

  /**
   * Finds the {@link Discoverable} for the given {@link HttpRequest} to route to.
   */
  private Discoverable getDiscoverable(HttpRequest httpRequest) {
    EndpointStrategy strategy = serviceLookup.getDiscoverable(httpRequest);
    if (strategy == null) {
      throw new HandlerException(HttpResponseStatus.SERVICE_UNAVAILABLE,
          "No endpoint strategy found for request " + getRequestLine(httpRequest));
    }
    // Do a non-blocking pick first. If the service has been discovered before, this should return an endpoint
    // immediately.
    Discoverable discoverable = strategy.pick();
    if (discoverable != null) {
      return discoverable;
    }

    // Do a blocking pick for up to 1 second. It is for the case where a service is being discovered for the first time,
    // in which population of the cache might take time.
    discoverable = strategy.pick(1, TimeUnit.SECONDS);
    if (discoverable == null) {
      throw new HandlerException(HttpResponseStatus.SERVICE_UNAVAILABLE,
          "No discoverable found for request " + getRequestLine(httpRequest));
    }
    return discoverable;
  }

  /**
   * Returns the {@link MessageSender} for writing messages to the endpoint represented by the given
   * {@link Discoverable}.
   */
  private MessageSender getMessageSender(Channel httpRequestChannel,
      Discoverable discoverable) {
    Queue<MessageSender> senders = messageSenders.computeIfAbsent(discoverable,
        k -> new LinkedList<>());

    MessageSender sender = senders.poll();

    // Found a MessageSender to reuse, return it
    if (sender != null) {
      LOG.trace("Reuse message sender for {}", discoverable);
      return sender;
    }

    // Create new MessageSender
    sender = new MessageSender(cConf, httpRequestChannel, discoverable);
    LOG.trace("Create new message sender for {}", discoverable);
    return sender;
  }

  private String getRequestLine(HttpRequest request) {
    return request.method() + " " + request.uri() + " " + request.protocolVersion();
  }

  private HttpResponse createPipeliningNotSupported() {
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
        HttpResponseStatus.NOT_IMPLEMENTED);
    response.content()
        .writeCharSequence("HTTP pipelining is not supported", StandardCharsets.UTF_8);
    HttpUtil.setContentLength(response, response.content().readableBytes());
    return response;
  }

  private static HttpResponse createErrorResponse(Throwable cause) {
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
        HttpResponseStatus.INTERNAL_SERVER_ERROR);
    if (cause.getMessage() != null) {
      response.content().writeCharSequence(cause.getMessage(), StandardCharsets.UTF_8);
    }
    HttpUtil.setContentLength(response, response.content().readableBytes());
    return response;
  }

  /**
   * For sending messages to internalServiceChannel while maintaining the order of messages
   * according to the order that {@link #send(Object, ChannelFutureListener)} method is called.
   */
  private static final class MessageSender implements Flushable, Closeable {

    private final Discoverable discoverable;
    private final Queue<OutboundMessage> pendingMessages;
    private final Bootstrap clientBootstrap;
    private final AtomicReference<ChannelFuture> internalServiceChannelFutureRef;

    private volatile SslContext sslContext;

    private MessageSender(final CConfiguration cConf, final Channel httpRequestChannel,
        final Discoverable discoverable) {
      this.discoverable = discoverable;
      this.pendingMessages = new LinkedList<>();
      this.internalServiceChannelFutureRef = new AtomicReference<>();

      // A channel listener for resetting the state of this message sender on
      // closing of internalServiceChannel
      ChannelFutureListener onCloseResetListener = closedFuture -> {
        ChannelFuture connectFuture = internalServiceChannelFutureRef.get();
        if (connectFuture != null && connectFuture.channel().equals(closedFuture.channel())) {
          internalServiceChannelFutureRef.compareAndSet(connectFuture, null);
        }
      };

      // Create a client Bootstrap for connecting to internal services
      // It must be created using the same EventLoopGroup as the inbound channel to make
      // sure thread safety between the httpRequestChannel and internalServiceChannel callbacks.
      this.clientBootstrap = new Bootstrap()
          .group(httpRequestChannel.eventLoop())
          .channel(NioSocketChannel.class)
          .option(ChannelOption.SO_KEEPALIVE, true)
          .handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
              ch.closeFuture().addListener(onCloseResetListener);
              ChannelPipeline pipeline = ch.pipeline();

              SslHandler sslHandler = getSslHandler(discoverable, ch.alloc());
              if (sslHandler != null) {
                pipeline.addLast("ssl", sslHandler);
              }
              pipeline.addLast("idle-state-handler",
                  new IdleStateHandler(0, 0,
                      cConf.getInt(Constants.Router.CONNECTION_TIMEOUT_SECS)));
              pipeline.addLast("codec", new HttpClientCodec());
              pipeline.addLast("forwarder", new InternalServiceRequestHandler(httpRequestChannel));
            }
          });
    }

    /**
     * Sends a message to the internalServiceChannel.
     *
     * @param msg the message to be sent
     * @param writeCompletedListener a {@link ChannelFutureListener} to be notified when write
     *     completed
     */
    void send(Object msg, ChannelFutureListener writeCompletedListener) {
      // If not yet connected or still connecting, just add the message to the pending queue
      pendingMessages.add(new OutboundMessage(msg, writeCompletedListener));

      // Get or create a connection to the internal service
      ChannelFuture connectFuture = internalServiceChannelFutureRef.get();
      while (connectFuture == null) {
        connectFuture = clientBootstrap.connect(discoverable.getSocketAddress());
        if (!internalServiceChannelFutureRef.compareAndSet(null, connectFuture)) {
          connectFuture.addListener(
              (ChannelFutureListener) channelFuture -> channelFuture.channel().close());
          connectFuture = internalServiceChannelFutureRef.get();
        }
      }

      // Send all the pending messages to the internal services
      connectFuture.addListener((ChannelFutureListener) this::processAllMessages);
    }

    @Override
    public void flush() {
      ChannelFuture connectFuture = internalServiceChannelFutureRef.get();
      if (connectFuture != null) {
        Channel channel = connectFuture.channel();
        if (channel.isActive()) {
          channel.flush();
        }
      }
    }

    @Override
    public void close() {
      ChannelFuture connectFuture = internalServiceChannelFutureRef.get();
      if (connectFuture != null) {
        internalServiceChannelFutureRef.compareAndSet(connectFuture, null);
        LOG.info("Closing channel to {} with id {}",
            discoverable.getSocketAddress(), connectFuture.channel().id().asShortText());
        Channels.closeOnFlush(connectFuture.channel());
      }
    }

    Discoverable getDiscoverable() {
      return discoverable;
    }

    /**
     * Returns the {@link SslContext} to be used for a given discoverable endpoint
     *
     * @param discoverable the endpoint to connect to
     * @return the {@link SslContext} or {@code null} if SSL is not needed
     */
    @Nullable
    private SslHandler getSslHandler(Discoverable discoverable, ByteBufAllocator alloc)
        throws SSLException {
      if (!URIScheme.HTTPS.isMatch(discoverable)) {
        return null;
      }
      SslContext context = sslContext;
      if (context != null) {
        return context.newHandler(alloc);
      }
      synchronized (this) {
        context = sslContext;
        if (context == null) {
          sslContext = context = SslContextBuilder.forClient()
              .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        }
        return context.newHandler(alloc);
      }
    }

    /**
     * Process the message by sending to the given channel or have a failure call to the message
     * callback, depending on the state of this sender. This method should only be called from the
     * callback thread from the given {@link ChannelFuture}.
     */
    private void processAllMessages(ChannelFuture channelFuture) throws Exception {
      OutboundMessage message = pendingMessages.poll();
      while (message != null) {
        if (channelFuture.isSuccess()) {
          Channel channel = channelFuture.channel();
          if (channel.isOpen()) {
            LOG.info("Sending message to {} via channel {}",
                discoverable.getName(), channel.id().asShortText());
            message.write(channel);
          } else {
            LOG.info("Drop message to {} on channel {} due to channel closed",
                discoverable.getName(), channel.id().asShortText());
            message.writeCompletedListener.operationComplete(
                channel.newFailedFuture(new ClosedChannelException()));
          }
        } else {
          LOG.info("Drop message to {} due to connection failure", discoverable.getName());
          message.writeCompletedListener.operationComplete(channelFuture);
        }
        message = pendingMessages.poll();
      }
      flush();
    }

    /**
     * Setting the reading capability (ChannelHandlerContext. read())of a channel.
     */
    private void setAutoRead(Boolean isAutoRead) {
      ChannelFuture connectFuture = internalServiceChannelFutureRef.get();
      if (connectFuture != null) {
        LOG.info("Message sender's internalServiceChannel {} readable is set to {}.",
            connectFuture.channel().id().asShortText(), isAutoRead);
        connectFuture.channel().config().setAutoRead(isAutoRead);
      }
    }
  }

  /**
   * A wrapper for a message and the {@link ChannelPromise} to use for writing to a
   * {@link Channel}.
   */
  private static final class OutboundMessage {

    private final Object message;
    private final ChannelFutureListener writeCompletedListener;

    OutboundMessage(Object message, ChannelFutureListener writeCompletedListener) {
      this.message = message;
      this.writeCompletedListener = writeCompletedListener;
    }

    void write(Channel channel) {
      channel.write(message).addListener(writeCompletedListener);
    }
  }
}
