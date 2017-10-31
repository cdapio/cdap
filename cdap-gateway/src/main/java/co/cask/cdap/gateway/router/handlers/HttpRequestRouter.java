/*
 * Copyright © 2017 Cask Data, Inc.
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
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.gateway.router.RouterServiceLookup;
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
import org.apache.twill.discovery.Discoverable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import javax.annotation.Nullable;
import javax.net.ssl.SSLException;

/**
 * A {@link ChannelInboundHandler} for forwarding incoming request to appropriate CDAP service endpoint
 * based on the request. This class doesn't need to be thread safe as Netty will make sure there is no
 * concurrent calls to ChannelHandler and each call always have a happens-before relationship to the previous call.
 */
public class HttpRequestRouter extends ChannelDuplexHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HttpRequestRouter.class);
  private static final byte[] HTTPS_SCHEME_BYTES = Constants.Security.SSL_URI_SCHEME.getBytes();

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
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
    try {
      final Channel inboundChannel = ctx.channel();
      ChannelFutureListener writeCompletedListener = getFailureResponseListener(inboundChannel);

      if (msg instanceof HttpRequest) {
        inflightRequests++;
        if (inflightRequests != 1) {
          // This means there is concurrent request via HTTP pipelining.
          // Simply return
          // At the end of the first response, we'll respond to all the other requests as well
          return;
        }

        // Disable read until sending of this request object is completed successfully
        // This is for handling the initial connection delay
        inboundChannel.config().setAutoRead(false);
        writeCompletedListener = new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
              inboundChannel.config().setAutoRead(true);
            } else {
              getFailureResponseListener(inboundChannel).operationComplete(future);
            }
          }
        };
        HttpRequest request = (HttpRequest) msg;
        currentMessageSender = getMessageSender(
          inboundChannel, getDiscoverable(request, (InetSocketAddress) inboundChannel.localAddress())
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
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    if (currentMessageSender != null) {
      currentMessageSender.flush();
    }
    ctx.fireChannelReadComplete();
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
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
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    HttpResponse response = cause instanceof HandlerException
      ? ((HandlerException) cause).createFailureResponse()
      : createErrorResponse(cause);
    ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
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

  private ChannelFutureListener getFailureResponseListener(final Channel inboundChannel) {
    if (failureResponseListener == null) {
      failureResponseListener = new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (!future.isSuccess()) {
            inboundChannel.writeAndFlush(createErrorResponse(future.cause())).addListener(ChannelFutureListener.CLOSE);
          }
        }
      };
    }
    return failureResponseListener;
  }

  /**
   * Finds the {@link Discoverable} for the given {@link HttpRequest} to route to.
   */
  private Discoverable getDiscoverable(HttpRequest httpRequest, InetSocketAddress address) {
    EndpointStrategy strategy = serviceLookup.getDiscoverable(address.getPort(), httpRequest);
    if (strategy == null) {
      throw new HandlerException(HttpResponseStatus.SERVICE_UNAVAILABLE,
                                 "No endpoint strategy found for request " + getRequestLine(httpRequest));
    }
    Discoverable discoverable = strategy.pick();
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
  private MessageSender getMessageSender(Channel inboundChannel,
                                         Discoverable discoverable) throws SSLException {
    Queue<MessageSender> senders = messageSenders.get(discoverable);
    if (senders == null) {
      senders = new LinkedList<>();
      messageSenders.put(discoverable, senders);
    }

    MessageSender sender = senders.poll();

    // Found a MessageSender to reuse, return it
    if (sender != null) {
      LOG.trace("Reuse message sender for {}", discoverable);
      return sender;
    }

    // Create new MessageSender
    sender = new MessageSender(cConf, inboundChannel, discoverable);
    LOG.trace("Create new message sender for {}", discoverable);
    return sender;
  }

  private String getRequestLine(HttpRequest request) {
    return request.method() + " " + request.uri() + " " + request.protocolVersion();
  }

  private HttpResponse createPipeliningNotSupported() {
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_IMPLEMENTED);
    response.content().writeCharSequence("HTTP pipelining is not supported", StandardCharsets.UTF_8);
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
   * For sending messages to outbound channel while maintaining the order of messages according to
   * the order that {@link #send(Object, ChannelFutureListener)} method is called.
   */
  private static final class MessageSender implements Flushable, Closeable {

    private final Discoverable discoverable;
    private final Queue<OutboundMessage> pendingMessages;
    private final Bootstrap clientBootstrap;
    private volatile SslContext sslContext;
    private Channel outboundChannel;
    private boolean closed;
    private boolean connecting;

    private MessageSender(final CConfiguration cConf, final Channel inboundChannel, final Discoverable discoverable) {
      this.discoverable = discoverable;
      this.pendingMessages = new LinkedList<>();

      // A channel listener for resetting the state of this message sender on closing of outbound channel
      final ChannelFutureListener onCloseResetListener = new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          outboundChannel = null;
          connecting = false;
        }
      };

      // Create a client Bootstrap for connecting to internal services
      // It must be create using the same EventLoopGroup as the inbound channel to make
      // sure thread safety between the inbound and outbound channels callbacks.
      this.clientBootstrap = new Bootstrap()
        .group(inboundChannel.eventLoop())
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
                             new IdleStateHandler(0, 0, cConf.getInt(Constants.Router.CONNECTION_TIMEOUT_SECS)));
            pipeline.addLast("codec", new HttpClientCodec());
            pipeline.addLast("forwarder", new OutboundHandler(inboundChannel));
          }
        });
    }

    /**
     * Sends a message to the outbound channel.
     *
     * @param msg the message to be sent
     * @param writeCompletedListener a {@link ChannelFutureListener} to be notified when the write completed
     */
    void send(Object msg, ChannelFutureListener writeCompletedListener) {
      if (outboundChannel != null) {
        outboundChannel.write(msg).addListener(writeCompletedListener);
        return;
      }

      // If not yet connected or still connecting, just add the message to the pending queue
      pendingMessages.add(new OutboundMessage(msg, writeCompletedListener));

      // If connecting, we can just return. When the connection completed, it will send all messages in the queue.
      if (connecting) {
        return;
      }

      // Make a new connection
      ChannelFuture connectFuture = clientBootstrap.connect(discoverable.getSocketAddress());
      connectFuture.addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          // Always remember the outbound channel even if the connection fail.
          // This make sure any message received before the inbound channel is closed will not get forwarded
          outboundChannel = future.channel();
          connecting = false;

          if (future.isSuccess()) {
            // If this sender is closed (because inbound channel is closed), just close the outbound channel
            if (closed) {
              Channels.closeOnFlush(outboundChannel);
            }
          }
          OutboundMessage message = pendingMessages.poll();
          while (message != null) {
            processMessage(message, future);
            message = pendingMessages.poll();
          }
          if (future.isSuccess()) {
            flush();
          }
        }
      });

      connecting = true;
    }

    @Override
    public void flush() {
      if (outboundChannel != null && !closed) {
        outboundChannel.flush();
      }
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        if (outboundChannel != null) {
          Channels.closeOnFlush(outboundChannel);
        }
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
    private SslHandler getSslHandler(Discoverable discoverable, ByteBufAllocator alloc) throws SSLException {
      if (!Arrays.equals(HTTPS_SCHEME_BYTES, discoverable.getPayload())) {
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
     * Process the message by sending to the given channel or have a failure call to the message callback,
     * depending on the state of this sender.
     */
    private void processMessage(OutboundMessage message, ChannelFuture channelFuture) throws Exception {
      Channel channel = channelFuture.channel();

      if (closed) {
        message.writeCompletedListener.operationComplete(channel.newFailedFuture(new ClosedChannelException()));
        return;
      }
      if (channelFuture.isSuccess()) {
        message.write(channelFuture.channel());
      } else {
        message.writeCompletedListener.operationComplete(channelFuture);
      }
    }
  }

  /**
   * A wrapper for a message and the {@link ChannelPromise} to use for writing to a {@link Channel}.
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
