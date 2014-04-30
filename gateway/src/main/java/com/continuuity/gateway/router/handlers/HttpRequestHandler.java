package com.continuuity.gateway.router.handlers;

import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.gateway.router.HeaderDecoder;
import com.continuuity.gateway.router.RouterServiceLookup;
import com.google.common.base.Supplier;
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
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestEncoder;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Handler that handles HTTP requests and forwards to appropriate services. The service discovery is
 * performed using Discovery service for forwarding.
 */
public class HttpRequestHandler extends SimpleChannelUpstreamHandler {
  private static final Logger LOG = LoggerFactory.getLogger(HttpRequestHandler.class);

  private final ClientBootstrap clientBootstrap;
  private final RouterServiceLookup serviceLookup;
  // Data structure is used to clean up the channel futures on connection close.
  private final Map<WrappedDiscoverable, ChannelFuture> discoveryLookup;
  private ChannelFuture chunkHandler;

  public HttpRequestHandler(ClientBootstrap clientBootstrap,
                            final RouterServiceLookup serviceLookup) {
    this.clientBootstrap = clientBootstrap;
    this.serviceLookup = serviceLookup;
    this.discoveryLookup = Maps.newHashMap();
  }

  @Override
  public void messageReceived(final ChannelHandlerContext ctx,
                              final MessageEvent event) throws Exception {

    final Channel inboundChannel = event.getChannel();
    if (event.getMessage() instanceof HttpChunk) {
      sendChunk((HttpChunk) event.getMessage());
    } else {
      // Discover and forward event.
      final HttpRequest request = (HttpRequest) event.getMessage();
      final HeaderDecoder.HeaderInfo headInfo = new HeaderDecoder.HeaderInfo(request.getUri(),
                                                                             request.getHeader(HttpHeaders.Names.HOST),
                                                                             request.getMethod().getName());
      // Suspend incoming traffic until connected to the outbound service.
      //TODO: Channel interest change
      //      inboundChannel.setReadable(false);
      int inboundPort = ((InetSocketAddress) inboundChannel.getLocalAddress()).getPort();

      EndpointStrategy strategy = serviceLookup.getDiscoverable(inboundPort,
                                                                new Supplier<HeaderDecoder.HeaderInfo>() {
                                                                  @Override
                                                                  public HeaderDecoder.HeaderInfo get() {
                                                                    return headInfo;
                                                                  }
                                                                });
      raiseExceptionIfNull(strategy, HttpResponseStatus.SERVICE_UNAVAILABLE,
                           "Router cannot forward this request to any service");
      Discoverable discoverable = strategy.pick();
      raiseExceptionIfNull(discoverable, HttpResponseStatus.SERVICE_UNAVAILABLE,
                           "Router cannot forward this request to any service");
      ChannelFuture future = null;

      WrappedDiscoverable wrappedDiscoverable = new WrappedDiscoverable(discoverable);
      if (discoveryLookup.containsKey(wrappedDiscoverable)) {
        future = discoveryLookup.get(wrappedDiscoverable);
      }

      if (future != null) {
        // TODO: Do the channelInterest change
        // inboundChannel.setReadable(true);
        if (future.isSuccess()) {
          Channels.write(future.getChannel(), request);
        } else {
          future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
              if (future.isSuccess()) {
                Channels.write(future.getChannel(), request);
              } else {
                // Close the connection if the connection attempt has failed.
                inboundChannel.close();
                LOG.trace("Failed to open connection from {} for {}",
                          inboundChannel.getLocalAddress(), inboundChannel.getRemoteAddress(), future.getCause());
              }
            }
          });
        }
      } else {
        final InetSocketAddress address = discoverable.getSocketAddress();
        ChannelFuture outFuture = clientBootstrap.connect(address);
        //Save the channelFuture for subsequent chunks
        if (request.isChunked()) {
          chunkHandler = Channels.future(outFuture.getChannel());
        }
        Channel outboundChannel = outFuture.getChannel();
        outboundChannel.getPipeline().addLast("request-encoder", new HttpRequestEncoder());
        outboundChannel.getPipeline().addLast("outbound-handler", new OutboundHandler(inboundChannel));
        outFuture.addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
              // TODO: Do the channelInterest change
              //inboundChannel.setReadable(true);
              Channels.write(future.getChannel(), request);
              if (request.isChunked()) {
                // Set success to start handling chunks. This will be after the HttpMessage is sent.
                HttpRequestHandler.this.chunkHandler.setSuccess();
              }
            } else {
              // Close the connection if the connection attempt has failed.
              inboundChannel.close();
              LOG.trace("Failed to open connection from {} for {}",
                        inboundChannel.getLocalAddress(), inboundChannel.getRemoteAddress(), future.getCause());
            }
          }
        });
        discoveryLookup.put(new WrappedDiscoverable(discoverable), outFuture);
      }
    }
  }

  @Override
  public void channelClosed(ChannelHandlerContext ctx,
                            ChannelStateEvent e) throws Exception {
    for (ChannelFuture cf : discoveryLookup.values()) {
      if (cf.getChannel().isOpen()) {
        cf.getChannel()
          .write(ChannelBuffers.EMPTY_BUFFER)
          .addListener(ChannelFutureListener.CLOSE);
      }
    }
    discoveryLookup.clear();
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

  /**
   * Closes the specified channel after all queued write requests are flushed.
   */
  static void closeOnFlush(Channel ch) {
    if (ch.isConnected()) {
      ch.write(ChannelBuffers.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }
  }

  //send the chunk to the ChannelFuture cached already in the first call.
  private void sendChunk(final HttpChunk chunk) {
    // This case below should never happen.
    raiseExceptionIfNull(chunkHandler, HttpResponseStatus.INTERNAL_SERVER_ERROR,
                         "Chunk received and channel future is null");
    chunkHandler.addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        future.addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            Channels.write(future.getChannel(), chunk);
          }
        });
      }
    });
  }

  private  static <T> T raiseExceptionIfNull(T reference, HttpResponseStatus status, String message) {
    if (reference == null) {
      throw new HandlerException(status, message);
    }
    return reference;
  }
}
