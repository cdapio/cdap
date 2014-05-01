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
  private final Map<WrappedDiscoverable, EventSender> discoveryLookup;
  private EventSender chunkHandler;



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
      // This case below should never happen.
      raiseExceptionIfNull(chunkHandler, HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           "Chunk received and channel future is null");
      chunkHandler.sendMessage(((HttpChunk) event.getMessage()));
    } else if (event.getMessage() instanceof HttpRequest) {
      // Discover and forward event.
      HttpRequest request = (HttpRequest) event.getMessage();
      HeaderDecoder.HeaderInfo headInfo = new HeaderDecoder.HeaderInfo(request.getUri(),
                                                                       request.getHeader(HttpHeaders.Names.HOST),
                                                                       request.getMethod().getName());
      // Suspend incoming traffic until connected to the outbound service.
      inboundChannel.setReadable(false);
      WrappedDiscoverable discoverable = getDiscoverable(headInfo,
                                                         (InetSocketAddress) inboundChannel.getLocalAddress());

      if (discoveryLookup.containsKey(discoverable)) {
        inboundChannel.setReadable(true);
        EventSender eventSender =  discoveryLookup.get(discoverable);
        eventSender.sendMessage(request);
        //Save the channelFuture for subsequent chunks
        if (request.isChunked()) {
          chunkHandler = eventSender;
        }
      } else {
        InetSocketAddress address = discoverable.getSocketAddress();

        ChannelFuture outFuture = clientBootstrap.connect(address);
        Channel outboundChannel = outFuture.getChannel();
        outboundChannel.getPipeline().addLast("request-encoder", new HttpRequestEncoder());
        outboundChannel.getPipeline().addLast("outbound-handler", new OutboundHandler(inboundChannel));

        EventSender eventSender = new EventSender(outFuture);
        eventSender.sendMessage(request);
        inboundChannel.setReadable(true);
        //Save the channelFuture for subsequent chunks
        if (request.isChunked()) {
          chunkHandler = eventSender;
        }
        discoveryLookup.put(discoverable, eventSender);
      }

    } else {
      throw new HandlerException(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Only HttpRequests are handled by router");
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

  /**
   * Closes the specified channel after all queued write requests are flushed.
   */
  static void closeOnFlush(Channel ch) {
    if (ch.isConnected()) {
      ch.write(ChannelBuffers.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }
  }

  private  static <T> T raiseExceptionIfNull(T reference, HttpResponseStatus status, String message) {
    if (reference == null) {
      throw new HandlerException(status, message);
    }
    return reference;
  }

  private WrappedDiscoverable getDiscoverable(final HeaderDecoder.HeaderInfo headInfo,
                                              final InetSocketAddress address) {
    EndpointStrategy strategy = serviceLookup.getDiscoverable(address.getPort(),
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

    return new WrappedDiscoverable(discoverable);
  }

  /**
   * EventSender uses connection future or event future to send the message.
   * ConnectionFuture is used for the first event and event future for subsequent events.
   * Reason to use event future is to send chunked requests in the same channel.
   */
  private static final class EventSender {
    private final ChannelFuture connectionFuture;
    private final ChannelFuture eventFuture;
    boolean connected;


    private EventSender(ChannelFuture connectionFuture) {
      connected = false;
      this.connectionFuture = connectionFuture;
      this.eventFuture = Channels.future(connectionFuture.getChannel());
    }

    void sendMessage(final Object o) {
      if (!connected) {
        if (connectionFuture.isSuccess()) {
          Channels.write(connectionFuture.getChannel(), o);
          eventFuture.setSuccess();
          connected = true;
        } else {
          connectionFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
              Channels.write(future.getChannel(), o);
              connected = true;
              eventFuture.setSuccess();
            }
          });
        }
      } else {
        if (eventFuture.isSuccess()) {
          Channels.write(eventFuture.getChannel(), o);
        } else {
          eventFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
              Channels.write(future.getChannel(), o);
            }
          });
        }
      }
    }
  }


}

