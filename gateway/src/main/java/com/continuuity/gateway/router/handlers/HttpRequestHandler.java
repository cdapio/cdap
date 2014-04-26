package com.continuuity.gateway.router.handlers;

import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.gateway.router.HeaderDecoder;
import com.continuuity.gateway.router.RouterServiceLookup;
import com.google.common.base.Supplier;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.twill.discovery.Discoverable;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
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
  // DiscoveryTable is used to make sure there is only one discoverable used for a service in a given channel
  // incase there are more that would mean the previously cached (stored) discovery service is un-registered
  // and this information is used to close the ChannelFuture.
  private final Table<String, WrappedDiscoverable, ChannelFuture> discoveryTable;
  private ChannelFuture channelFutureForChunkRequest;

  public HttpRequestHandler(ClientBootstrap clientBootstrap,
                            final RouterServiceLookup serviceLookup) {
    this.clientBootstrap = clientBootstrap;
    this.serviceLookup = serviceLookup;
    this.discoveryTable = HashBasedTable.create();
    channelFutureForChunkRequest = null;
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

      // We use sticky endpoint strategy for picking up discoverable. So if there is a service name and
      // if the discoverable doesn't exist which means that the ChannelFuture(s) for the service and discoverable
      // should be closed.
      if (discoveryTable.containsRow(discoverable.getName()) &&
        !discoveryTable.contains(discoverable.getName(), new WrappedDiscoverable(discoverable))) {
        // close the ChannelFuture
        for (Map.Entry<WrappedDiscoverable, ChannelFuture> entry :
          discoveryTable.row(discoverable.getName()).entrySet()) {
          entry.getValue().addListener(ChannelFutureListener.CLOSE);
        }
        // clear the row.
        discoveryTable.row(discoverable.getName()).clear();
      } else {
        future = discoveryTable.get(discoverable.getName(), new WrappedDiscoverable(discoverable));
      }

      if (future != null) {
 // TODO: Do the channelInterest change
 //        inboundChannel.setReadable(true);
        if (future.isSuccess()) {
          future.getChannel().write(request);
        } else {
          future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
              if (future.isSuccess()) {
                future.getChannel().write(request);
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
        Channel outboundChannel = outFuture.getChannel();
        outboundChannel.getPipeline().addLast("outbound-handler", new OutboundHandler(inboundChannel));
        outboundChannel.getPipeline().addLast("request-encoder", new HttpRequestEncoder());
        outFuture.addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
  // TODO: Do the channelInterest change
  //              inboundChannel.setReadable(true);
              future.getChannel().write(request);
            } else {
              // Close the connection if the connection attempt has failed.
              inboundChannel.close();
              LOG.trace("Failed to open connection from {} for {}",
                        inboundChannel.getLocalAddress(), inboundChannel.getRemoteAddress(), future.getCause());
            }
          }
        });

        discoveryTable.put(discoverable.getName(),
                           new WrappedDiscoverable(discoverable),
                           outFuture);
        //Save the channelFuture for subsequent chunks
        if (request.isChunked()) {
          channelFutureForChunkRequest = outFuture;
        }
      }
    }
  }

  @Override
  public void channelClosed(ChannelHandlerContext ctx,
                            ChannelStateEvent e) throws Exception {
    for (ChannelFuture cf : discoveryTable.values()){
      cf.getChannel().write(ChannelBuffers.EMPTY_BUFFER)
                     .addListener(ChannelFutureListener.CLOSE);
    }
    discoveryTable.clear();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    LOG.error("Exception caught in channel processing.", e.getCause());
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
    if (channelFutureForChunkRequest != null) {
      if (!channelFutureForChunkRequest.getChannel().isConnected()) {
           return; //NOThing to do.
      }
      channelFutureForChunkRequest.getChannel().write(chunk);
    } else {
      //Chunk as the first event. This can never happen!
      // THrow an exception
      throw new HandlerException(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Chunk received and channelfuture is null");
    }

    if (chunk.isLast()) {
      //lastChunk nothing much to do
      channelFutureForChunkRequest = null;
      return;
    }
  }

  private  static <T> T raiseExceptionIfNull(T reference, HttpResponseStatus status, String message) {
    if (reference == null) {
      throw new HandlerException(status, message);
    }
    return reference;
  }
}
