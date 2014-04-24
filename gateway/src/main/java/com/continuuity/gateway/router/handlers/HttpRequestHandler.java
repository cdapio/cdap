package com.continuuity.gateway.router.handlers;

import com.continuuity.gateway.router.HeaderDecoder;
import com.continuuity.gateway.router.RouterServiceLookup;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import org.apache.twill.discovery.Discoverable;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Handler that handles HTTP requests and forwards to appropriate services. The service discovery is
 * performed using Discovery service
 */
public class HttpRequestHandler extends SimpleChannelUpstreamHandler {
  private static final Logger LOG = LoggerFactory.getLogger(HttpRequestHandler.class);
  private final ClientBootstrap clientBootstrap;
  private final RouterServiceLookup serviceLookup;
  private final Map<Discoverable, ChannelFuture> channelFutureMap;

  public HttpRequestHandler(ClientBootstrap clientBootstrap,
                            final RouterServiceLookup serviceLookup) {
    this.clientBootstrap = clientBootstrap;
    this.serviceLookup = serviceLookup;
    this.channelFutureMap = Maps.newHashMap();
  }

  @Override
  public void messageReceived(final ChannelHandlerContext ctx,
                              final MessageEvent e) throws Exception {
    final HttpRequest msg = (HttpRequest) e.getMessage();

    // Suspend incoming traffic until connected to the outbound service.
    final Channel inboundChannel = e.getChannel();
    inboundChannel.setReadable(false);

    String path = msg.getUri();
    String host = msg.getHeader(HttpHeaders.Names.HOST);
    String httpMethod = msg.getMethod().getName();

    final HeaderDecoder.HeaderInfo headerInfo = new HeaderDecoder.HeaderInfo(path, host, httpMethod);
    int inboundPort = ((InetSocketAddress) inboundChannel.getLocalAddress()).getPort();
    Discoverable discoverable = serviceLookup.getDiscoverable(inboundPort, new Supplier<HeaderDecoder.HeaderInfo>() {
      @Override
      public HeaderDecoder.HeaderInfo get() {
        return headerInfo;
      }
    });
    if (discoverable == null) {
      inboundChannel.close();
      return;
    }

    if (channelFutureMap.containsKey(discoverable)) {
      ChannelFuture future = channelFutureMap.get(discoverable);
      future.addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
            inboundChannel.setReadable(true);
            future.getChannel().write(e.getMessage());
          }
         }
        });
    } else {
      final InetSocketAddress address = discoverable.getSocketAddress();
      ChannelFuture outFuture = clientBootstrap.connect(address);
      Channel outboundChannel = outFuture.getChannel();
      outboundChannel.getPipeline().addLast("outbound-handler", new OutboundHandler(inboundChannel));
      // the decoder is added after Outboundhandler in the pipeline as it is a downstream channel
      outboundChannel.getPipeline().addLast("HttpRequestEncoder", new HttpRequestEncoder());
      outFuture.addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
            inboundChannel.setReadable(true);

            future.getChannel().write(e.getMessage());
          }
        }
      });
      channelFutureMap.put(discoverable, outFuture);
    }
  }
 }
