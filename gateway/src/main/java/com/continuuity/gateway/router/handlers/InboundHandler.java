package com.continuuity.gateway.router.handlers;

import com.continuuity.gateway.router.HeaderInfo;
import com.continuuity.gateway.router.RouterServiceLookup;
import com.continuuity.security.auth.TokenValidator;
import org.apache.twill.discovery.Discoverable;
import com.google.common.base.Supplier;
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
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestEncoder;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetSocketAddress;


/**
 * Proxies incoming requests to a discoverable endpoint.
 */
public class InboundHandler extends SimpleChannelUpstreamHandler {
  private static final Logger LOG = LoggerFactory.getLogger(InboundHandler.class);

  private final ClientBootstrap clientBootstrap;
  private final RouterServiceLookup serviceLookup;

  private volatile Channel outboundChannel;
  private TokenValidator tokenValidator;
  private boolean securityEnabled;
  private String realm;

  public InboundHandler(String realm, ClientBootstrap clientBootstrap, final RouterServiceLookup serviceLookup,
                        TokenValidator tokenValidator, boolean securityEnabled) {
    this.clientBootstrap = clientBootstrap;
    this.serviceLookup = serviceLookup;
    this.tokenValidator = tokenValidator;
    this.securityEnabled = securityEnabled;
    this.realm = realm;

  }


  private void openOutboundAndWrite(MessageEvent e) throws Exception {
    final HttpRequest msg = (HttpRequest) e.getMessage();

    // Suspend incoming traffic until connected to the outbound service.
    final Channel inboundChannel = e.getChannel();
    inboundChannel.setReadable(false);

    String auth = msg.getHeader(HttpHeaders.Names.AUTHORIZATION);
    String path = msg.getUri();
    String host = msg.getHeader(HttpHeaders.Names.HOST);
    String httpMethod = msg.getMethod().getName();
    String accessToken = null;

    if (auth != null) {
      int spIndex = auth.trim().indexOf(' ') + 1;
      if (spIndex != -1) {
        accessToken = auth.substring(spIndex).trim();
      }
    }
    //Decoding the header
    final HeaderInfo headerInfo = new HeaderInfo(path, host, httpMethod);

    if (securityEnabled) {
      TokenValidator.State tokenState = tokenValidator.validate(accessToken);
      HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.UNAUTHORIZED);
      switch (tokenState) {
        case TOKEN_MISSING:
          httpResponse.addHeader(HttpHeaders.Names.WWW_AUTHENTICATE, "Bearer realm=\"" + realm + "\"");
          httpResponse.setHeader(HttpHeaders.Names.CONTENT_LENGTH, 0);
          break;

        case TOKEN_INVALID:
        case TOKEN_EXPIRED:
        case TOKEN_INTERNAL:
          httpResponse.addHeader(HttpHeaders.Names.WWW_AUTHENTICATE, "Bearer realm=\"" + realm + "\"" +
            "  error=\"invalid_token\"" +
            "  error_description=\"" + tokenState.getMsg() + "\"");
          httpResponse.setHeader(HttpHeaders.Names.CONTENT_LENGTH, 0);
          break;
      }
      if (tokenState != TokenValidator.State.TOKEN_VALID) {
        inboundChannel.getPipeline().addLast("encoder", new HttpResponseEncoder());
        e.getChannel().write(httpResponse).addListener(ChannelFutureListener.CLOSE);
        return;
      }
    }

    // Discover endpoint.
    int inboundPort = ((InetSocketAddress) inboundChannel.getLocalAddress()).getPort();
    Discoverable discoverable = serviceLookup.getDiscoverable(inboundPort, new Supplier<HeaderInfo>() {
      @Override
      public HeaderInfo get() {
        return headerInfo;
      }
    });

    if (discoverable == null) {
      inboundChannel.close();
      return;
    }

    // Connect to outbound service.
    final InetSocketAddress address = discoverable.getSocketAddress();
    LOG.trace("Opening connection from {} to {} for {}",
              inboundChannel.getLocalAddress(), address, inboundChannel.getRemoteAddress());
    ChannelFuture outFuture = clientBootstrap.connect(address);

    outboundChannel = outFuture.getChannel();
    outFuture.addListener(new ChannelFutureListener() {
      public void operationComplete(ChannelFuture future) throws Exception {
        if (future.isSuccess()) {

          outboundChannel.getPipeline().addLast("outbound-handler", new OutboundHandler(inboundChannel));

          // the decoder is added after Outboundhandler in the pipeline as it is a downstream channel
          outboundChannel.getPipeline().addLast("HttpRequestEncoder", new HttpRequestEncoder());

          // Write the message to outBoundChannel.
          outboundChannel.write(msg);

          // Begin to accept incoming traffic.
          inboundChannel.setReadable(true);
          LOG.trace("Connection opened from {} to {} for {}",
                    inboundChannel.getLocalAddress(), address, inboundChannel.getRemoteAddress());
        } else {
          // Close the connection if the connection attempt has failed.
          inboundChannel.close();
          LOG.trace("Failed to open connection from {} to {} for {}",
                    inboundChannel.getLocalAddress(), address, inboundChannel.getRemoteAddress(), future.getCause());
        }
      }
    });
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    if (outboundChannel == null) {
      openOutboundAndWrite(e);
      return;
    }
    outboundChannel.write(e.getMessage());
  }

  @Override
  public void channelInterestChanged(ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception {
    if (outboundChannel != null) {
      outboundChannel.getPipeline().execute(new Runnable() {
        @Override
        public void run() {
          // If inboundChannel is not saturated anymore, continue accepting
          // the incoming traffic from the outboundChannel.
          if (e.getChannel().isWritable()) {
            LOG.trace("Setting outboundChannel readable.");
            outboundChannel.setReadable(true);
          } else {
            // If inboundChannel is saturated, do not read from outboundChannel
            LOG.trace("Setting outboundChannel non-readable.");
            outboundChannel.setReadable(false);
          }
        }
      });
    }
  }

  @Override
  public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    if (outboundChannel != null) {
      closeOnFlush(outboundChannel);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    LOG.error("Got exception", e.getCause());
    closeOnFlush(e.getChannel());
  }

  /**
   * Closes the specified channel after all queued write requests are flushed.
   */
  static void closeOnFlush(Channel ch) {
    if (ch.isConnected()) {
      ch.write(ChannelBuffers.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }
  }
}
