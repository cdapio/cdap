package com.continuuity.gateway.router.handlers;

import com.continuuity.gateway.router.HeaderInfo;
import com.continuuity.gateway.router.RouterServiceLookup;
import com.continuuity.security.auth.Validator;
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
  private Validator tokenValidator;
  private boolean securityEnabled;

  public InboundHandler(ClientBootstrap clientBootstrap, final RouterServiceLookup serviceLookup,
                        Validator tokenValidator, boolean securityEnabled) {
    this.clientBootstrap = clientBootstrap;
    this.serviceLookup = serviceLookup;
    this.tokenValidator = tokenValidator;
    this.securityEnabled = securityEnabled;
  }


  private void openOutboundAndWrite(MessageEvent e) throws Exception {
    final HttpRequest msg = (HttpRequest) e.getMessage();

    // Suspend incoming traffic until connected to the outbound service.
    final Channel inboundChannel = e.getChannel();
    inboundChannel.setReadable(false);

    String auth = msg.getHeader(HttpHeaders.Names.AUTHORIZATION);
    String path = msg.getUri();
    String host = msg.getHeader(HttpHeaders.Names.HOST);
    String accessToken = null;

    if (auth != null) {
      String[] fragments = auth.split("\\s+");
      if (fragments.length > 1) {
        accessToken = fragments[1];
      }
    }
    //Decoding the header
    final HeaderInfo headerInfo = new HeaderInfo(path, host, accessToken);

    if (securityEnabled) {
      Validator.State tokenState = tokenValidator.validate(accessToken);
      HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.UNAUTHORIZED);
      switch (tokenState) {
        case TOKEN_MISSING:
          httpResponse.addHeader(HttpHeaders.Names.WWW_AUTHENTICATE, "Bearer realm = \"continuuity\"");
          httpResponse.setHeader(HttpHeaders.Names.CONTENT_LENGTH, 0);
          break;

        case TOKEN_INVALID:
          httpResponse.addHeader(HttpHeaders.Names.WWW_AUTHENTICATE, "Bearer realm=\"continuuity\"" +
            "  error=\"invalid_token\"" +
            "  error_description=\"The access token expired\"");
          httpResponse.setHeader(HttpHeaders.Names.CONTENT_LENGTH, 0);
          break;
      }
      if (tokenState != Validator.State.TOKEN_VALID) {
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
