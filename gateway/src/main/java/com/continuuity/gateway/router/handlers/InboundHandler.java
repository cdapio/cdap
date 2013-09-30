package com.continuuity.gateway.router.handlers;

import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.gateway.router.HeaderDecoder;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Proxies incoming requests to a discoverable endpoint.
 */
public class InboundHandler extends SimpleChannelUpstreamHandler {
  private static final Logger LOG = LoggerFactory.getLogger(InboundHandler.class);

  private final ClientBootstrap clientBootstrap;
  private final Map<Integer, String> serviceMap;
  private final LoadingCache<CacheKey, EndpointStrategy> discoverableCache;

  private volatile Channel outboundChannel;

  public InboundHandler(ClientBootstrap clientBootstrap, final DiscoveryServiceClient discoveryServiceClient,
                        final Map<Integer, String> serviceMap) {
    this.clientBootstrap = clientBootstrap;
    this.serviceMap = serviceMap;

    this.discoverableCache = CacheBuilder.newBuilder().build(new CacheLoader<CacheKey, EndpointStrategy>() {
      @Override
      public EndpointStrategy load(CacheKey key) throws Exception {
        String service = serviceMap.get(key.getPort());
        if (service == null) {
          return null;
        }

        service = service.replace("$HOST", key.getHost());
        return new RandomEndpointStrategy(discoveryServiceClient.discover(service));
      }
    });
  }

  private void openOutboundAndWrite(MessageEvent e) throws Exception {
    final ChannelBuffer msg = (ChannelBuffer) e.getMessage();
    msg.markReaderIndex();

    // Suspend incoming traffic until connected to the outbound service.
    final Channel inboundChannel = e.getChannel();
    inboundChannel.setReadable(false);

    // Discover endpoint.
    int inboundPort = ((InetSocketAddress) inboundChannel.getLocalAddress()).getPort();
    Discoverable discoverable = discover(inboundPort, msg);
    if (discoverable == null) {
      LOG.error("No discoverable endpoints found for service {}", serviceMap.get(inboundPort));
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

          // Connection attempt succeeded.

          // Write the message to outBoundChannel.
          msg.resetReaderIndex();
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

    ChannelBuffer msg = (ChannelBuffer) e.getMessage();
    outboundChannel.write(msg);
  }

  @Override
  public void channelInterestChanged(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    if (outboundChannel != null) {
      // If inboundChannel is not saturated anymore, continue accepting
      // the incoming traffic from the outboundChannel.
      if (e.getChannel().isWritable() && !outboundChannel.isReadable()) {
        outboundChannel.setReadable(true);
      }

      // If inboundChannel is saturated, do not read from outboundChannel
      if (!e.getChannel().isWritable() && outboundChannel.isReadable()) {
        outboundChannel.setReadable(false);
      }
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

  private Discoverable discover(int inboundPort, ChannelBuffer msg) throws Exception {
    // If the forward rule has $HOST in it, get Host header
    String service = serviceMap.get(inboundPort);
    String host = "";
    if (service != null && service.contains("$HOST")) {
      msg.resetReaderIndex();
      host = HeaderDecoder.decodeHeader(msg, "Host");
      if (host == null) {
        LOG.trace("Cannot find host header for service {} on port {}", service, inboundPort);
        return null;
      }
    }

    host = normalizeHost(host);
    EndpointStrategy endpointStrategy = discoverableCache.get(new CacheKey(inboundPort, host));
    if (endpointStrategy == null) {
      LOG.error("Cannot find forward rule for port {} and host {}", inboundPort, host);
      return null;
    }

    return endpointStrategy.pick();
  }

  /**
   * Removes "www." from beginning and ":80" from end of the host.
   * @param host host that needs to be normalized.
   * @return the shortened host.
   */
  static String normalizeHost(String host) {
    if (host.startsWith("www.")) {
      host = host.substring(4);
    }

    if (host.endsWith(":80")) {
      host = host.substring(0, host.length() - 3);
    }

    return host;
  }

  /**
   * Key to Discoverable cache.
   */
  private static final class CacheKey {
    private final int port;
    private final String host;

    private CacheKey(int port, String host) {
      this.port = port;
      this.host = host;
    }

    public int getPort() {
      return port;
    }

    public String getHost() {
      return host;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      CacheKey cacheKey = (CacheKey) o;

      return port == cacheKey.port && !(host != null ? !host.equals(cacheKey.host) : cacheKey.host != null);

    }

    @Override
    public int hashCode() {
      int result = port;
      result = 31 * result + (host != null ? host.hashCode() : 0);
      return result;
    }
  }
}
