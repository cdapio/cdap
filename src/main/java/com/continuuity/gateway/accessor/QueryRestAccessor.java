package com.continuuity.gateway.accessor;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.gateway.Accessor;
import com.continuuity.gateway.util.HttpConfig;
import com.continuuity.gateway.util.NettyHttpPipelineFactory;
import com.continuuity.gateway.util.NettyRequestHandlerFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 * Accessor for making calls to procedure.
 */
public class QueryRestAccessor
  extends Accessor implements NettyRequestHandlerFactory {

  private static final Logger LOG = LoggerFactory
    .getLogger(QueryRestAccessor.class);

  /**
   * this will provide defaults for the HTTP service, such as port and paths.
   */
  private static final HttpConfig defaultHttpConfig =
    new HttpConfig("procedure.rest")
      .setPort(10010)
      .setPathMiddle("/procedure/");

  /**
   * this will provide the actual HTTP configuration, backed by the default.
   */
  private HttpConfig httpConfig = defaultHttpConfig;

  /**
   * return the HTTP configuration for this accessor.
   *
   * @return the HTTP configuration
   */
  public HttpConfig getHttpConfig() {
    return this.httpConfig;
  }

  /**
   * this is the active Netty server channel.
   */
  private Channel serverChannel;

  @Override
  public void configure(CConfiguration configuration) throws Exception {
    super.configure(configuration);
    this.httpConfig = HttpConfig.configure(
      this.getName(), configuration, defaultHttpConfig);
  }

  @Override
  public SimpleChannelUpstreamHandler newHandler() {
    return new QueryRestHandler(this);
  }

  @Override
  public void start() throws Exception {
    LOG.info("Starting up " + this);
    // construct the internet address
    InetSocketAddress address =
      new InetSocketAddress(this.httpConfig.getPort());
    try {
      // create a server bootstrap
      ServerBootstrap bootstrap = new ServerBootstrap(
        new NioServerSocketChannelFactory(
          Executors.newCachedThreadPool(),
          Executors.newCachedThreadPool(),
          this.httpConfig.getThreads()));
      // and use a pipeline factory that uses this to configure itself
      // and to create a request handler for each client request.
      bootstrap.setPipelineFactory(
        new NettyHttpPipelineFactory(this.httpConfig, this));
      // bind to the address = start the service
      this.serverChannel = bootstrap.bind(address);
      // server is now running
    } catch (Exception e) {
      LOG.error("Failed to startup accessor '" + this.getName()
                  + "' at " + this.httpConfig.getBaseUrl() + ".");
      throw e;
    }
    LOG.info("Connector " + this.getName() + " now running" +
               " at " + this.httpConfig.getBaseUrl() +
               " with " + this.httpConfig.getThreads() + " threads.");
  }

  @Override
  public void stop() {
    LOG.info("Stopping " + this);
    // closing the channel stops the service
    if (this.serverChannel != null) {
      this.serverChannel.close();
    }
    LOG.info("Stopped " + this);
  }
}
