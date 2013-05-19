package com.continuuity.gateway.accessor;

import com.continuuity.app.store.Store;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.gateway.Accessor;
import com.continuuity.gateway.MetaDataServiceAware;
import com.continuuity.gateway.MetaDataStoreAware;
import com.continuuity.gateway.StoreAware;
import com.continuuity.gateway.util.HttpConfig;
import com.continuuity.gateway.util.NettyHttpPipelineFactory;
import com.continuuity.gateway.util.NettyRequestHandlerFactory;
import com.continuuity.metadata.MetadataService;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 * Accessor for retrieving system stats.
 */
public class SystemStatsRestAccessor extends Accessor
  implements NettyRequestHandlerFactory, MetaDataServiceAware, MetaDataStoreAware, StoreAware {

  private static final Logger LOG = LoggerFactory
    .getLogger(SystemStatsRestAccessor.class);

  public static final String SYSTEM_STATS_SERVICE_NAME = "systemstats";

  /**
   * this will provide defaults for the HTTP service, such as port and paths.
   */
  private static final HttpConfig defaultHttpConfig =
    new HttpConfig(SYSTEM_STATS_SERVICE_NAME + ".rest")
      .setPort(10006)
      .setPathMiddle("/" + SYSTEM_STATS_SERVICE_NAME + "/");

  private MetaDataStore metaDataStore;
  private Store store;
  private MetadataService metadataService;

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
    return new SystemStatsRestHandler(this);
  }

  @Override
  public void start() throws Exception {
    LOG.info("Starting up " + this);
    // construct the internet address
    // NOTE: we want to bind to localhost so that it is not accessible from the outside.
    InetSocketAddress address =
      new InetSocketAddress("127.0.0.1", this.httpConfig.getPort());
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

  @Override
  public void setMetadataService(MetadataService mds) {
    this.metadataService = mds;
  }

  public MetadataService getMetadataService() {
    return metadataService;
  }

  @Override
  public void setMetadataStore(MetaDataStore metaDataStore) {
    this.metaDataStore = metaDataStore;
  }

  public MetaDataStore getMetaDataStore() {
    return metaDataStore;
  }

  @Override
  public void setStore(Store store) {
    this.store = store;
  }

  public Store getStore() {
    return store;
  }
}
