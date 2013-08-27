package com.continuuity.gateway.collector;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.gateway.Collector;
import com.continuuity.gateway.DataAccessor;
import com.continuuity.gateway.util.HttpConfig;
import com.continuuity.gateway.util.NettyHttpPipelineFactory;
import com.continuuity.gateway.util.NettyRequestHandlerFactory;
import com.continuuity.weave.filesystem.LocationFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 * This collector that provides a RESTful interface to push events to an event
 * stream. It uses Netty to start up the service, and the DataRestHandler to
 * implement the handling of a request.
 */
public class RestCollector extends Collector
  implements DataAccessor, NettyRequestHandlerFactory {

  private static final Logger LOG = LoggerFactory
    .getLogger(RestCollector.class);

  /**
   * the data fabric executor to use for all data access.
   */
  protected OperationExecutor executor;

  /**
   * the data fabric location factory to use for data access.
   */
  protected LocationFactory locationFactory;

  // To support early integration of TxDs2
  private DataSetAccessor dataSetAccessor;

  // to support early integration with TxDs2
  private TransactionSystemClient txSystemClient;

  @Override
  public void setExecutor(OperationExecutor executor) {
    this.executor = executor;
  }

  @Override
  public OperationExecutor getExecutor() {
    return this.executor;
  }

  @Override
  public void setLocationFactory(LocationFactory locationFactory) {
    this.locationFactory = locationFactory;
  }

  @Override
  public LocationFactory getLocationFactory() {
    return this.locationFactory;
  }

  @Override
  public DataSetAccessor getDataSetAccessor() {
    return dataSetAccessor;
  }

  @Override
  public void setDataSetAccessor(DataSetAccessor dataSetAccessor) {
    this.dataSetAccessor = dataSetAccessor;
  }

  @Override
  public TransactionSystemClient getTxSystemClient() {
    return txSystemClient;
  }

  @Override
  public void setTxSystemClient(TransactionSystemClient txSystemClient) {
    this.txSystemClient = txSystemClient;
  }

  /**
   * this will provide defaults for the HTTP service, such as port and paths.
   */
  private static final HttpConfig defaultConfig =
    new HttpConfig("stream.rest")
      .setPort(10000)
      .setPathMiddle("/stream/");

  /**
   * this will provide the actual HTTP configuration, backed by the default.
   */
  private HttpConfig httpConfig = defaultConfig;

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
    this.httpConfig =
      HttpConfig.configure(this.getName(), configuration, defaultConfig);
  }

  @Override
  public SimpleChannelUpstreamHandler newHandler() {
    return new RestHandler(this);
  }

  @Override
  public void start() throws Exception {
    LOG.info("Starting up " + this);
    // construct the internet address
    InetSocketAddress address = null;
    try {
      address = new InetSocketAddress(this.httpConfig.getPort());
    } catch (Exception e) {
      e.printStackTrace();
    }
    try {
      // create a server bootstrap
      ServerBootstrap bootstrap = new ServerBootstrap(
        new NioServerSocketChannelFactory(
          Executors.newCachedThreadPool(),
          Executors.newCachedThreadPool(),
          this.httpConfig.getThreads()));
      // and use a pipeline factory that uses this to cnfigure itself and to
      // create a request handler for each client request.
      bootstrap.setPipelineFactory(
        new NettyHttpPipelineFactory(this.httpConfig, this));
      // bind to the address = start the service
      this.serverChannel = bootstrap.bind(address);
      // server is now running
    } catch (Exception e) {
      LOG.error("Failed to startup collector '" + this.getName()
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
