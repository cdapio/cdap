package com.continuuity.data2.transaction.distributed;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An abstract tx client provider that implements common functionality.
 */
public abstract class AbstractClientProvider implements ThriftClientProvider {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractClientProvider.class);

  // Discovery service. If null, no service discovery.
  private final DiscoveryServiceClient discoveryServiceClient;
  protected final AtomicBoolean initialized = new AtomicBoolean(false);

  // the configuration
  final CConfiguration configuration;

  // the endpoint strategy for service discovery.
  EndpointStrategy endpointStrategy;

  protected AbstractClientProvider(CConfiguration configuration, DiscoveryServiceClient discoveryServiceClient) {
    this.configuration = configuration;
    this.discoveryServiceClient = discoveryServiceClient;
  }

  public void initialize() throws TException {
    // initialize the service discovery client
    this.initDiscovery();
  }

  /**
   * Initialize the service discovery client, we will reuse that
   * every time we need to create a new client.
   */
  private void initDiscovery() {
    if (discoveryServiceClient == null) {
      LOG.info("No DiscoveryServiceClient provided. Skipping service discovery.");
      return;
    }

    endpointStrategy = new TimeLimitEndpointStrategy(
      new RandomEndpointStrategy(discoveryServiceClient.discover(Constants.Service.TRANSACTION)),
      2, TimeUnit.SECONDS);
  }

  protected TransactionServiceThriftClient newClient() throws TException {
    return newClient(-1);
  }

  protected TransactionServiceThriftClient newClient(int timeout) throws TException {
    if (initialized.compareAndSet(false, true)) {
      initialize();
    }
    String address;
    int port;

    if (endpointStrategy == null) {
      // if there is no discovery service, try to read host and port directly
      // from the configuration
      LOG.info("Reading address and port from configuration.");
      address = configuration.get(Constants.Transaction.Service.CFG_DATA_TX_BIND_ADDRESS,
                                  Constants.Transaction.Service.DEFAULT_DATA_TX_BIND_ADDRESS);
      port = configuration.getInt(Constants.Transaction.Service.CFG_DATA_TX_BIND_PORT,
                                  Constants.Transaction.Service.DEFAULT_DATA_TX_BIND_PORT);
      LOG.info("Service assumed at " + address + ":" + port);
    } else {
      Discoverable endpoint = endpointStrategy.pick();
      if (endpoint == null) {
        LOG.error("Unable to discover tx service.");
        throw new TException("Unable to discover tx service.");
      }
      address = endpoint.getSocketAddress().getHostName();
      port = endpoint.getSocketAddress().getPort();
      LOG.info("Service discovered at " + address + ":" + port);
    }

    // now we have an address and port, try to connect a client
    if (timeout < 0) {
      timeout = configuration.getInt(Constants.Transaction.Service.CFG_DATA_TX_CLIENT_TIMEOUT,
          Constants.Transaction.Service.DEFAULT_DATA_TX_CLIENT_TIMEOUT);
    }
    LOG.info("Attempting to connect to tx service at " +
               address + ":" + port + " with timeout " + timeout + " ms.");
    // thrift transport layer
    TTransport transport =
        new TFramedTransport(new TSocket(address, port, timeout));
    try {
      transport.open();
    } catch (TTransportException e) {
      LOG.error("Unable to connect to tx service: " + e.getMessage());
      throw e;
    }
    // and create a thrift client
    TransactionServiceThriftClient newClient = new TransactionServiceThriftClient(transport);

    LOG.info("Connected to tx service at " +
               address + ":" + port);
    return newClient;
  }
}
