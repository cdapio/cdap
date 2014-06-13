package com.continuuity.data2.transaction.distributed;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.transaction.TxConstants;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Random;
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
      address = configuration.get(TxConstants.Service.CFG_DATA_TX_BIND_ADDRESS,
                                  TxConstants.Service.DEFAULT_DATA_TX_BIND_ADDRESS);
      port = configuration.getInt(TxConstants.Service.CFG_DATA_TX_BIND_PORT,
                                  TxConstants.Service.DEFAULT_DATA_TX_BIND_PORT);
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
      timeout = configuration.getInt(TxConstants.Service.CFG_DATA_TX_CLIENT_TIMEOUT,
          TxConstants.Service.DEFAULT_DATA_TX_CLIENT_TIMEOUT);
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

  /**
   * This class helps picking up an endpoint from a list of Discoverable.
   */
  public interface EndpointStrategy {

    /**
     * Picks a {@link Discoverable} using its strategy.
     * @return A {@link Discoverable} based on the stragegy or {@code null} if no endpoint can be found.
     */
    Discoverable pick();
  }

  /**
   * An {@link EndpointStrategy} that make sure it picks an endpoint within the given
   * timeout limit.
   */
  public final class TimeLimitEndpointStrategy implements EndpointStrategy {

    private final EndpointStrategy delegate;
    private final long timeout;
    private final TimeUnit timeoutUnit;

    public TimeLimitEndpointStrategy(EndpointStrategy delegate, long timeout, TimeUnit timeoutUnit) {
      this.delegate = delegate;
      this.timeout = timeout;
      this.timeoutUnit = timeoutUnit;
    }

    @Override
    public Discoverable pick() {
      Discoverable pick = delegate.pick();
      try {
        long count = 0;
        while (pick == null && count++ < timeout) {
          timeoutUnit.sleep(1);
          pick = delegate.pick();
        }
      } catch (InterruptedException e) {
        // Simply propagate the interrupt.
        Thread.currentThread().interrupt();
      }
      return pick;
    }
  }

  /**
   * Randomly picks endpoint from the list of available endpoints.
   */
  public final class RandomEndpointStrategy implements EndpointStrategy {

    private final Iterable<Discoverable> endpoints;

    /**
     * Constructs a random endpoint strategy.
     * @param endpoints Endpoints for the strategy to use. Note that this strategy will
     *                  invoke {@link Iterable#iterator()} and traverse through it on
     *                  every call to the {@link #pick()} method. One could leverage this
     *                  behavior with the live {@link Iterable} as provided by
     *                  {@link org.apache.twill.discovery.DiscoveryServiceClient#discover(String)} method.
     */
    public RandomEndpointStrategy(Iterable<Discoverable> endpoints) {
      this.endpoints = endpoints;
    }

    @Override
    public Discoverable pick() {
      // Reservoir sampling
      Discoverable result = null;
      Iterator<Discoverable> itor = endpoints.iterator();
      Random random = new Random();
      int count = 0;
      while (itor.hasNext()) {
        Discoverable next = itor.next();
        if (random.nextInt(++count) == 0) {
          result = next;
        }
      }
      return result;
    }
  }
}

