package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.twill.AbstractDistributedReactorServiceManager;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.google.inject.Inject;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Transaction Reactor Service Management in Distributed Mode.
 */
public class TransactionServiceManager extends AbstractDistributedReactorServiceManager {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionServiceManager.class);
  private TransactionSystemClient txClient;
  private DiscoveryServiceClient discoveryServiceClient;

  @Inject
  public TransactionServiceManager(CConfiguration cConf, TwillRunnerService twillRunnerService,
                                   TransactionSystemClient txClient, DiscoveryServiceClient discoveryServiceClient) {
    super(cConf, Constants.Service.TRANSACTION, twillRunnerService, discoveryServiceClient);
    this.txClient = txClient;
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @Override
  public int getMaxInstances() {
    return cConf.getInt(Constants.Transaction.Container.MAX_INSTANCES);
  }

  @Override
  public boolean isServiceAvailable() {
    try {
      Iterable<Discoverable> discoverables = this.discoveryServiceClient.discover(serviceName);
      EndpointStrategy endpointStrategy = new TimeLimitEndpointStrategy(
        new RandomEndpointStrategy(discoverables), discoveryTimeout, TimeUnit.SECONDS);
      Discoverable discoverable = endpointStrategy.pick();
      if (discoverable == null) {
        return false;
      }

      return txClient.status().equals(Constants.Monitor.STATUS_OK);
    } catch (IllegalArgumentException e) {
      return false;
    } catch (Exception e) {
      LOG.warn("Unable to ping {} : Reason {} ", serviceName, e.getMessage());
      return false;
    }
  }

  @Override
  public String getDescription() {
    return Constants.Transaction.SERVICE_DESCRIPTION;
  }
}
