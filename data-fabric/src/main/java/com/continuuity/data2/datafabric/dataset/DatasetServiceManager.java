package com.continuuity.data2.datafabric.dataset;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.twill.AbstractDistributedReactorServiceManager;
import com.google.inject.Inject;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.TimeUnit;

/**
 * Dataset Reactor Service management in distributed mode.
 */
public class DatasetServiceManager extends AbstractDistributedReactorServiceManager {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetServiceManager.class);
  private final DiscoveryServiceClient discoveryServiceClient;

  @Inject
  public DatasetServiceManager(CConfiguration cConf, TwillRunnerService twillRunnerService,
                               DiscoveryServiceClient discoveryServiceClient) {
    super(cConf, Constants.Service.DATASET_EXECUTOR, twillRunnerService);
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @Override
  public int getMaxInstances() {
    return cConf.getInt(Constants.Dataset.Executor.MAX_INSTANCES);
  }

  @Override
  public boolean isServiceAvailable() {
    try {
      Iterable<Discoverable> discoverables = this.discoveryServiceClient.discover(serviceName);
      EndpointStrategy endpointStrategy = new TimeLimitEndpointStrategy(new RandomEndpointStrategy(discoverables),
                                                                        discoveryTimeout, TimeUnit.SECONDS);
      Discoverable discoverable = endpointStrategy.pick();
      if (discoverable == null) {
        return false;
      }

      //Ping the discovered service to check its status.
      String url = String.format("http://%s:%d/ping", discoverable.getSocketAddress().getHostName(),
                                 discoverable.getSocketAddress().getPort());
      return checkGetStatus(url).equals(HttpResponseStatus.OK);
    } catch (IllegalArgumentException e) {
      return false;
    } catch (Exception e) {
      LOG.warn("Unable to ping {} : Reason : {}", serviceName, e.getMessage());
      return false;
    }
  }
}
