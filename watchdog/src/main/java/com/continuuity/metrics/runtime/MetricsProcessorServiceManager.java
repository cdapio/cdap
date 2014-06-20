package com.continuuity.metrics.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbstractDistributedReactorServiceManager;
import com.google.inject.Inject;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metrics Processor Reactor Service Management in Distributed Mode.
 */
public class MetricsProcessorServiceManager extends AbstractDistributedReactorServiceManager {
  private final DiscoveryServiceClient discoveryServiceClient;
  private static final Logger LOG = LoggerFactory.getLogger(MetricsProcessorServiceManager.class);

  @Inject
  public MetricsProcessorServiceManager(CConfiguration cConf, TwillRunnerService twillRunnerService,
                                        DiscoveryServiceClient discoveryServiceClient) {
    super(cConf, Constants.Service.METRICS_PROCESSOR, twillRunnerService);
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @Override
  public int getMaxInstances() {
    return cConf.getInt(Constants.MetricsProcessor.MAX_INSTANCES);
  }

  public boolean isServiceAvailable() {
    try {
      Iterable<Discoverable> discoverables = this.discoveryServiceClient.discover(serviceName);
      for (Discoverable discoverable : discoverables) {
        //Ping the discovered service to check its status.
        String url = String.format("http://%s:%d/ping", discoverable.getSocketAddress().getHostName(),
                                   discoverable.getSocketAddress().getPort());
        if (checkGetStatus(url).equals(HttpResponseStatus.OK)) {
          return true;
        }
      }
      return false;
    } catch (IllegalArgumentException e) {
      return false;
    } catch (Exception e) {
      LOG.warn("Unable to ping {} : Reason : {}", serviceName, e.getMessage());
      return false;
    }
  }
}
