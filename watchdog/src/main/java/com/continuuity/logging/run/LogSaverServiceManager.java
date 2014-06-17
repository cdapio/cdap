package com.continuuity.logging.run;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Log Saver Reactor Service Management in Distributed Mode.
 */
public class LogSaverServiceManager extends AbstractDistributedReactorServiceManager {
  private DiscoveryServiceClient discoveryServiceClient;
  private static final Logger LOG = LoggerFactory.getLogger(LogSaverServiceManager.class);

  @Inject
  public LogSaverServiceManager(CConfiguration cConf, TwillRunnerService twillRunnerService,
                                DiscoveryServiceClient discoveryServiceClient) {
    super(cConf, Constants.Service.LOGSAVER, twillRunnerService);
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @Override
  public int getMaxInstances() {
    return cConf.getInt(Constants.LogSaver.MAX_INSTANCES);
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
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    } catch (Exception e) {
      LOG.warn("Unable to discover {} : Reason : {}", serviceName, e.getMessage());
      return false;
    }
  }

  @Override
  public boolean isLogAvailable() {
    return false;
  }
}
