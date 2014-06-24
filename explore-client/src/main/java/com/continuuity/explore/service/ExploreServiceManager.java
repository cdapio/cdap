package com.continuuity.explore.service;

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
 * Service manager for explore service in distributed mode.
 */
public class ExploreServiceManager extends AbstractDistributedReactorServiceManager {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreServiceManager.class);
  private final DiscoveryServiceClient discoveryServiceClient;

  @Inject
  public ExploreServiceManager(CConfiguration cConf, TwillRunnerService twillRunnerService,
                              DiscoveryServiceClient discoveryServiceClient) {
    super(cConf, Constants.Service.EXPLORE_HTTP_USER_SERVICE, twillRunnerService);
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @Override
  public boolean isServiceEnabled() {
    return cConf.getBoolean(Constants.Explore.CFG_EXPLORE_ENABLED);
  }

  @Override
  public int getMaxInstances() {
    return cConf.getInt(Constants.Explore.MAX_INSTANCES, 1);
  }

  @Override
  public boolean isServiceAvailable() {
    try {
      //Look in discovery service only if explore option is enabled.
      if (isServiceEnabled()) {
        Iterable<Discoverable> discoverables = this.discoveryServiceClient.discover(serviceName);
        for (Discoverable discoverable : discoverables) {
          //Ping the discovered service to check its status.
          String url = String.format("http://%s:%d/ping", discoverable.getSocketAddress().getHostName(),
                                     discoverable.getSocketAddress().getPort());
          if (checkGetStatus(url).equals(HttpResponseStatus.OK)) {
            return true;
          }
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
