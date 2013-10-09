package com.continuuity.gateway.router.discovery;

import com.continuuity.app.program.Type;
import com.continuuity.weave.api.WeaveRunner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds the discovery service name for a given program when it is running in weave.
 */
public class WeaveDiscoveryNameFinder implements DiscoveryNameFinder {
  private static final Logger LOG = LoggerFactory.getLogger(WeaveDiscoveryNameFinder.class);

  private final Provider<Iterable<WeaveRunner.LiveInfo>> liveAppsProvider;

  @Inject
  public WeaveDiscoveryNameFinder(Provider<Iterable<WeaveRunner.LiveInfo>> liveAppsProvider) {
    this.liveAppsProvider = liveAppsProvider;
  }

  @Override
  public String findDiscoveryServiceName(Type type, String name) {
    String typeStr = type.name().toLowerCase() + ".";

    // Find the accountId and appId for the service name.
    for (WeaveRunner.LiveInfo liveInfo : liveAppsProvider.get()) {
      String appName = liveInfo.getApplicationName();
      LOG.debug("Got application name {}", appName);

      if (appName.endsWith("." + name) && appName.startsWith(typeStr)) {
        String [] splits = Iterables.toArray(Splitter.on('.').split(appName), String.class);
        if (splits.length > 3) {
          name = String.format("%s%s.%s.%s", typeStr, splits[1], splits[2], name);
          break;
        }
      }
    }

    return name;
  }
}
