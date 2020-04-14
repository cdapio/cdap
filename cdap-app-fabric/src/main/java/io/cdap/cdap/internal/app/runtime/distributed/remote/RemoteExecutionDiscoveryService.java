/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.utils.Networks;
import io.cdap.cdap.internal.app.runtime.distributed.runtimejob.DefaultRuntimeJob;
import io.cdap.cdap.internal.app.runtime.monitor.proxy.ServiceSocksProxy;
import io.cdap.cdap.master.spi.discovery.DefaultServiceDiscovered;
import io.cdap.cdap.proto.ProgramType;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Inject;

/**
 * A discovery service implementation used in remote runtime execution. It has a fixed list of services
 * that will go through {@link ServiceSocksProxy}, hence won't resolve the service name to an actual address.
 * It also use the {@link CConfiguration} as the backing store for service announcements, which is suitable for
 * the current remote runtime that has some "mini" system services running in the {@link DefaultRuntimeJob}.
 */
public class RemoteExecutionDiscoveryService implements DiscoveryServiceClient, DiscoveryService {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteExecutionDiscoveryService.class);

  private final CConfiguration cConf;
  private final Map<String, DefaultServiceDiscovered> services;
  private final Discoverable runtimeMonitorDiscoverable;

  @Inject
  RemoteExecutionDiscoveryService(CConfiguration cConf, RemoteMonitorType monitorType) {
    this.cConf = cConf;
    this.services = new ConcurrentHashMap<>();
    this.runtimeMonitorDiscoverable = createMonitorDiscoverable(cConf, monitorType);
  }

  @Override
  public synchronized Cancellable register(Discoverable discoverable) {
    String serviceName = discoverable.getName();
    DefaultServiceDiscovered serviceDiscovered = services.computeIfAbsent(serviceName, DefaultServiceDiscovered::new);

    // Add the address to cConf
    String key = Constants.RuntimeMonitor.DISCOVERY_SERVICE_PREFIX + serviceName;
    Networks.addDiscoverable(cConf, key, discoverable);
    LOG.debug("Discoverable {} with address {} added to configuration with payload {}",
              discoverable.getName(), discoverable.getSocketAddress(), Bytes.toString(discoverable.getPayload()));

    // Update the ServiceDiscovered
    updateServiceDiscovered(serviceDiscovered, cConf, key);

    return () -> {
      synchronized (RemoteExecutionDiscoveryService.this) {
        Networks.removeDiscoverable(cConf, key, discoverable);
        updateServiceDiscovered(serviceDiscovered, cConf, key);
      }
    };
  }

  @Override
  public ServiceDiscovered discover(String name) {
    // In Remote runtime, we don't support program discovery, hence always return an empty ServiceDiscovered
    for (ProgramType programType : ProgramType.values()) {
      if (programType.isDiscoverable() && name.startsWith(programType.getDiscoverableTypeName() + ".")) {
        return new DefaultServiceDiscovered(name);
      }
    }

    // Discovery for system services
    DefaultServiceDiscovered serviceDiscovered = services.get(name);
    String key = Constants.RuntimeMonitor.DISCOVERY_SERVICE_PREFIX + name;

    // Use a while loop to resolve races between discover and cancellable of register
    while (serviceDiscovered == null) {
      // Try to add the ServiceDiscovered object
      serviceDiscovered = new DefaultServiceDiscovered(name);
      synchronized (this) {
        updateServiceDiscovered(serviceDiscovered, cConf, key);
        services.putIfAbsent(name, serviceDiscovered);
      }

      serviceDiscovered = services.get(name);
    }

    return serviceDiscovered;
  }

  private void updateServiceDiscovered(DefaultServiceDiscovered serviceDiscovered, CConfiguration cConf, String key) {
    String name = serviceDiscovered.getName();
    Set<Discoverable> discoverables = Networks.getDiscoverables(cConf, key);

    if (discoverables.isEmpty()) {
      Discoverable discoverable;

      if (name.equals(runtimeMonitorDiscoverable.getName())) {
        discoverable = runtimeMonitorDiscoverable;
      } else {
        // If there is no address from the configuration, assuming it is using the service proxy,
        // hence the discovery name is the hostname. Also use port == 0 so that the RemoteExecutionProxySelector
        // knows that it need to use service proxy.
        URIScheme scheme = cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED) ? URIScheme.HTTPS : URIScheme.HTTP;
        discoverable = scheme.createDiscoverable(name, InetSocketAddress.createUnresolved(name, 0));
      }

      discoverables = Collections.singleton(discoverable);
    }

    if (LOG.isDebugEnabled()) {
      for (Discoverable d : discoverables) {
        LOG.debug("Update discoverable {} with address {} and payload {}",
                  d.getName(), d.getSocketAddress(), Bytes.toString(d.getPayload()));
      }
    }

    serviceDiscovered.setDiscoverables(discoverables);
  }

  /**
   * Creates a {@link Discoverable} for the runtime monitor service.
   */
  private static Discoverable createMonitorDiscoverable(CConfiguration cConf, RemoteMonitorType monitorType) {
    if (monitorType == RemoteMonitorType.URL) {
      // For monitor type URL, the monitor url is always set
      String url = cConf.get(Constants.RuntimeMonitor.MONITOR_URL);
      try {
        return URIScheme.createDiscoverable(Constants.Service.RUNTIME, new URL(url));
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException("Invalid monitor URL " + url, e);
      }
    }

    // If there is no runtime monitor URL, default to the service socks proxy mechanism, in which the
    // hostname is the service name with port == 0.
    URIScheme scheme = cConf.getBoolean(Constants.RuntimeMonitor.SSL_ENABLED) ? URIScheme.HTTPS : URIScheme.HTTP;
    return scheme.createDiscoverable(Constants.Service.RUNTIME,
                                     InetSocketAddress.createUnresolved(Constants.Service.RUNTIME, 0));
  }
}
