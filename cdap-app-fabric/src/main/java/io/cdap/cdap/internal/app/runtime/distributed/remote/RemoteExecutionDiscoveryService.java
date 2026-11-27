/*
 * Copyright © 2019-2021 Cask Data, Inc.
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

import com.google.common.base.Strings;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.utils.Networks;
import io.cdap.cdap.internal.app.runtime.distributed.runtimejob.DefaultRuntimeJob;
import io.cdap.cdap.master.spi.discovery.DefaultServiceDiscovered;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import javax.inject.Inject;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A discovery service implementation used in remote runtime execution. It uses {@link
 * CConfiguration} as the backing store for service announcements, which is suitable for the
 * current remote runtime that has some "mini" system services running in the {@link
 * DefaultRuntimeJob}.
 */
public class RemoteExecutionDiscoveryService implements DiscoveryServiceClient, DiscoveryService {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteExecutionDiscoveryService.class);

  private final CConfiguration cConf;
  private final Map<String, DefaultServiceDiscovered> services;
  @Nullable
  private final Discoverable runtimeMonitorDiscoverable;

  @Inject
  RemoteExecutionDiscoveryService(CConfiguration cConf) {
    this.cConf = cConf;
    this.services = new ConcurrentHashMap<>();
    this.runtimeMonitorDiscoverable = createMonitorDiscoverable(cConf);
  }

  @Override
  public synchronized Cancellable register(Discoverable discoverable) {
    String serviceName = discoverable.getName();
    DefaultServiceDiscovered serviceDiscovered = services.computeIfAbsent(serviceName,
        DefaultServiceDiscovered::new);

    // Add the address to cConf
    String key = Constants.RuntimeMonitor.DISCOVERY_SERVICE_PREFIX + serviceName;
    Networks.addDiscoverable(cConf, key, discoverable);
    LOG.debug("Discoverable {} with address {} added to configuration with payload {}",
        discoverable.getName(), discoverable.getSocketAddress(),
        Bytes.toString(discoverable.getPayload()));

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
    // In Remote runtime, we don't support program discovery except for the SYSTEM namespace.
    for (ProgramType programType : ProgramType.values()) {
      if (programType.isDiscoverable()
          && name.startsWith(programType.getDiscoverableTypeName() + ".")
          && !name.startsWith(
          programType.getDiscoverableTypeName() + "." + NamespaceId.SYSTEM.getNamespace() + ".")) {
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

  private void updateServiceDiscovered(DefaultServiceDiscovered serviceDiscovered,
      CConfiguration cConf, String key) {
    String name = serviceDiscovered.getName();
    Set<Discoverable> discoverables = Networks.getDiscoverables(cConf, key);

    if (discoverables.isEmpty()) {
      Discoverable discoverable;

      if (Constants.Service.RUNTIME.equals(name)) {
        if (runtimeMonitorDiscoverable == null) {
          throw new IllegalStateException(
              "Missing configuration " + Constants.RuntimeMonitor.MONITOR_URL);
        }
        discoverable = runtimeMonitorDiscoverable;
      } else {
        // If there is no address from the configuration, return a placeholder discoverable with
        // port == 0 so RemoteClient can rewrite the service URL via the runtime routing base URI.
        URIScheme scheme =
            cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED) ? URIScheme.HTTPS
                : URIScheme.HTTP;
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
   * Creates a {@link Discoverable} for the runtime monitor service, or {@code null} if the monitor
   * URL is not configured.
   */
  @Nullable
  private static Discoverable createMonitorDiscoverable(CConfiguration cConf) {
    String url = cConf.getTrimmed(Constants.RuntimeMonitor.MONITOR_URL);
    if (Strings.isNullOrEmpty(url)) {
      return null;
    }
    try {
      return URIScheme.createDiscoverable(Constants.Service.RUNTIME, new URL(url));
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Invalid monitor URL " + url, e);
    }
  }
}
