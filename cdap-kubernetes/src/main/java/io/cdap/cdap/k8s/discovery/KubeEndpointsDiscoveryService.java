/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.k8s.discovery;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.master.environment.k8s.ApiClientFactory;
import io.cdap.cdap.master.spi.discovery.DefaultServiceDiscovered;
import io.kubernetes.client.openapi.models.CoreV1EndpointPort;
import io.kubernetes.client.openapi.models.V1EndpointAddress;
import io.kubernetes.client.openapi.models.V1EndpointSubset;
import io.kubernetes.client.openapi.models.V1Endpoints;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;

/**
 * A {@link DiscoveryServiceClient} that discovers the address of each ready pod behind a CDAP
 * service, instead of the one ClusterIP address that {@link KubeDiscoveryService} discovers. It
 * watches the Kubernetes {@link V1Endpoints} of the services that carry the label
 * "cdap.service=[prefixed-service-name]". Kubernetes copies that label from each Service onto its
 * Endpoints. The pod's service account must be allowed to get, list and watch {@code endpoints} in
 * the namespace.
 */
public final class KubeEndpointsDiscoveryService implements DiscoveryServiceClient, AutoCloseable {

  private static final byte[] EMPTY_PAYLOAD = new byte[0];

  private final String namespace;
  private final String namePrefix;
  private final ApiClientFactory apiClientFactory;
  private final Map<String, DefaultServiceDiscovered> serviceDiscovereds;
  private volatile EndpointsWatcherThread watcherThread;
  private boolean closed;

  /**
   * Creates an instance. No thread starts and no API call is made until the first
   * {@link #discover(String)}.
   *
   * @param namespace the Kubernetes namespace to discover services in
   * @param namePrefix the prefix of every Kubernetes service name
   * @param apiClientFactory the factory of Kubernetes API clients
   */
  public KubeEndpointsDiscoveryService(String namespace, String namePrefix,
      ApiClientFactory apiClientFactory) {
    this.namespace = namespace;
    this.namePrefix = namePrefix;
    this.apiClientFactory = apiClientFactory;
    this.serviceDiscovereds = new ConcurrentHashMap<>();
  }

  @Override
  public ServiceDiscovered discover(String name) {
    ServiceDiscovered serviceDiscovered = serviceDiscovereds.computeIfAbsent(
        name, DefaultServiceDiscovered::new);

    // Start the watcher thread if it is not yet started
    EndpointsWatcherThread watcherThread = this.watcherThread;
    if (watcherThread == null) {
      synchronized (this) {
        if (closed) {
          throw new IllegalStateException("Discovery service is already closed");
        }

        watcherThread = this.watcherThread;
        if (watcherThread == null) {
          watcherThread = new EndpointsWatcherThread();
          watcherThread.addService(name);
          watcherThread.start();
          this.watcherThread = watcherThread;
          return serviceDiscovered;
        }
      }
    }

    // If the thread is already running, simply add the service name to watch for changes.
    watcherThread.addService(name);
    return serviceDiscovered;
  }

  @Override
  public void close() {
    EndpointsWatcherThread watcherThread;
    synchronized (this) {
      closed = true;
      watcherThread = this.watcherThread;
      this.watcherThread = null;
    }
    if (watcherThread != null) {
      watcherThread.close();
    }
  }

  /**
   * Returns the watcher, or {@code null} if no service has been discovered yet.
   */
  @VisibleForTesting
  @Nullable
  AbstractDiscoveryWatcherThread<V1Endpoints> getWatcherThread() {
    return watcherThread;
  }

  /**
   * Returns a {@link Discoverable} for each ready address and numbered port in the given
   * {@link V1Endpoints}.
   *
   * <p>The payload is the pod's node name in UTF-8, or empty if it is unknown. It is not a URI
   * scheme like the payload from {@link KubeDiscoveryService}: CDAP stores the scheme as a
   * {@code V1Service} annotation, and Kubernetes doesn't copy annotations onto Endpoints.
   *
   * <p>An Endpoints object without subsets yields an empty set. Kubernetes keeps the Endpoints
   * object when every pod goes away and only drops its subsets, so the empty set is the only
   * signal that there is nothing left to route to.
   */
  @VisibleForTesting
  static Set<Discoverable> toDiscoverables(String name, @Nullable V1Endpoints endpoints) {
    if (endpoints == null || endpoints.getSubsets() == null) {
      return Collections.emptySet();
    }

    Set<Discoverable> discoverables = new HashSet<>();
    for (V1EndpointSubset subset : endpoints.getSubsets()) {
      // Pods that aren't ready are in notReadyAddresses, which leaves addresses null.
      if (subset.getAddresses() == null || subset.getPorts() == null) {
        continue;
      }
      for (V1EndpointAddress address : subset.getAddresses()) {
        byte[] payload = address.getNodeName() == null
            ? EMPTY_PAYLOAD : address.getNodeName().getBytes(StandardCharsets.UTF_8);
        for (CoreV1EndpointPort port : subset.getPorts()) {
          if (port.getPort() != null) {
            discoverables.add(new Discoverable(name,
                InetSocketAddress.createUnresolved(address.getIp(), port.getPort()), payload));
          }
        }
      }
    }
    return discoverables;
  }

  /**
   * A {@link Thread} that keeps watching for changes in {@link V1Endpoints} in Kubernetes.
   */
  private final class EndpointsWatcherThread extends AbstractDiscoveryWatcherThread<V1Endpoints> {

    EndpointsWatcherThread() {
      super("kube-discovery-endpoints", namespace, "endpoints", apiClientFactory, namePrefix,
          serviceDiscovereds);
    }

    @Override
    protected Set<Discoverable> toDiscoverables(String name, V1Endpoints endpoints) {
      return KubeEndpointsDiscoveryService.toDiscoverables(name, endpoints);
    }
  }
}
