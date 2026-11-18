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

import io.cdap.cdap.k8s.common.AbstractWatcherThread;
import io.cdap.cdap.master.environment.k8s.ApiClientFactory;
import io.cdap.cdap.master.spi.discovery.DefaultServiceDiscovered;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.util.generic.options.ListOptions;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.twill.discovery.Discoverable;

/**
 * A watcher of core v1 Kubernetes resources that carry the {@code cdap.service} label. It keeps
 * the {@link DefaultServiceDiscovered} of each discovered CDAP service up to date. Subclasses only
 * decide how a resource maps to {@link Discoverable}s.
 *
 * @param <T> the type of the watched resource
 */
abstract class AbstractDiscoveryWatcherThread<T extends KubernetesObject>
    extends AbstractWatcherThread<T> {

  private final String namePrefix;
  private final Map<String, DefaultServiceDiscovered> serviceDiscovereds;
  private final Set<String> services;

  /**
   * Creates a watcher.
   *
   * @param threadName the name of the watcher thread
   * @param namespace the Kubernetes namespace to watch
   * @param plural the plural name of the core v1 resource to watch, such as {@code services}
   * @param apiClientFactory the factory of Kubernetes API clients
   * @param namePrefix the prefix of every Kubernetes service name
   * @param serviceDiscovereds the discovered services to update, keyed by CDAP service name
   */
  AbstractDiscoveryWatcherThread(String threadName, String namespace, String plural,
      ApiClientFactory apiClientFactory, String namePrefix,
      Map<String, DefaultServiceDiscovered> serviceDiscovereds) {
    super(threadName, namespace, "", "v1", plural, apiClientFactory);
    this.namePrefix = namePrefix;
    this.serviceDiscovereds = serviceDiscovereds;
    this.services = Collections.newSetFromMap(new ConcurrentHashMap<>());
  }

  /**
   * Returns the {@link Discoverable}s for the given resource of the given CDAP service.
   */
  protected abstract Set<Discoverable> toDiscoverables(String name, T resource);

  /**
   * Starts watching the resources of the given CDAP service.
   */
  final void addService(String name) {
    // Service names in K8s are prefixed.
    // If this is a new service to watch, reset the watch so that the new selector gets picked up.
    if (services.add(namePrefix + name)) {
      closeWatch();
    }
  }

  @Override
  protected final void updateListOptions(ListOptions options) {
    options.setLabelSelector(String.format("%s in (%s)", KubeDiscoveryService.SERVICE_LABEL,
        String.join(",", services)));
  }

  @Override
  public final void resourceAdded(T resource) {
    getServiceDiscovered(resource)
        .ifPresent(s -> s.setDiscoverables(toDiscoverables(s.getName(), resource)));
  }

  @Override
  public final void resourceModified(T resource) {
    // Treat modify the same as add, since both carry the complete resource.
    resourceAdded(resource);
  }

  @Override
  public final void resourceDeleted(T resource) {
    getServiceDiscovered(resource).ifPresent(s -> s.setDiscoverables(Collections.emptySet()));
  }

  private Optional<DefaultServiceDiscovered> getServiceDiscovered(T resource) {
    V1ObjectMeta metadata = resource.getMetadata();
    if (metadata == null || metadata.getLabels() == null) {
      return Optional.empty();
    }
    String serviceName = metadata.getLabels().get(KubeDiscoveryService.SERVICE_LABEL);
    if (serviceName == null || !serviceName.startsWith(namePrefix)) {
      return Optional.empty();
    }
    // Remove the name prefix to get the original CDAP service name
    return Optional.ofNullable(serviceDiscovereds.get(serviceName.substring(namePrefix.length())));
  }
}
