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

package io.cdap.cdap.k8s.discovery;

import io.cdap.cdap.k8s.common.AbstractWatcherThread;
import io.cdap.cdap.master.environment.k8s.ApiClientFactory;
import io.cdap.cdap.master.spi.discovery.DefaultServiceDiscovered;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceBuilder;
import io.kubernetes.client.openapi.models.V1ServiceList;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import io.kubernetes.client.util.generic.options.ListOptions;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link DiscoveryService} and {@link DiscoveryServiceClient} that uses
 * Kubernetes API to announce and discover service locations. This service assumes kubernetes
 * services are created with label, "cdap.service=[service-name]", where [service-name] is the CDAP
 * service name. On registering service via the {@link #register(Discoverable)} method, a new k8s
 * Service with name "cdap-[transformed-service-name]" will be created with label
 * "cdap.service=[service-name]". The [transformed-service-name] is the CDAP service name with "."
 * replaced with "-" to conform to the naming requirement in K8s. The service selector will be set
 * to include the current pod labels.
 */
public class KubeDiscoveryService implements DiscoveryService, DiscoveryServiceClient,
    AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(KubeDiscoveryService.class);

  private static final String SERVICE_LABEL = "cdap.service";
  private static final String PAYLOAD_NAME = "cdap.service.payload";
  static final String USE_LOAD_BALANCER_IP_APPS = "master.environment.k8s.use.load-balancer.apps";

  private static final byte[] EMPTY_PAYLOAD = new byte[0];

  private final String namespace;
  private final String namePrefix;
  private final Map<String, String> podLabels;
  private final List<V1OwnerReference> ownerReferences;
  private final Map<String, DefaultServiceDiscovered> serviceDiscovereds;
  private final ApiClientFactory apiClientFactory;
  private volatile CoreV1Api coreApi;
  private volatile WatcherThread watcherThread;
  private boolean closed;
  private final List<String> loadBalancerServiceList;

  /**
   * Constructor to create an instance for service discovery on the given Kubernetes namespace.
   *
   * @param namespace the Kubernetes namespace to perform service discovery on
   * @param namePrefix prefix applies to all service names in k8s
   * @param podLabels the set of labels for the current pod
   */
  public KubeDiscoveryService(String namespace, String namePrefix, Map<String, String> podLabels,
      List<V1OwnerReference> ownerReferences, ApiClientFactory apiClientFactory,
      Map<String, String> cConf) {
    this.namespace = namespace;
    this.namePrefix = namePrefix;
    this.serviceDiscovereds = new ConcurrentHashMap<>();
    this.podLabels = Collections.unmodifiableMap(new HashMap<>(podLabels));
    this.ownerReferences = Collections.unmodifiableList(new ArrayList<>(ownerReferences));
    this.apiClientFactory = apiClientFactory;
    String[] loadBalancerAppList = cConf
        .getOrDefault(USE_LOAD_BALANCER_IP_APPS, "").split(",");
    System.out.println("LoadBalancer IP list: " + cConf
        .getOrDefault(USE_LOAD_BALANCER_IP_APPS, ""));
    this.loadBalancerServiceList = Arrays.asList(loadBalancerAppList);
  }

  @Override
  public Cancellable register(Discoverable discoverable) {
    // Create or update the k8s Service
    // The service is created with label selector based on the current pod labels
    String serviceName = namePrefix + discoverable.getName().toLowerCase().replace('.', '-');

    try {
      CoreV1Api api = getCoreApi();
      while (true) {
        Optional<V1Service> currentService = getV1Service(api, serviceName, discoverable.getName());
        if (!currentService.isPresent()) {
          if (createV1Service(api, serviceName, discoverable)) {
            break;
          }
          // Service create encountered a conflict, loop and check for service again
          continue;
        }

        // Update service.
        if (updateV1Service(api, currentService.get(), discoverable)) {
          break;
        }
      }
    } catch (ApiException e) {
      throw new RuntimeException("Failure response from API service, code="
          + e.getCode() + ", body=" + e.getResponseBody(), e);
    } catch (IOException e) {
      throw new RuntimeException("Failed to connect to API server", e);
    }

    // We don't delete the service on cancelling since the service should stay when on restarting of one instance
    // It is the CDAP K8s operator task to remove services on CRD instance deletion by the label selector.
    return () -> {
    };
  }

  @Override
  public ServiceDiscovered discover(String name) {
    System.out.println("Discovering service: " + name);
    // Get/Create the ServiceDiscovered to return.
    ServiceDiscovered serviceDiscovered = serviceDiscovereds.computeIfAbsent(name,
        DefaultServiceDiscovered::new);

    // Start the watcher thread if it is not yet started
    WatcherThread watcherThread = this.watcherThread;

    if (watcherThread == null) {
      synchronized (this) {
        if (closed) {
          throw new IllegalStateException("Discovery service is already closed");
        }

        watcherThread = this.watcherThread;
        if (watcherThread == null) {
          watcherThread = new WatcherThread();
          watcherThread.setDaemon(true);
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
    WatcherThread watcherThread;
    synchronized (this) {
      closed = true;
      watcherThread = this.watcherThread;
      this.watcherThread = null;
    }
    if (watcherThread != null) {
      closeQuietly(watcherThread);
      watcherThread.interrupt();
    }
  }

  /**
   * Returns a {@link CoreV1Api} instance for interacting with the API server.
   *
   * @throws IOException if exception was raised during creation of {@link CoreV1Api}
   */
  private CoreV1Api getCoreApi() throws IOException {
    CoreV1Api api = coreApi;
    if (api != null) {
      return api;
    }

    synchronized (this) {
      api = coreApi;
      if (api != null) {
        return api;
      }

      ApiClient client = apiClientFactory.create();

      // Set a reasonable timeout for the watch.
      client.setReadTimeout((int) TimeUnit.MINUTES.toMillis(5));

      coreApi = api = new CoreV1Api(client);
      return api;
    }
  }

  /**
   * Finds the given Kubernetes Service.
   *
   * @param api the {@link CoreV1Api} for talking to the master
   * @param serviceName the Kubernetes service name
   * @param discoveryName the CDAP service name
   * @return an {@link Optional} of {@link V1Service}
   * @throws ApiException if failed to fetch service information from the master
   */
  private Optional<V1Service> getV1Service(CoreV1Api api,
      String serviceName, String discoveryName) throws ApiException {
    V1ServiceList serviceList = api.listNamespacedService(namespace, null, null, null, null,
        "cdap.service=" + namePrefix + discoveryName, 1,
        null, null, null, null);
    // Find the service with the given name
    return serviceList.getItems().stream()
        .filter(service -> serviceName.equals(service.getMetadata().getName()))
        .findFirst();
  }

  /**
   * Performs a create service call.
   *
   * @param api the {@link CoreV1Api} for talking to the master
   * @param serviceName name of the kubernetes service to create
   * @param discoverable the {@link Discoverable} for creating the service
   * @return {@code true} if the creation was succeeded. {@code false} if the service already exists
   * @throws ApiException if failed to create service that doens't due to service already
   *     exists
   */
  private boolean createV1Service(CoreV1Api api, String serviceName, Discoverable discoverable)
      throws ApiException {
    // Try to create the service
    V1Service service = new V1Service();
    V1ObjectMeta meta = new V1ObjectMeta();
    meta.setName(serviceName);
    meta.setLabels(Collections.singletonMap(SERVICE_LABEL, namePrefix + discoverable.getName()));

    byte[] payload = discoverable.getPayload();
    if (payload != null && payload.length > 0) {
      meta.setAnnotations(
          Collections.singletonMap(PAYLOAD_NAME, Base64.getEncoder().encodeToString(payload)));
    }

    // Set the owner reference for GC
    if (!ownerReferences.isEmpty()) {
      meta.setOwnerReferences(ownerReferences);
    }
    service.setMetadata(meta);

    V1ServicePort port = new V1ServicePort();
    port.setPort(discoverable.getSocketAddress().getPort());

    V1ServiceSpec spec = new V1ServiceSpec();
    spec.setPorts(Collections.singletonList(port));
    spec.setSelector(podLabels);
    if ("internal.router".equals(discoverable.getName())) {
      meta.putAnnotationsItem("networking.gke.io/load-balancer-type", "Internal");
      spec.setType("LoadBalancer");
    }

    service.setSpec(spec);

    try {
      api.createNamespacedService(namespace, service, null, null, null, null);
      LOG.info("Service created in kubernetes with name {} and port {}", serviceName,
          port.getPort());
    } catch (ApiException e) {
      // It means the service already exists. In this case we update the port if it is not the same.
      if (e.getCode() == HttpURLConnection.HTTP_CONFLICT) {
        LOG.debug("Service {} already exists.", serviceName);
        return false;
      }
      throw e;
    }

    return true;
  }

  /**
   * Performs an update service call.
   *
   * @param api the {@link CoreV1Api} for talking to the master
   * @param currentService the current version of {@link V1Service} to be updated
   * @param discoverable the {@link Discoverable} for updating the service
   * @return {@code true} if the update was successful {@code false} if the current service version
   *     doesn't match with the server, and there is no change to the service; or if the service
   *     does not exist
   * @throws ApiException if the update failed for reasons other than mismatch of current
   *     version or service not found
   */
  private boolean updateV1Service(CoreV1Api api, V1Service currentService,
      Discoverable discoverable) throws ApiException {
    // Find if the service is already setup to use the given discoverable port.
    // The assumption here is that the cdap operator will
    // setup the cConf in a way that pods of the same service should be binded to the same port inside a pod
    for (V1ServicePort servicePort : currentService.getSpec().getPorts()) {
      Integer port = servicePort.getPort();
      // If the port is the same, no need to update
      if (port != null && port == discoverable.getSocketAddress().getPort()) {
        return true;
      }
    }

    // Otherwise update the port and label selector
    V1ServicePort port = new V1ServicePort();
    port.setPort(discoverable.getSocketAddress().getPort());

    V1Service service = new V1ServiceBuilder(currentService).build();
    V1ObjectMeta meta = service.getMetadata();

    // Update payload
    byte[] payload = discoverable.getPayload();
    if (payload != null && payload.length > 0) {
      meta.putAnnotationsItem(PAYLOAD_NAME, Base64.getEncoder().encodeToString(payload));
    } else if (meta.getAnnotations() != null) {
      // Remove the PAYLOAD_NAME key from existing annotations
      Map<String, String> annotations = new HashMap<>(meta.getAnnotations());
      annotations.remove(PAYLOAD_NAME);
      meta.setAnnotations(annotations);
    }

    // Update the owner reference for GC
    if (!ownerReferences.isEmpty()) {
      meta.setOwnerReferences(ownerReferences);
    }
    service.getSpec().setPorts(Collections.singletonList(port));
    service.getSpec().setSelector(podLabels);
    if ("internal.router".equals(discoverable.getName())) {
      meta.putAnnotationsItem("networking.gke.io/load-balancer-type", "Internal");
      service.getSpec().setType("LoadBalancer");
    }

    try {
      api.replaceNamespacedService(meta.getName(), namespace, service, null, null, null, null);
      LOG.info("Service updated in kubernetes with name {} and port {}",
          currentService.getMetadata().getName(), port.getPort());
    } catch (ApiException e) {
      if (e.getCode() == HttpURLConnection.HTTP_CONFLICT
          || e.getCode() == HttpURLConnection.HTTP_NOT_FOUND) {
        return false;
      }
      throw e;
    }
    return true;
  }

  /**
   * Closes a {@link AutoCloseable} and swallow any exception.
   *
   * @param closeable if not null, the {@link AutoCloseable#close()} of the given closeable will
   *     be called
   */
  private void closeQuietly(@Nullable AutoCloseable closeable) {
    if (closeable == null) {
      return;
    }
    try {
      closeable.close();
    } catch (Exception e) {
      LOG.trace("Exception raised when closing watch", e);
    }
  }

  /**
   * A {@link Thread} that keep watching for changes in service in Kubernetes.
   */
  private final class WatcherThread extends AbstractWatcherThread<V1Service> {

    private final Set<String> services;

    WatcherThread() {
      super("kube-discovery-service", namespace, "", "v1", "services", apiClientFactory);
      this.services = Collections.newSetFromMap(new ConcurrentHashMap<>());
    }

    void addService(String name) {
      // Service name in K8s are prefixed
      // If this is a new service to watch, reset the watch so that the new selector will get pickup.
      if (services.add(namePrefix + name)) {
        closeWatch();
      }
    }

    @Override
    protected void updateListOptions(ListOptions options) {
      options.setLabelSelector(
          String.format("%s in (%s)", SERVICE_LABEL, String.join(",", services)));
    }

    @Override
    public void resourceAdded(V1Service service) {
      getServiceDiscovered(service)
          .ifPresent(s -> s.setDiscoverables(toDiscoverables(s.getName(), service)));
    }

    @Override
    public void resourceModified(V1Service service) {
      // Treat modify the same as add since both would contain the complete list of
      // ports exposed by the given service
      resourceAdded(service);
    }

    @Override
    public void resourceDeleted(V1Service service) {
      getServiceDiscovered(service).ifPresent(s -> s.setDiscoverables(Collections.emptySet()));
    }

    private Optional<DefaultServiceDiscovered> getServiceDiscovered(V1Service service) {
      String serviceName = service.getMetadata().getLabels().get(SERVICE_LABEL);
      if (serviceName == null) {
        return Optional.empty();
      }
      // Remove the name prefix to get the original CDAP service name
      serviceName = serviceName.substring(namePrefix.length());
      return Optional.ofNullable(serviceDiscovereds.get(serviceName));
    }

    /**
     * Creates a {@link Set} of {@link Discoverable} for the given service.
     *
     * @param name name of the service
     * @param service the K8s service object for creating the Discoverable
     * @return a {@link Set} of {@link Discoverable}.
     */
    private Set<Discoverable> toDiscoverables(String name, V1Service service) {
      V1ObjectMeta meta = service.getMetadata();
      String hostname = meta.getName();
      boolean isIpResolved = false;
      if (loadBalancerServiceList.contains(name)) {
        if (service.getStatus() != null &&
            service.getStatus().getLoadBalancer() != null &&
            service.getStatus().getLoadBalancer().getIngress() != null &&
            !service.getStatus().getLoadBalancer().getIngress().isEmpty()) {
          hostname = service.getStatus().getLoadBalancer().getIngress().get(0).getIp();
          LOG.info("Using loadbalancer IP for service {} due to config.", name);
          isIpResolved = true;
        } else {
          throw new RuntimeException("Requested to use load balancer IP but the service status doesn't have that.");
        }
      }
      List<V1ServicePort> servicePorts = service.getSpec().getPorts();

      // Decode the payload from annotation. If absent, default to empty payload
      byte[] payload = Optional.ofNullable(meta.getAnnotations())
          .map(m -> m.get(PAYLOAD_NAME))
          .map(Base64.getDecoder()::decode)
          .orElse(EMPTY_PAYLOAD);

      // We don't expect there is more than one service port, hence only pick the first one
      String finalHostname = hostname;
      boolean finalIsIpResolved = isIpResolved;
      return servicePorts.stream()
          .map(port -> createDiscoverable(name, finalHostname, port, payload,
              finalIsIpResolved))
          .filter(Objects::nonNull)
          .findFirst()
          .map(Collections::singleton)
          .orElse(Collections.emptySet());
    }

    /**
     * Creates a {@link Discoverable} for the given service.
     *
     * @param name name of the service
     * @param hostname the hostname of the service inside the Kubernetes cluster
     * @param servicePort the service port exposed by the service
     * @return a {@link Discoverable}
     */
    @Nullable
    private Discoverable createDiscoverable(String name, String hostname, V1ServicePort servicePort,
        byte[] payload, boolean isIpResolved) {
      Integer port = servicePort.getPort();
      if (port == null) {
        return null;
      }
      String namespacedHostName = isIpResolved ? hostname : String.format("%s.%s", hostname, namespace);
      LOG.info("Creating discoverable: {}-{}-{}", hostname, port, isIpResolved);
      return new Discoverable(name, InetSocketAddress.createUnresolved(namespacedHostName, port),
          payload);
    }
  }
}
