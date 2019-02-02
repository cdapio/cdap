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

package co.cask.cdap.k8s.discovery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.gson.reflect.TypeToken;
import com.squareup.okhttp.Call;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1Service;
import io.kubernetes.client.models.V1ServiceBuilder;
import io.kubernetes.client.models.V1ServiceList;
import io.kubernetes.client.models.V1ServicePort;
import io.kubernetes.client.models.V1ServiceSpec;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Watch;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Implementation of {@link DiscoveryService} and {@link DiscoveryServiceClient} that uses Kubernetes API to
 * announce and discover service locations.
 * This service assumes kubernetes services are created with two labels, "app=cdap" and "service=[service-name]",
 * where [service-name] is the CDAP service name.
 * On registering service via the {@link #register(Discoverable)} method, a new k8s Service with name
 * "cdap-[transformed-service-name]" will be created with labels "app=cdap" and "service=[service-name]".
 * The [transformed-service-name] is the CDAP service name with "." replaced with "-" to conform to the naming
 * requirement in K8s. The service selector will be set to include the current pod labels of keys "app" and "container".
 */
public class KubeDiscoveryService implements DiscoveryService, DiscoveryServiceClient, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(KubeDiscoveryService.class);

  private static final Range<Integer> FAILURE_RETRY_RANGE = Range.closedOpen(100, 2000);

  // The response type from K8s when a service was added
  private static final String ADDED = "ADDED";
  // The response type from K8s when a service was deleted
  private static final String DELETED = "DELETED";
  // The response type from K8s when a service was modified
  private static final String MODIFIED = "MODIFIED";
  // Not to support payload. Use a constant for the payload due to bug in TWILL-264
  private static final byte[] EMPTY_PAYLOAD = new byte[0];

  private final String namespace;
  private final Map<String, String> podLabels;
  private final Map<String, DefaultServiceDiscovered> serviceDiscovereds;
  private volatile CoreV1Api coreApi;
  private volatile WatcherThread watcherThread;
  private boolean closed;

  /**
   * Constructor to create an instance for service discovery on the given Kubernetes namespace.
   *
   * @param namespace the Kubernetes namespace to perform service discovery on
   * @param podLabels the set of labels for the current pod
   */
  public KubeDiscoveryService(String namespace, Map<String, String> podLabels) {
    this.namespace = namespace;
    this.serviceDiscovereds = new ConcurrentHashMap<>();
    this.podLabels = Collections.unmodifiableMap(new HashMap<>(podLabels));
  }

  @Override
  public Cancellable register(Discoverable discoverable) {
    // Create or update the k8s Service
    // The service is created with label selector based on the current pod labels
    String serviceName = "cdap-" + discoverable.getName().toLowerCase().replace('.', '-');

    try {
      CoreV1Api api = getCoreApi();
      while (true) {
        // First try to create the service
        if (createV1Service(api, serviceName, discoverable)) {
          break;
        }

        // If creation failed, we update the service.
        // To update, first need to find the service version.
        Optional<V1Service> currentService = getV1Service(api, serviceName, discoverable.getName());
        if (!currentService.isPresent()) {
          // Loop and try to create again
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
    return () -> { };
  }

  @Override
  public ServiceDiscovered discover(String name) {
    // Get/Create the ServiceDiscovered to return.
    ServiceDiscovered serviceDiscovered = serviceDiscovereds.computeIfAbsent(name, DefaultServiceDiscovered::new);

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

      ApiClient client = Config.defaultClient();

      // Turn off timeout, otherwise the watch will throw SocketTimeoutException while watching.
      client.getHttpClient().setReadTimeout(0, TimeUnit.MILLISECONDS);

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
                                                          "app=cdap,service=" + discoveryName, 1, null, null, null);
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
   * @throws ApiException if failed to create service that doens't due to service already exists
   */
  private boolean createV1Service(CoreV1Api api, String serviceName, Discoverable discoverable) throws ApiException {
    // Try to create the service
    V1Service service = new V1Service();
    V1ObjectMeta meta = new V1ObjectMeta();
    meta.setName(serviceName);
    meta.setLabels(ImmutableMap.of("app", "cdap", "service", discoverable.getName()));
    service.setMetadata(meta);

    V1ServicePort port = new V1ServicePort();
    port.setPort(discoverable.getSocketAddress().getPort());

    V1ServiceSpec spec = new V1ServiceSpec();
    spec.setPorts(Collections.singletonList(port));
    spec.setSelector(podLabels);

    service.setSpec(spec);

    try {
      api.createNamespacedService(namespace, service, null);
    } catch (ApiException e) {
      // It means the service already exists. In this case we update the port if it is not the same.
      if (e.getCode() == HttpURLConnection.HTTP_CONFLICT) {
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
   * @return {@code true} if the update was successful
   *         {@code false} if the current service version doesn't match with the server, and there is no
   *         change to the service
   * @throws ApiException if the update failed that doesn't due to mismatch of current version
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

    // Otherwise update the port
    V1ServicePort port = new V1ServicePort();
    port.setPort(discoverable.getSocketAddress().getPort());

    V1Service service = new V1ServiceBuilder(currentService).build();
    service.getSpec().setPorts(Collections.singletonList(port));

    try {
      api.replaceNamespacedService(service.getMetadata().getName(), namespace, service, null);
    } catch (ApiException e) {
      if (e.getCode() == HttpURLConnection.HTTP_CONFLICT) {
        return false;
      }
      throw e;
    }
    return true;
  }

  /**
   * Closes a {@link AutoCloseable} and swallow any exception.
   *
   * @param closeable if not null, the {@link AutoCloseable#close()} of the given closeable will be called
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
  private final class WatcherThread extends Thread implements AutoCloseable {

    private final Set<String> services;
    private final Random random;
    private volatile Watch<V1Service> watch;
    private volatile boolean stopped;

    WatcherThread() {
      super("kube-discovery-service");
      this.services = Collections.newSetFromMap(new ConcurrentHashMap<>());
      this.random = new Random();
    }

    /**
     * Adds the given service to the watch list.
     *
     * @param service name of the service
     */
    void addService(String service) {
      // If not adding a new service to watch, just return
      if (!services.add(service)) {
        return;
      }
      resetAndCloseWatch();
    }

    @Override
    public void run() {
      LOG.info("Start watching for changes in kubernetes services");

      int failureCount = 0;
      while (!stopped) {
        try {
          Watch<V1Service> watch = getWatch();

          // If we can create the watch, reset the failure count.
          failureCount = 0;

          // The watch can only be null if the watcher thread is stopping
          if (watch == null) {
            break;
          }

          // The hasNext() will block until there are new data or watch is closed
          while (!stopped && watch.hasNext()) {
            Watch.Response<V1Service> response = watch.next();
            String serviceName = response.object.getMetadata().getLabels().get("service");
            if (serviceName == null) {
              continue;
            }
            DefaultServiceDiscovered serviceDiscovered = serviceDiscovereds.get(serviceName);
            if (serviceDiscovered == null) {
              continue;
            }

            switch (response.type) {
              // For ADDED and MODIFIED, they are treated the same since both would contains the complete list of
              // ports exposed by the given service
              case ADDED:
              case MODIFIED:
                serviceDiscovered.setDiscoverables(toDiscoverables(serviceName,
                                                                   response.object.getMetadata().getName(),
                                                                   response.object.getSpec().getPorts()));
                break;
              case DELETED: {
                serviceDiscovered.setDiscoverables(Collections.emptySet());
                break;
              }
              default:
                LOG.trace("Ignore watch type {}", response.type);
            }
          }
        } catch (Exception e) {
          // Ignore the exception if it is during stopping of the thread, which is expected to happen
          if (stopped) {
            break;
          }

          // We just retry on any form of exceptions
          Throwable cause = e.getCause();
          if (cause instanceof IOException || cause instanceof IllegalStateException) {
            // Log at lower level if it is caused by IOException or IllegalStateException, which will happen
            // if connection to the API server is lost or the watch is closed
            LOG.trace("Exception raised when watching for service changes", e);
          } else {
            LOG.warn("Exception raised when watching for service changes", e);
          }

          // Clear watch so that a new only will be created in next iteration.
          resetAndCloseWatch();

          try {
            // If not stopped and failed more than once, sleep for some random second.
            // We only sleep when fail more than one time because exception could be thrown when a new service
            // is being added, the watch would get closed, hence throwing exception.
            if (!stopped && failureCount++ > 0) {
              // Sleep for some random milliseconds before retrying
              int sleepMs = random.nextInt(FAILURE_RETRY_RANGE.upperEndpoint()) + FAILURE_RETRY_RANGE.lowerEndpoint();
              TimeUnit.MILLISECONDS.sleep(sleepMs);
            }
          } catch (InterruptedException ex) {
            // Can only happen on stopping
            break;
          }
        }
      }
      LOG.info("Stop watching for changes in kubernetes services");
    }

    @Override
    public void close() {
      LOG.debug("Stop watching for kubernetes service");
      stopped = true;
      resetAndCloseWatch();
    }

    /**
     * Gets a {@link Watch} for watching for service resources change. This method should only be called
     * from the watcher thread.
     *
     * @return a {@link Watch} or {@code null} if the watching thread is already terminated.
     */
    @Nullable
    private Watch<V1Service> getWatch() throws IOException, ApiException {
      Watch<V1Service> watch = this.watch;
      if (watch != null) {
        return watch;
      }

      synchronized (this) {
        // Need to check here for the case where the thread that calls the close() method set the flag to true,
        // then this thread grab the lock and update the watch field.
        if (stopped) {
          return null;
        }

        // There is only single thread (the run thread) that will call this method,
        // hence if the watch was null outside of this sync block, it will stay as null here.
        String labelSelector = String.format("app=cdap,service in (%s)",
                                             services.stream().collect(Collectors.joining(",")));
        LOG.info("Creating watch with label selector {}", labelSelector);
        CoreV1Api coreApi = getCoreApi();
        Call call = coreApi.listNamespacedServiceCall(namespace, null, null, null, null, labelSelector,
                                                      null, null, null, true, null, null);
        this.watch = watch = Watch.createWatch(KubeDiscoveryService.this.coreApi.getApiClient(), call,
                                               new TypeToken<Watch.Response<V1Service>>() { }.getType());
        return watch;
      }
    }

    /**
     * Closes the existing watch if there is one and reset the field to {@code null}.
     */
    private void resetAndCloseWatch() {
      Watch<V1Service> watch;
      synchronized (this) {
        watch = this.watch;
        this.watch = null;
      }
      closeQuietly(watch);
    }

    /**
     * Creates a {@link Set} of {@link Discoverable} for the given service.
     *
     * @param name name of the service
     * @param hostname the hostname of the service inside the Kubernetes cluster
     * @param servicePorts the list of service ports exposed by the Kubernetes service
     * @return a {@link Set} of {@link Discoverable}.
     */
    private Set<Discoverable> toDiscoverables(String name, String hostname,
                                              Collection<? extends V1ServicePort> servicePorts) {
      // We don't expect there is more than one service port, hence only pick the first one
      return servicePorts.stream()
        .map(port -> createDiscoverable(name, hostname, port))
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
    private Discoverable createDiscoverable(String name, String hostname, V1ServicePort servicePort) {
      Integer port = servicePort.getPort();
      if (port == null) {
        return null;
      }
      return new Discoverable(name, InetSocketAddress.createUnresolved(hostname, port), EMPTY_PAYLOAD);
    }
  }
}
