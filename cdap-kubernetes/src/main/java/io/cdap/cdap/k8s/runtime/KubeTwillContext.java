/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.k8s.runtime;

import com.google.common.collect.ImmutableSet;
import com.squareup.okhttp.Call;
import io.cdap.cdap.k8s.common.AbstractWatcherThread;
import io.cdap.cdap.master.environment.k8s.KubeMasterEnvironment;
import io.cdap.cdap.master.environment.k8s.PodInfo;
import io.cdap.cdap.master.spi.twill.ExtendedTwillContext;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CustomObjectsApi;
import io.kubernetes.client.models.V1OwnerReference;
import io.kubernetes.client.util.Config;
import org.apache.twill.api.ElectionHandler;
import org.apache.twill.api.RunId;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nullable;

/**
 * An implementation of {@link TwillContext} for k8s environment.
 */
public class KubeTwillContext implements ExtendedTwillContext, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(KubeTwillContext.class);

  private final RuntimeSpecification runtimeSpec;
  private final RunId appRunId;
  private final RunId runId;
  private final String[] appArguments;
  private final String[] arguments;
  private final DiscoveryService discoveryService;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final int instanceId;
  private final CompletableFuture<AtomicInteger> instancesFuture;
  private final Cancellable instanceWatcherCancellable;
  private final String uid;

  public KubeTwillContext(RuntimeSpecification runtimeSpec, RunId appRunId, RunId runId,
                          String[] appArguments, String[] arguments,
                          KubeMasterEnvironment masterEnv) throws IOException {
    this.runtimeSpec = runtimeSpec;
    this.appRunId = appRunId;
    this.runId = runId;
    this.appArguments = appArguments;
    this.arguments = arguments;
    this.discoveryService = masterEnv.getDiscoveryServiceSupplier().get();
    this.discoveryServiceClient = masterEnv.getDiscoveryServiceClientSupplier().get();

    PodInfo podInfo = masterEnv.getPodInfo();

    int instanceId = 0;
    // For stateful set, the instance ID is the suffix number. Otherwise, we don't support instance id for now.
    if (podInfo.getOwnerReferences().stream().anyMatch(r -> "StatefulSet".equals(r.getKind()))) {
      int idx = podInfo.getName().lastIndexOf('-');
      try {
        instanceId = idx < 0 ? 0 : Integer.parseInt(podInfo.getName().substring(idx + 1));
      } catch (NumberFormatException e) {
        LOG.warn("Failed to parse instance id from pod name {}", podInfo.getName(), e);
      }
    }
    this.instanceId = instanceId;
    this.instancesFuture = new CompletableFuture<>();
    this.instanceWatcherCancellable = startInstanceWatcher(podInfo, instancesFuture);
    this.uid = new String(Files.readAllBytes(Paths.get(podInfo.getPodInfoDir(), podInfo.getUidFile())),
                          StandardCharsets.UTF_8);
  }

  @Override
  public RunId getRunId() {
    return runId;
  }

  @Override
  public RunId getApplicationRunId() {
    return appRunId;
  }

  @Override
  public int getInstanceCount() {
    try {
      return instancesFuture.get().get();
    } catch (Exception e) {
      throw new RuntimeException("Failed to get total number of instances", e);
    }
  }

  @Override
  public InetAddress getHost() {
    try {
      return InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String[] getArguments() {
    return arguments;
  }

  @Override
  public String[] getApplicationArguments() {
    return appArguments;
  }

  @Override
  public TwillRunnableSpecification getSpecification() {
    return runtimeSpec.getRunnableSpecification();
  }

  @Override
  public int getInstanceId() {
    return instanceId;
  }

  @Override
  public int getVirtualCores() {
    return runtimeSpec.getResourceSpecification().getVirtualCores();
  }

  @Override
  public int getMaxMemoryMB() {
    return runtimeSpec.getResourceSpecification().getMemorySize();
  }

  @Override
  public ServiceDiscovered discover(String name) {
    return discoveryServiceClient.discover(name);
  }

  @Override
  public Cancellable electLeader(String name, ElectionHandler participantHandler) {
    throw new UnsupportedOperationException("Leader election is not supported");
  }

  @Override
  public Lock createLock(String name) {
    throw new UnsupportedOperationException("Distributed lock is not supported");
  }

  @Override
  public Cancellable announce(String serviceName, int port) {
    return discoveryService.register(new Discoverable(serviceName, new InetSocketAddress(getHost(), port)));
  }

  @Override
  public Cancellable announce(String serviceName, int port, byte[] payload) {
    return discoveryService.register(new Discoverable(serviceName, new InetSocketAddress(getHost(), port), payload));
  }

  @Override
  public void close() {
    instanceWatcherCancellable.cancel();
    if (!instancesFuture.isDone()) {
      instancesFuture.complete(new AtomicInteger());
    }
  }

  @Nullable
  @Override
  public String getUID() {
    return uid;
  }

  /**
   * Starts a {@link AbstractWatcherThread} to watch for changes in number of replicas.
   *
   * @param podInfo the {@link PodInfo} about the pod that this process is running in
   * @param replicasFuture a {@link CompletableFuture} of {@link AtomicInteger}. The future will be completed when the
   *                       initial replicas is fetched. The {@link AtomicInteger} used to complete the future
   *                       will be reflecting the number of replicas live.
   * @return a {@link Cancellable} to stop the watcher thread
   * @throws IOException if failed to create an {@link ApiClient}
   */
  private static Cancellable startInstanceWatcher(PodInfo podInfo,
                                                  CompletableFuture<AtomicInteger> replicasFuture) throws IOException {
    List<V1OwnerReference> ownerReferences = podInfo.getOwnerReferences();

    // This shouldn't happen, but just return to protect against failure
    if (ownerReferences.isEmpty()) {
      replicasFuture.complete(new AtomicInteger());
      return () -> { };
    }

    // Find the ReplicaSet, Deployment, or StatefulSet owner
    Set<String> supportedKind = ImmutableSet.of("ReplicaSet", "Deployment", "StatefulSet");
    V1OwnerReference ownerRef = ownerReferences.stream()
      .filter(ref -> supportedKind.contains(ref.getKind()))
      .findFirst()
      .orElse(null);

    // If can't find any, which shouldn't happen, just return
    if (ownerRef == null) {
      replicasFuture.complete(new AtomicInteger());
      return () -> { };
    }

    ApiClient client = Config.defaultClient();
    // Set a reasonable timeout for the watch.
    client.getHttpClient().setReadTimeout(5, TimeUnit.MINUTES);

    CustomObjectsApi api = new CustomObjectsApi(client);

    // apiVersion is in the format of [group]/[version]
    String[] apiVersion = ownerRef.getApiVersion().split("/", 2);
    String group = apiVersion[0];
    String version = apiVersion[1];
    String plurals = ownerRef.getKind().toLowerCase() + "s";
    String fieldSelector = "metadata.name=" + ownerRef.getName();
    AtomicInteger replicas = new AtomicInteger();

    // Watch for the status.replicas field
    AbstractWatcherThread<Object> watcherThread = new AbstractWatcherThread<Object>("twill-instance-watch",
                                                                                    podInfo.getNamespace()) {
      @Override
      protected Call createCall(String namespace, @Nullable String labelSelector) throws ApiException {
        return api.listNamespacedCustomObjectCall(group, version, namespace, plurals, null, fieldSelector,
                                                  labelSelector, null, null, true, null, null);
      }

      @Override
      protected ApiClient getApiClient() {
        return client;
      }

      @Override
      public void resourceAdded(Object resource) {
        resourceModified(resource);
      }

      @Override
      public void resourceModified(Object resource) {
        // The CustomObjectsApi returns Map<String, Map> to represent the nested structure of the resource
        Object statusReplicas = getValue(getValue(resource, "status"), "replicas");
        if (statusReplicas == null) {
          return;
        }
        if (!(statusReplicas instanceof Number)) {
          try {
            statusReplicas = Double.parseDouble(statusReplicas.toString());
          } catch (NumberFormatException e) {
            LOG.warn("Failed to parse status.replicas from resource {}/{}", plurals, ownerRef.getName(), e);
          }
        }
        replicas.set(((Number) statusReplicas).intValue());
        if (!replicasFuture.isDone()) {
          replicasFuture.complete(replicas);
        }
      }

      @Nullable
      private Object getValue(Object obj, String key) {
        if (!(obj instanceof Map)) {
          return null;
        }
        //noinspection rawtypes
        return ((Map) obj).get(key);
      }
    };

    watcherThread.setDaemon(true);
    watcherThread.start();

    return watcherThread::close;
  }
}
