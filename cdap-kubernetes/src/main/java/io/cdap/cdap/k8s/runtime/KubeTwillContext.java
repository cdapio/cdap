/*
 * Copyright © 2020-2021 Cask Data, Inc.
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
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.k8s.common.AbstractWatcherThread;
import io.cdap.cdap.master.environment.k8s.ApiClientFactory;
import io.cdap.cdap.master.environment.k8s.KubeMasterEnvironment;
import io.cdap.cdap.master.environment.k8s.PodInfo;
import io.cdap.cdap.master.spi.twill.ExtendedTwillContext;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesApi;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import io.kubernetes.client.util.generic.options.ListOptions;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.twill.api.ElectionHandler;
import org.apache.twill.api.RunId;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link TwillContext} for k8s environment.
 */
public class KubeTwillContext implements ExtendedTwillContext, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(KubeTwillContext.class);
  private static final String INSTANCES_ASSIGNMENT = "cdap.instances.assignment";

  private final RuntimeSpecification runtimeSpec;
  private final RunId appRunId;
  private final RunId runId;
  private final String[] appArguments;
  private final String[] arguments;
  private final DiscoveryService discoveryService;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final CompletableFuture<AtomicInteger> instancesFuture;
  private final CompletableFuture<AtomicInteger> instanceIdFuture;
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

    this.instancesFuture = new CompletableFuture<>();
    this.instanceIdFuture = new CompletableFuture<>();
    this.instanceWatcherCancellable = startInstanceWatcher(podInfo, instancesFuture,
        instanceIdFuture,
        masterEnv.getApiClientFactory());
    this.uid = new String(
        Files.readAllBytes(Paths.get(podInfo.getPodInfoDir(), podInfo.getUidFile())),
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

  /**
   * Returns the instanceId. Note that the instance ID might change over the lifetime of the process
   * due to k8s Deployment can be removing a pod with lower instance Id on scaling down.
   */
  @Override
  public int getInstanceId() {
    try {
      return instanceIdFuture.get().get();
    } catch (Exception e) {
      throw new RuntimeException("Failed to get instance id", e);
    }
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
    return discoveryService.register(
        new Discoverable(serviceName, new InetSocketAddress(getHost(), port)));
  }

  @Override
  public Cancellable announce(String serviceName, int port, byte[] payload) {
    return discoveryService.register(
        new Discoverable(serviceName, new InetSocketAddress(getHost(), port), payload));
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
   * @param instancesFuture a {@link CompletableFuture} of {@link AtomicInteger}. The future
   *     will be completed when the initial replicas is fetched. The {@link AtomicInteger} used to
   *     complete the future will be reflecting the number of replicas live.
   * @param instanceIdFuture a {@link CompletableFuture} that will be completed when the
   *     instanceId is determined.
   * @return a {@link Cancellable} to stop the watcher thread
   */
  private static Cancellable startInstanceWatcher(PodInfo podInfo,
      CompletableFuture<AtomicInteger> instancesFuture,
      CompletableFuture<AtomicInteger> instanceIdFuture,
      ApiClientFactory apiClientFactory) {

    List<V1OwnerReference> ownerReferences = podInfo.getOwnerReferences();

    // This shouldn't happen, but just return to protect against failure
    if (ownerReferences.isEmpty()) {
      instancesFuture.complete(new AtomicInteger());
      instanceIdFuture.complete(new AtomicInteger());
      return () -> {
      };
    }

    // Find the ReplicaSet, Deployment, or StatefulSet owner
    Set<String> supportedKind = ImmutableSet.of("ReplicaSet", "Deployment", "StatefulSet", "Job");
    V1OwnerReference ownerRef = ownerReferences.stream()
        .filter(ref -> supportedKind.contains(ref.getKind()))
        .findFirst()
        .orElse(null);

    // If can't find any, which shouldn't happen, just return
    if (ownerRef == null) {
      instancesFuture.complete(new AtomicInteger());
      instanceIdFuture.complete(new AtomicInteger());
      return () -> {
      };
    }

    // apiVersion is in the format of [group]/[version]
    String[] apiVersion = ownerRef.getApiVersion().split("/", 2);
    String group = apiVersion[0];
    String version = apiVersion[1];
    String plural = ownerRef.getKind().toLowerCase() + "s";

    // Watch for the changes in number of instances.
    InstanceWatcherThread watcherThread = new InstanceWatcherThread(group, version, plural,
        podInfo, ownerRef,
        instancesFuture, instanceIdFuture,
        apiClientFactory);
    watcherThread.start();

    return watcherThread::close;
  }

  /**
   * A {@link AbstractWatcherThread} for dynamically watching for changes in number of instances.
   */
  private static class InstanceWatcherThread extends
      AbstractWatcherThread<DynamicKubernetesObject> {

    private static final Random RANDOM = new Random();

    private final PodInfo podInfo;
    private final V1OwnerReference ownerRef;
    private final AtomicInteger instances;
    private final AtomicInteger instanceId;
    private final CompletableFuture<AtomicInteger> instancesFuture;
    private final CompletableFuture<AtomicInteger> instanceIdFuture;
    private final ExecutorService executor;

    InstanceWatcherThread(String group, String version, String plural, PodInfo podInfo,
        V1OwnerReference ownerRef, CompletableFuture<AtomicInteger> instancesFuture,
        CompletableFuture<AtomicInteger> instanceIdFuture, ApiClientFactory apiClientFactory) {
      super("twill-instance-watch", podInfo.getNamespace(), group, version, plural,
          apiClientFactory);
      this.podInfo = podInfo;
      this.ownerRef = ownerRef;
      this.instances = new AtomicInteger();
      this.instanceId = new AtomicInteger();
      this.instancesFuture = instancesFuture;
      this.instanceIdFuture = instanceIdFuture;
      this.executor = Executors.newSingleThreadExecutor(
          Threads.createDaemonThreadFactory("twill-instance-assign"));
    }

    @Override
    public void close() {
      super.close();
      executor.shutdownNow();
    }

    @Override
    protected void updateListOptions(ListOptions options) {
      options.setFieldSelector("metadata.name=" + ownerRef.getName());
    }

    @Override
    public void resourceAdded(DynamicKubernetesObject resource) {
      resourceModified(resource);
    }

    @Override
    public void resourceModified(DynamicKubernetesObject resource) {
      // For Job type, the number of instances is in the spec.parallelism field, otherwise it is in
      // the status.replicas field
      int count;
      if ("Job".equals(ownerRef.getKind())) {
        JsonObject spec = resource.getRaw().getAsJsonObject("spec");
        if (spec == null) {
          return;
        }
        count = spec.getAsJsonPrimitive("parallelism").getAsInt();
      } else {
        JsonObject status = resource.getRaw().getAsJsonObject("status");
        if (status == null) {
          return;
        }
        JsonPrimitive value = status.getAsJsonPrimitive("replicas");
        if (value == null) {
          return;
        }
        count = value.getAsInt();
      }

      int oldInstances = instances.getAndSet(count);
      if (oldInstances != count) {
        LOG.debug("Number of instances change from {} to {}", oldInstances, count);
        executor.execute(this::assignInstanceId);
      }

      instancesFuture.complete(instances);
    }

    /**
     * Assigns the instance Id for this process.
     */
    private void assignInstanceId() {
      // For StatefulSet, the instance ID is the suffix number.
      if ("StatefulSet".equals(ownerRef.getKind())) {
        int idx = podInfo.getName().lastIndexOf('-');
        try {
          if (idx >= 0) {
            setInstanceId(Integer.parseInt(podInfo.getName().substring(idx + 1)));
            return;
          }
        } catch (NumberFormatException e) {
          LOG.warn("Failed to parse instance id from pod name {}", podInfo.getName(), e);
        }
      }

      // If it is not StatefulSet or failed to extract instanceId from the pod name, use the assignment logic
      performAssignInstanceId();
    }

    /**
     * Performs the instance Id assignment logic using annotations on the owner object. The logic
     * involves setting an annotation with key {@link #INSTANCES_ASSIGNMENT}, and value is a Json
     * map that stores association of instance Id to pod name ({@code instanceId -> Pod Name}). Each
     * of the pod instances will race to update the assignment map, which we rely on the compare and
     * set property of the k8s replace API to guarantee a consistent view across all pods.
     */
    private void performAssignInstanceId() {

      while (InstanceWatcherThread.this.isAlive()) {
        int instances = this.instances.get();

        try {
          DynamicKubernetesApi api = new DynamicKubernetesApi(group, version, plural,
              getApiClient());
          Gson gson = api.getGson();

          DynamicKubernetesObject owner = api.get(podInfo.getNamespace(),
              ownerRef.getName()).throwsApiException().getObject();

          V1ObjectMeta ownerMetadata = owner.getMetadata();
          Map<String, String> annotations = new HashMap<>(
              Optional.ofNullable(ownerMetadata.getAnnotations())
                  .orElse(Collections.emptyMap()));
          Map<Integer, String> instanceAssignments;
          if (!annotations.containsKey(INSTANCES_ASSIGNMENT)) {
            instanceAssignments = new TreeMap<>();
          } else {
            instanceAssignments = gson.fromJson(annotations.get(INSTANCES_ASSIGNMENT),
                new TypeToken<TreeMap<Integer, String>>() {
                }.getType());
            // Remove instanceId >= number of instances
            instanceAssignments.entrySet().removeIf(e -> e.getKey() >= instances);
          }

          // See if the current pod already has an assignment
          int instanceId = instanceAssignments.entrySet().stream()
              .filter(e -> podInfo.getName().equals(e.getValue()))
              .map(Map.Entry::getKey)
              .findFirst()
              .orElse(-1);

          if (instanceId >= 0) {
            LOG.debug("Found existing instance ID {}", instanceId);
            setInstanceId(instanceId);
            break;
          }

          // If there is no empty slot, it can either be:
          // 1. Pods get restarted in deployment, hence some of the assignments are no longer valid
          // 2. Number of instances changed
          // If there still no empty slot after cleaning up, we'll retry with fetching a new assignments
          // and instances again.
          if (!cleanupAssignments(instanceAssignments, instances)) {
            continue;
          }

          // make sure we choose index that is still unreserved yet, start from a random point to avoid too many
          // conflicts at the beginning.
          int id = RANDOM.nextInt(instances);
          for (int i = 0; i < instances; i++) {
            if (!instanceAssignments.containsKey(id)) {
              instanceId = id;
              break;
            }
            id = (id + 1) % instances;
          }

          if (instanceId < 0) {
            // This shouldn't happen. Retry just in case.
            continue;
          }

          instanceAssignments.put(instanceId, podInfo.getName());
          annotations.put(INSTANCES_ASSIGNMENT, gson.toJson(instanceAssignments,
              new TypeToken<TreeMap<Integer, String>>() {
              }.getType()));
          owner.setMetadata(ownerMetadata.annotations(annotations));

          api.update(owner).throwsApiException();
          LOG.debug("Assigned new instance ID {}", instanceId);
          setInstanceId(instanceId);
          break;
        } catch (Exception e) {
          if (e instanceof ApiException && ((ApiException) e).getCode() == 409) {
            LOG.debug(
                "Conflict occurred while updating the instanceId assignment, operation will be be retried.");
            LOG.trace("Trace log for conflict", e);
          } else {
            LOG.warn(
                "Exception raised when trying to assign instance ID. Operation will be retried", e);
          }
          try {
            TimeUnit.MILLISECONDS.sleep(RANDOM.nextInt(300));
          } catch (InterruptedException interruptedException) {
            return;
          }
        }
      }
    }

    private void setInstanceId(int id) {
      instanceId.set(id);
      instanceIdFuture.complete(instanceId);
    }

    /**
     * Removes instance ids assigned to inactive pods.
     *
     * @param assignments the current assignment to cleanup
     * @param instances total number of instances
     * @return {@code true} if the assignments map is clear to have enough space for a new
     *     assignment
     * @throws ApiException if failed to query the k8s API server
     */
    private boolean cleanupAssignments(Map<Integer, String> assignments,
        int instances) throws ApiException, IOException {
      if (assignments.size() < instances) {
        return true;
      }

      CoreV1Api coreApi = new CoreV1Api(getApiClient());
      String labelSelector = podInfo.getLabels().entrySet().stream()
          .map(e -> e.getKey() + "=" + e.getValue())
          .collect(Collectors.joining(","));

      V1PodList podList = coreApi.listNamespacedPod(podInfo.getNamespace(), null, null, null,
          "status.phase!=Failed",
          labelSelector, null, null, null, null, null);
      Set<String> activePods = podList.getItems().stream()
          .map(V1Pod::getMetadata)
          .filter(Objects::nonNull)
          .map(V1ObjectMeta::getName)
          .collect(Collectors.toSet());

      Iterator<Map.Entry<Integer, String>> iterator = assignments.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<Integer, String> entry = iterator.next();
        if (!activePods.contains(entry.getValue())) {
          LOG.debug("Removed inactive pod instance id assignment {}={}", entry.getKey(),
              entry.getValue());
          iterator.remove();
        }
      }

      return assignments.size() < instances;
    }
  }
}
