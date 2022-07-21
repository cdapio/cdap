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

package io.cdap.cdap.k8s.runtime;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import io.cdap.cdap.k8s.common.AbstractWatcherThread;
import io.cdap.cdap.k8s.common.ResourceChangeListener;
import io.cdap.cdap.master.environment.k8s.PodInfo;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.util.Config;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RunId;
import org.apache.twill.api.SecureStoreUpdater;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.security.SecureStoreRenewer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.RunIds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Kubernetes version of a TwillRunner.
 *
 * All resources created by the runner will have a name of the form:
 *
 * [cleansed app name]-[hash]
 *
 * Each resource will also have the following labels:
 *
 * cdap.twill.runner=k8s
 * cdap.twill.run.id=[run id]
 * cdap.twill.app=[cleansed app name]
 *
 * and annotations:
 *
 * cdap.twill.app=[literal app name]
 */
public class KubeTwillRunnerService implements TwillRunnerService {

  static final String START_TIMEOUT_ANNOTATION = "cdap.app.start.timeout.millis";

  private static final Logger LOG = LoggerFactory.getLogger(KubeTwillRunnerService.class);

  static final String APP_LABEL = "cdap.twill.app";
  private static final String RUN_ID_LABEL = "cdap.twill.run.id";
  private static final String RUNNER_LABEL = "cdap.twill.runner";
  private static final String RUNNER_LABEL_VAL = "k8s";

  private final MasterEnvironmentContext masterEnvContext;
  private final String kubeNamespace;
  private final String resourcePrefix;
  private final PodInfo podInfo;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final Map<String, String> extraLabels;
  private final Map<String, Map<Type, AppResourceWatcherThread<?>>> resourceWatchers;
  private final Map<String, KubeLiveInfo> liveInfos;
  private final Lock liveInfoLock;
  private final int jobCleanupIntervalMins;
  private final int jobCleanBatchSize;
  private final String selector;
  private final boolean enableMonitor;
  private ApiClient apiClient;
  private CoreV1Api coreV1Api;
  private ScheduledExecutorService monitorScheduler;
  private ScheduledExecutorService jobCleanerService;

  public KubeTwillRunnerService(MasterEnvironmentContext masterEnvContext,
                                String kubeNamespace, DiscoveryServiceClient discoveryServiceClient,
                                PodInfo podInfo, String resourcePrefix, Map<String, String> extraLabels,
                                int jobCleanupIntervalMins, int jobCleanBatchSize,
                                boolean enableMonitor) {
    this.masterEnvContext = masterEnvContext;
    this.kubeNamespace = kubeNamespace;
    this.podInfo = podInfo;
    this.resourcePrefix = resourcePrefix;
    this.discoveryServiceClient = discoveryServiceClient;
    this.extraLabels = Collections.unmodifiableMap(new HashMap<>(extraLabels));

    // Selects all runs started by the k8s twill runner that has the run id label
    this.selector = String.format("%s=%s,%s", RUNNER_LABEL, RUNNER_LABEL_VAL, RUN_ID_LABEL);
    // Contains mapping of the Kubernetes namespace to a map of resource types and the watcher threads
    this.resourceWatchers = new HashMap<>();
    this.liveInfos = new ConcurrentSkipListMap<>();
    this.liveInfoLock = new ReentrantLock();
    this.jobCleanupIntervalMins = jobCleanupIntervalMins;
    this.jobCleanBatchSize = jobCleanBatchSize;
    this.enableMonitor = enableMonitor;
  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable) {
    return prepare(runnable, ResourceSpecification.BASIC);
  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable, ResourceSpecification resourceSpecification) {
    return prepare(new SingleRunnableApplication(runnable, resourceSpecification));
  }

  @Override
  public TwillPreparer prepare(TwillApplication application) {
    TwillSpecification spec = application.configure();
    RunId runId = RunIds.generate();

    Location appLocation = getApplicationLocation(spec.getName(), runId);
    Map<String, String> labels = new HashMap<>(extraLabels);
    labels.put(RUNNER_LABEL, RUNNER_LABEL_VAL);
    labels.put(APP_LABEL, spec.getName());
    labels.put(RUN_ID_LABEL, runId.getId());

    return new KubeTwillPreparer(masterEnvContext, apiClient, kubeNamespace, podInfo,
                                 spec, runId, appLocation, resourcePrefix, labels,
                                 (resourceType, meta, timeout, timeoutUnit) -> {
      // Adds the controller to the LiveInfo.
      liveInfoLock.lock();
      try {
        KubeTwillController controller = createKubeTwillController(spec.getName(), runId, resourceType, meta);
        if (!enableMonitor) {
          //since monitor is disabled, we fire and forget
          return controller;
        }
        KubeLiveInfo liveInfo = liveInfos.computeIfAbsent(spec.getName(), n -> new KubeLiveInfo(resourceType, n));
        return liveInfo.addControllerIfAbsent(runId, timeout, timeoutUnit, controller, meta);
      } finally {
        liveInfoLock.unlock();
      }
    });
  }

  @Nullable
  @Override
  public TwillController lookup(String applicationName, RunId runId) {
    KubeLiveInfo liveInfo = liveInfos.get(applicationName);
    return liveInfo == null ? null : liveInfo.getController(runId);
  }

  @Override
  public Iterable<TwillController> lookup(String applicationName) {
    KubeLiveInfo liveInfo = liveInfos.get(applicationName);
    return liveInfo == null ? Collections.emptyList() : liveInfo.getControllers();
  }

  @Override
  public Iterable<LiveInfo> lookupLive() {
    // Protect against modifications
    return () -> Collections.unmodifiableCollection(liveInfos.values()).stream().map(LiveInfo.class::cast).iterator();
  }

  @Override
  public Cancellable scheduleSecureStoreUpdate(@SuppressWarnings("deprecation") SecureStoreUpdater updater,
                                               long initialDelay, long delay, TimeUnit unit) {
    return () -> { };
  }

  @Override
  public Cancellable setSecureStoreRenewer(SecureStoreRenewer renewer, long initialDelay, long delay, long retryDelay,
                                           TimeUnit unit) {
    return () -> { };
  }

  @Override
  public void start() {
    LOG.error("Starting KubeTwillRunnerService with {} monitor", enableMonitor ? "enabled" : "disabled");
    try {
      apiClient = Config.defaultClient();
      coreV1Api = new CoreV1Api(apiClient);
      if (!enableMonitor) {
        return;
      }
      monitorScheduler = Executors.newSingleThreadScheduledExecutor(
        Threads.createDaemonThreadFactory("kube-monitor-executor"));
      Map<Type, AppResourceWatcherThread<?>> typeMap = ImmutableMap.of(
        V1Deployment.class, AppResourceWatcherThread.createDeploymentWatcher(kubeNamespace, selector),
        V1StatefulSet.class, AppResourceWatcherThread.createStatefulSetWatcher(kubeNamespace, selector),
        V1Job.class, AppResourceWatcherThread.createJobWatcher(kubeNamespace, selector)
      );
      typeMap.values().forEach(watcher -> {
        watcher.addListener(new AppResourceChangeListener<>());
        watcher.start();
      });
      resourceWatchers.put(kubeNamespace, typeMap);

      // start job cleaner service
      jobCleanerService =
        Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("kube-job-cleaner"));
      jobCleanerService.scheduleAtFixedRate(new KubeJobCleaner(new BatchV1Api(apiClient), selector, jobCleanBatchSize),
                                            10, jobCleanupIntervalMins, TimeUnit.MINUTES);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to get Kubernetes API Client", e);
    }
  }

  @Override
  public void stop() {
    if (jobCleanerService != null) {
      jobCleanerService.shutdownNow();
    }
    resourceWatchers.values().forEach(typeMap -> {
      typeMap.values().forEach(AbstractWatcherThread::close);
    });
    if (monitorScheduler != null) {
      monitorScheduler.shutdownNow();
    }
  }

  /**
   * Monitors the given {@link KubeTwillController}.
   *
   * @param liveInfo the {@link KubeLiveInfo} that the controller belongs to
   * @param timeout the start timeout
   * @param timeoutUnit the start timeout unit
   * @param controller the controller top monitor
   * @param <T> the type of the resource to watch
   * @param resourceType resource type being controlled by controller
   * @param startupTaskCompletion startup task completion
   * @return the controller
   */
  private <T extends KubernetesObject> KubeTwillController monitorController(
    KubeLiveInfo liveInfo, long timeout, TimeUnit timeoutUnit, KubeTwillController controller,
    AppResourceWatcherThread<T> watcher, Type resourceType, CompletableFuture<Void> startupTaskCompletion) {

    String runId = controller.getRunId().getId();
    if (!enableMonitor) {
      throw new UnsupportedOperationException(
        String.format("Cannot monitor controller for run %s when monitoring is disabled", runId));
    }

    LOG.debug("Monitoring application {} with run {} starts in {} {}",
              liveInfo.getApplicationName(), runId, timeout, timeoutUnit);

    // Schedule to terminate the controller in the timeout time.
    Future<?> terminationFuture = monitorScheduler.schedule(controller::terminateOnTimeout, timeout, timeoutUnit);

    // This future is for transferring the cancel watch to the change listener
    CompletableFuture<Cancellable> cancellableFuture = new CompletableFuture<>();

    // Listen to resource changes. If the resource represented by the controller has all replicas ready, cancel
    // the terminationFuture. If the resource is deleted, also cancel the terminationFuture, and also terminate
    // the controller as we no longer need to watch for any future changes.
    Cancellable cancellable = watcher.addListener(new ResourceChangeListener<T>() {
      @Override
      public void resourceAdded(T resource) {
        // Handle the same way as modified
        resourceModified(resource);
      }

      @Override
      public void resourceModified(T resource) {
        V1ObjectMeta metadata = resource.getMetadata();

        if (!runId.equals(metadata.getLabels().get(RUN_ID_LABEL))) {
          return;
        }

        if (resourceType.equals(V1Job.class)) {
          // If job has status active we consider it as ready
          if (isJobReady((V1Job) resource)) {
            LOG.debug("Application {} with run {} is available in Kubernetes", liveInfo.getApplicationName(), runId);
            startupTaskCompletion.complete(null);
            // Cancel the scheduled termination
            terminationFuture.cancel(false);
          }

          // If job is in terminal state - success/failure - we consider it as complete.
          if (isJobComplete((V1Job) resource)) {
            // Cancel the watch
            try {
              Uninterruptibles.getUninterruptibly(cancellableFuture).cancel();
            } catch (ExecutionException e) {
              // This will never happen
            }

            controller.setJobStatus(((V1Job) resource).getStatus());
            // terminate the job controller
            controller.terminate();
          }
        } else {
          if (isAllReplicasReady(resource)) {
            LOG.debug("Application {} with run {} is available in Kubernetes", liveInfo.getApplicationName(), runId);
            // Cancel the scheduled termination
            terminationFuture.cancel(false);
            // Cancel the watch
            try {
              Uninterruptibles.getUninterruptibly(cancellableFuture).cancel();
            } catch (ExecutionException e) {
              // This will never happen
            }
          }
        }
      }

      @Override
      public void resourceDeleted(T resource) {
        V1ObjectMeta metadata = resource.getMetadata();

        // If the run is deleted, terminate the controller right away and cancel the scheduled termination
        if (runId.equals(metadata.getLabels().get(RUN_ID_LABEL))) {
          // Cancel the scheduled termination
          terminationFuture.cancel(false);
          // Cancel the watch
          try {
            Uninterruptibles.getUninterruptibly(cancellableFuture).cancel();
          } catch (ExecutionException e) {
            // This will never happen
          }
          controller.terminate();
        }
      }
    });

    cancellableFuture.complete(cancellable);

    // On controller termination, remove it from the liveInfo
    controller.onTerminated(() -> {
      // Cancel the scheduled termination
      terminationFuture.cancel(false);
      // Cancel the watch if there is one
      cancellable.cancel();
      liveInfoLock.lock();
      try {
        liveInfo.removeController(controller);
        if (liveInfo.isEmpty()) {
          liveInfos.remove(liveInfo.getApplicationName(), liveInfo);
        }
      } finally {
        liveInfoLock.unlock();
      }

      if (!resourceType.equals(V1Job.class)) {
        try {
          Uninterruptibles.getUninterruptibly(controller.terminate());
          LOG.debug("Controller for application {} of run {} is terminated", liveInfo.getApplicationName(), runId);
        } catch (ExecutionException e) {
          LOG.error("Controller for application {} of run {} is terminated due to failure",
                    liveInfo.getApplicationName(), runId, e.getCause());
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    return controller;
  }

  /**
   * Returns the application {@link Location} for staging files.
   *
   * @param name name of the twill application
   * @param runId the runId
   * @return the application {@link Location}
   */
  private Location getApplicationLocation(String name, RunId runId) {
    return masterEnvContext.getLocationFactory().create(String.format("twill/%s/%s", name, runId.getId()));
  }

  /**
   * Checks if number of requested replicas is the same as the number of ready replicas in the given resource.
   */
  private boolean isAllReplicasReady(Object resource) {
    try {
      Method getStatus = resource.getClass().getDeclaredMethod("getStatus");
      Object statusObj = getStatus.invoke(resource);

      Method getReplicas = statusObj.getClass().getDeclaredMethod("getReplicas");
      Integer replicas = (Integer) getReplicas.invoke(statusObj);

      Method getReadyReplicas = statusObj.getClass().getDeclaredMethod("getReadyReplicas");
      Integer readyReplicas = (Integer) getReadyReplicas.invoke(statusObj);

      return replicas != null && Objects.equals(replicas, readyReplicas);

    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | ClassCastException e) {
      LOG.warn("Failed to get number of replicas and ready replicas from the resource {}", resource, e);
      return false;
    }
  }

  /**
   * Checks if job is ready.
   */
  private boolean isJobReady(V1Job job) {
    V1JobStatus jobStatus = job.getStatus();
    if (jobStatus != null) {
      Integer active = jobStatus.getActive();
      if (active == null) {
        return false;
      }

      try {
        String labelSelector = job.getMetadata().getLabels().entrySet().stream()
          .map(e -> e.getKey() + "=" + e.getValue())
          .collect(Collectors.joining(","));

        // Make sure at least one pod launched from the job is in active state.
        // https://github.com/kubernetes-client/java/blob/master/kubernetes/docs/V1JobStatus.md
        V1PodList podList = coreV1Api.listNamespacedPod(job.getMetadata().getNamespace(), null, null, null, null,
                                                        labelSelector, null, null, null, null, null);

        for (V1Pod pod : podList.getItems()) {
          if (pod.getStatus() != null && pod.getStatus().getPhase() != null
            && pod.getStatus().getPhase().equalsIgnoreCase("RUNNING")) {
            return true;
          }
        }
      } catch (ApiException e) {
        // If there is an exception while getting active pods for a job, we will use job level status.
        LOG.warn("Error while getting active pods for job {}, {}", job.getMetadata().getName(), e.getResponseBody());
      }
    }
    return false;
  }

  /**
   * Checks if job is complete. Job completion can be in success or failed state.
   */
  private boolean isJobComplete(V1Job job) {
    V1JobStatus jobStatus = job.getStatus();
    if (jobStatus != null) {
      return jobStatus.getFailed() != null || jobStatus.getSucceeded() != null;
    }
    return false;
  }

  /**
   * An {@link ResourceChangeListener} to watch for changes in application resources. It is for refreshing the
   * liveInfos map.
   */
  private final class AppResourceChangeListener<T extends KubernetesObject> implements ResourceChangeListener<T> {

    @Override
    public void resourceAdded(T resource) {
      V1ObjectMeta metadata = resource.getMetadata();
      String appName = metadata.getAnnotations().get(APP_LABEL);
      if (appName == null) {
        // This shouldn't happen. Just to guard against future bug.
        return;
      }
      String resourceName = metadata.getName();
      RunId runId = RunIds.fromString(metadata.getLabels().get(RUN_ID_LABEL));

      // Read the start timeout millis from the annotation. It is set by the KubeTwillPreparer
      long startTimeoutMillis = TimeUnit.SECONDS.toMillis(120);
      try {
        startTimeoutMillis = Long.parseLong(metadata.getAnnotations().get(START_TIMEOUT_ANNOTATION));
      } catch (Exception e) {
        // This shouldn't happen
        LOG.warn("Failed to get start timeout from the annotation using key {} from resource {}. Defaulting to {} ms",
                 START_TIMEOUT_ANNOTATION, metadata.getName(), startTimeoutMillis, e);
      }

      // Add the LiveInfo and Controller
      liveInfoLock.lock();
      try {
        KubeLiveInfo liveInfo = liveInfos.computeIfAbsent(appName, k -> new KubeLiveInfo(resource.getClass(), appName));
        KubeTwillController controller = createKubeTwillController(appName, runId, resource.getClass(), metadata);
        liveInfo.addControllerIfAbsent(runId, startTimeoutMillis, TimeUnit.MILLISECONDS, controller, metadata);
      } finally {
        liveInfoLock.unlock();
      }
    }

    @Override
    public void resourceDeleted(T resource) {
      V1ObjectMeta metadata = resource.getMetadata();
      String appName = metadata.getAnnotations().get(APP_LABEL);
      if (appName == null) {
        // This shouldn't happen. Just to guard against future bug.
        return;
      }

      // Get and terminate the controller
      liveInfoLock.lock();
      try {
        KubeLiveInfo liveInfo = liveInfos.get(appName);
        if (liveInfo != null) {
          RunId runId = RunIds.fromString(metadata.getLabels().get(RUN_ID_LABEL));
          if (!liveInfo.resourceType.equals(V1Job.class)) {
            Optional.ofNullable(liveInfo.getController(runId)).ifPresent(KubeTwillController::terminate);
          }
        }
      } finally {
        liveInfoLock.unlock();
      }
    }
  }

  /**
   * Creates a {@link KubeTwillController}.
   */
  private KubeTwillController createKubeTwillController(String appName, RunId runId,
                                                        Type resourceType, V1ObjectMeta meta) {
    CompletableFuture<Void> startupTaskCompletion = new CompletableFuture<>();
    KubeTwillController controller = new KubeTwillController(meta.getNamespace(), runId, discoveryServiceClient,
                                                             apiClient, resourceType, meta, startupTaskCompletion);

    Location appLocation = getApplicationLocation(appName, runId);
    controller.onTerminated(() -> {
      try {
        appLocation.delete(true);
      } catch (IOException e) {
        throw new RuntimeException("Failed to delete location for " + appName + "-" + runId + " at " + appLocation, e);
      }
    }, command -> new Thread(command, "app-cleanup-" + appName).start());
    return controller;
  }

  /**
   * A TwillApplication with a single runnable.
   */
  private static class SingleRunnableApplication implements TwillApplication {

    private final TwillRunnable runnable;
    private final ResourceSpecification resourceSpec;

    SingleRunnableApplication(TwillRunnable runnable, ResourceSpecification resourceSpec) {
      this.runnable = runnable;
      this.resourceSpec = resourceSpec;
    }

    @Override
    public TwillSpecification configure() {
      TwillRunnableSpecification runnableSpec = runnable.configure();
      return TwillSpecification.Builder.with()
        .setName(runnableSpec.getName())
        .withRunnable().add(runnableSpec.getName(), runnable, resourceSpec)
        .noLocalFiles()
        .anyOrder()
        .build();
    }
  }

  /**
   * Kubernetes LiveInfo.
   */
  private final class KubeLiveInfo implements LiveInfo {
    private final Type resourceType;
    private final String applicationName;
    private final Map<String, KubeTwillController> controllers;

    KubeLiveInfo(Type resourceType, String applicationName) {
      this.resourceType = resourceType;
      this.applicationName = applicationName;
      this.controllers = new ConcurrentSkipListMap<>();
    }

    KubeTwillController addControllerIfAbsent(RunId runId, long timeout, TimeUnit timeoutUnit,
                                              KubeTwillController controller, V1ObjectMeta meta) {
      KubeTwillController existing = controllers.putIfAbsent(runId.getId(), controller);
      if (existing != null) {
        return existing;
      }
      String namespace = meta.getNamespace();
      // If it is newly added controller, monitor it.
      if (!resourceWatchers.containsKey(namespace)) {
        addAndStartJobWatcher(namespace);
      }
      return monitorController(this, timeout, timeoutUnit, controller,
                               resourceWatchers.get(namespace).get(resourceType),
                               resourceType, controller.getStartedFuture());
    }

    /**
     * Create and start job watcher for the given Kubernetes namespace
     */
    private void addAndStartJobWatcher(String namespace) {
      LOG.info("Adding job watcher for namespace {}", namespace);
      AppResourceWatcherThread<?> watcherThread = AppResourceWatcherThread.createJobWatcher(namespace, selector);
      watcherThread.addListener(new AppResourceChangeListener<>());
      watcherThread.start();
      resourceWatchers.put(namespace, ImmutableMap.of(V1Job.class, watcherThread));
    }

    /**
     * Remove the given controller instance.
     * @param controller the instance to remove
     */
    void removeController(KubeTwillController controller) {
      controllers.remove(controller.getRunId().getId(), controller);
    }

    boolean isEmpty() {
      return controllers.isEmpty();
    }

    @Override
    public String getApplicationName() {
      return applicationName;
    }

    @Override
    public Iterable<TwillController> getControllers() {
      // Protect against modifications
      return () -> Collections.unmodifiableCollection(controllers.values())
        .stream().map(TwillController.class::cast).iterator();
    }

    /**
     * Returns the {@link KubeTwillController} for the given {@link RunId}.
     */
    @Nullable
    KubeTwillController getController(RunId runId) {
      return controllers.get(runId.getId());
    }
  }

}
