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

import com.google.common.util.concurrent.Uninterruptibles;
import io.cdap.cdap.k8s.common.ResourceChangeListener;
import io.cdap.cdap.master.environment.k8s.PodInfo;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.models.V1Deployment;
import io.kubernetes.client.models.V1DeploymentCondition;
import io.kubernetes.client.models.V1DeploymentStatus;
import io.kubernetes.client.models.V1ObjectMeta;
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
import org.apache.twill.internal.RunIds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import javax.annotation.Nullable;

/**
 * Kubernetes version of a TwillRunner.
 *
 * All resources created by the runner will have a name of the form:
 *
 * [cleansed app name]-[runid]
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

  private static final String APP_ANNOTATION = "cdap.twill.app";
  private static final String APP_LABEL = "cdap.twill.app";
  private static final String RUNNER_LABEL = "cdap.twill.runner";
  private static final String RUNNER_LABEL_VAL = "k8s";
  private static final String RUN_ID_LABEL = "cdap.twill.run.id";

  private static final String AVAILABLE_TYPE = "Available";

  private final String kubeNamespace;
  private final String resourcePrefix;
  private final Map<String, String> cConf;
  private final PodInfo podInfo;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final Map<String, String> extraLabels;
  private final DeploymentWatcher deploymentWatcher;
  private final Map<String, KubeLiveInfo> liveInfos;
  private final Lock liveInfoLock;
  private ScheduledExecutorService monitorScheduler;
  private ApiClient apiClient;

  public KubeTwillRunnerService(String kubeNamespace, DiscoveryServiceClient discoveryServiceClient,
                                PodInfo podInfo, String resourcePrefix, Map<String, String> cConf,
                                Map<String, String> extraLabels) {
    this.kubeNamespace = kubeNamespace;
    this.podInfo = podInfo;
    this.resourcePrefix = resourcePrefix;
    this.cConf = cConf;
    this.discoveryServiceClient = discoveryServiceClient;
    this.extraLabels = Collections.unmodifiableMap(new HashMap<>(extraLabels));
    // Selects all runs start by the k8s twill runner that has the run id label
    this.deploymentWatcher = new DeploymentWatcher(kubeNamespace, String.format("%s=%s,%s",
                                                                                RUNNER_LABEL, RUNNER_LABEL_VAL,
                                                                                RUN_ID_LABEL));
    this.liveInfos = new ConcurrentSkipListMap<>();
    this.liveInfoLock = new ReentrantLock();
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
    Map<String, String> labels = new HashMap<>(extraLabels);
    labels.put(RUNNER_LABEL, RUNNER_LABEL_VAL);
    labels.put(APP_LABEL, asLabel(spec.getName()));
    labels.put(RUN_ID_LABEL, runId.getId());
    V1ObjectMeta resourceMeta = new V1ObjectMeta();
    resourceMeta.setOwnerReferences(podInfo.getOwnerReferences());
    resourceMeta.setName(getName(spec.getName(), runId));
    resourceMeta.setLabels(labels);
    // labels have more strict requirements around valid character sets,
    // so use annotations to store the app name.
    resourceMeta.setAnnotations(Collections.singletonMap(APP_ANNOTATION, spec.getName()));
    return new KubeTwillPreparer(apiClient, kubeNamespace, podInfo,
                                 spec, runId, resourceMeta, cConf, (timeout, timeoutUnit) -> {
      // Adds the controller to the LiveInfo.
      liveInfoLock.lock();
      try {
        KubeLiveInfo liveInfo = liveInfos.computeIfAbsent(spec.getName(), KubeLiveInfo::new);
        KubeTwillController controller = new KubeTwillController(resourceMeta.getName(), kubeNamespace,
                                                                 runId, apiClient, discoveryServiceClient);
        return liveInfo.addControllerIfAbsent(runId, timeout, timeoutUnit, controller);
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
  public Cancellable scheduleSecureStoreUpdate(SecureStoreUpdater updater, long initialDelay, long delay,
                                               TimeUnit unit) {
    return () -> { };
  }

  @Override
  public Cancellable setSecureStoreRenewer(SecureStoreRenewer renewer, long initialDelay, long delay, long retryDelay,
                                           TimeUnit unit) {
    return () -> { };
  }

  @Override
  public void start() {
    try {
      apiClient = Config.defaultClient();
      monitorScheduler = Executors.newSingleThreadScheduledExecutor(
        Threads.createDaemonThreadFactory("kube-monitor-executor"));
      deploymentWatcher.addListener(new DeploymentListener());
      deploymentWatcher.start();
    } catch (IOException e) {
      throw new IllegalStateException("Unable to get Kubernetes API Client", e);
    }
  }

  @Override
  public void stop() {
    deploymentWatcher.close();
    monitorScheduler.shutdownNow();
  }

  /**
   * Monitors the given {@link KubeTwillController}.
   *
   * @param liveInfo the {@link KubeLiveInfo} that the controller belongs to
   * @param timeout the start timeout
   * @param timeoutUnit the start timeout unit
   * @param controller the controller top monitor.
   * @return the controller
   */
  private KubeTwillController monitorController(KubeLiveInfo liveInfo, long timeout,
                                                TimeUnit timeoutUnit, KubeTwillController controller) {
    String runId = controller.getRunId().getId();

    LOG.debug("Monitoring application {} with run {} starts in {} {}",
              liveInfo.getApplicationName(), runId, timeout, timeoutUnit);

    // Schedule to terminate the controller in the timeout time.
    Future<?> terminationFuture = monitorScheduler.schedule(controller::terminate, timeout, timeoutUnit);

    // This future is for transferring the cancel watch to the change listener
    CompletableFuture<Cancellable> cancellableFuture = new CompletableFuture<>();

    // Listen to deployment changes. If the deployment represented by the controller changed to available, cancel
    // the terminationFuture. If the deployment is deleted, also cancel the terminationFuture, and also terminate
    // the controller.
    Cancellable cancellable = deploymentWatcher.addListener(new ResourceChangeListener<V1Deployment>() {
      @Override
      public void resourceAdded(V1Deployment deployment) {
        // Handle the same way as modified
        resourceModified(deployment);
      }

      @Override
      public void resourceModified(V1Deployment deployment) {
        if (runId.equals(deployment.getMetadata().getLabels().get(RUN_ID_LABEL))) {
          V1DeploymentStatus status = deployment.getStatus();
          if (status == null) {
            return;
          }
          List<V1DeploymentCondition> conditions = status.getConditions();
          if (conditions == null) {
            return;
          }
          boolean available = conditions.stream()
            .filter(c -> AVAILABLE_TYPE.equals(c.getType()))
            .map(V1DeploymentCondition::getStatus)
            .findFirst()
            .map(Boolean::parseBoolean)
            .orElse(false);

          if (available) {
            LOG.debug("Deployment for application {} with run {} is available", liveInfo.getApplicationName(), runId);
            // Cancel the scheduled termination
            terminationFuture.cancel(false);
            // Cancel the watch
            try {
              Uninterruptibles.getUninterruptibly(cancellableFuture).cancel();
            } catch (ExecutionException e) {
              // This never happen
            }
          }
        }
      }

      @Override
      public void resourceDeleted(V1Deployment deployment) {
        // If the run is deleted, terminate the controller right away and cancel the scheduled termination
        if (runId.equals(deployment.getMetadata().getLabels().get(RUN_ID_LABEL))) {
          // Cancel the scheduled termination
          terminationFuture.cancel(false);
          controller.terminate();
          // Cancel the watch
          try {
            Uninterruptibles.getUninterruptibly(cancellableFuture).cancel();
          } catch (ExecutionException e) {
            // This never happen
          }
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
      LOG.info("Controller for application {} of run {} is terminated", liveInfo.getApplicationName(), runId);
    }, Threads.SAME_THREAD_EXECUTOR);

    return controller;
  }

  /**
   * A {@link ResourceChangeListener} to watch for changes in all deployments. It is for refreshing the
   * liveInfos map.
   */
  private final class DeploymentListener implements ResourceChangeListener<V1Deployment> {

    @Override
    public void resourceAdded(V1Deployment deployment) {
      String appName = deployment.getMetadata().getAnnotations().get(APP_ANNOTATION);
      if (appName == null) {
        // This shouldn't happen. Just to guard against future bug.
        return;
      }
      String deploymentName = deployment.getMetadata().getName();
      RunId runId = RunIds.fromString(deployment.getMetadata().getLabels().get(RUN_ID_LABEL));

      // Read the start timeout millis from the annotation. It is set by the KubeTwillPreparer
      long startTimeoutMillis = TimeUnit.SECONDS.toMillis(120);
      try {
        startTimeoutMillis = Long.parseLong(deployment.getMetadata().getAnnotations().get(START_TIMEOUT_ANNOTATION));
      } catch (Exception e) {
        // This shouldn't happen
        LOG.warn("Failed to get start timeout from the deployment annotation using key {}. Defaulting to {} ms",
                 START_TIMEOUT_ANNOTATION, startTimeoutMillis, e);
      }

      // Add the LiveInfo and Controller
      liveInfoLock.lock();
      try {
        KubeLiveInfo liveInfo = liveInfos.computeIfAbsent(appName, KubeLiveInfo::new);
        KubeTwillController controller = new KubeTwillController(deploymentName, kubeNamespace,
                                                                 runId, apiClient, discoveryServiceClient);
        liveInfo.addControllerIfAbsent(runId, startTimeoutMillis, TimeUnit.MILLISECONDS, controller);
      } finally {
        liveInfoLock.unlock();
      }
    }

    @Override
    public void resourceDeleted(V1Deployment deployment) {
      String appName = deployment.getMetadata().getAnnotations().get(APP_ANNOTATION);
      if (appName == null) {
        // This shouldn't happen. Just to guard against future bug.
        return;
      }

      // Get and terminate the controller
      liveInfoLock.lock();
      try {
        KubeLiveInfo liveInfo = liveInfos.get(appName);
        if (liveInfo != null) {
          RunId runId = RunIds.fromString(deployment.getMetadata().getLabels().get(RUN_ID_LABEL));
          Optional.ofNullable(liveInfo.getController(runId)).ifPresent(KubeTwillController::terminate);
        }
      } finally {
        liveInfoLock.unlock();
      }
    }
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
   * Kubernetes names can be at max 253 characters and only contain lowercase alphanumeric, '-', and '.'.
   * This will lowercase the twill name, and append -[runid] to the end. If the result would be longer than 253
   * characters, the twill name is truncated.
   */
  private String getName(String applicationName, RunId runId) {
    String suffix = "-" + runId.getId();
    return resourcePrefix + cleanse(applicationName, 253 - suffix.length() - resourcePrefix.length()) + suffix;
  }

  /**
   * label values must be 63 characters or less and consist of alphanumeric, '.', '_', or '-'.
   */
  private String asLabel(String val) {
    return cleanse(val, 63);
  }

  /**
   * Kubernetes names must be lower case alphanumeric, or '-'. Some are less restrictive, but those characters
   * should always be ok.
   */
  private String cleanse(String val, int maxLength) {
    String cleansed = val.replaceAll("[^A-Za-z0-9\\-]", "-");
    return cleansed.length() > maxLength ? cleansed.substring(0, maxLength) : cleansed;
  }

  /**
   * Kubernetes LiveInfo.
   */
  private final class KubeLiveInfo implements LiveInfo {
    private final String applicationName;
    private final Map<String, KubeTwillController> controllers;

    KubeLiveInfo(String applicationName) {
      this.applicationName = applicationName;
      this.controllers = new ConcurrentSkipListMap<>();
    }

    KubeTwillController addControllerIfAbsent(RunId runId, long timeout, TimeUnit timeoutUnit,
                                              KubeTwillController controller) {
      KubeTwillController existing = controllers.putIfAbsent(runId.getId(), controller);
      if (existing != null) {
        return existing;
      }
      // If it is newly added controller, monitor it.
      return monitorController(this, timeout, timeoutUnit, controller);
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
