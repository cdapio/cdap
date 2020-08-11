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

import io.kubernetes.client.ApiCallback;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.AppsV1Api;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1DeleteOptions;
import io.kubernetes.client.models.V1Deployment;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1Preconditions;
import io.kubernetes.client.models.V1StatefulSet;
import io.kubernetes.client.models.V1Status;
import org.apache.twill.api.Command;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceController;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;

/**
 * Kubernetes version of a TwillController.
 */
class KubeTwillController implements TwillController {
  private static final Logger LOG = LoggerFactory.getLogger(KubeTwillController.class);

  private final String kubeNamespace;
  private final RunId runId;
  private final CompletableFuture<KubeTwillController> completion;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final ApiClient apiClient;
  private final Type resourceType;
  private final V1ObjectMeta meta;

  KubeTwillController(String kubeNamespace, RunId runId, DiscoveryServiceClient discoveryServiceClient,
                      ApiClient apiClient, Type resourceType, V1ObjectMeta meta) {
    this.kubeNamespace = kubeNamespace;
    this.runId = runId;
    this.completion = new CompletableFuture<>();
    this.discoveryServiceClient = discoveryServiceClient;
    this.apiClient = apiClient;
    this.resourceType = resourceType;
    this.meta = meta;
  }

  @Override
  public void addLogHandler(LogHandler handler) {
    // no-op
  }

  @Override
  public ServiceDiscovered discoverService(String serviceName) {
    return discoveryServiceClient.discover(serviceName);
  }

  @Override
  public Future<Integer> changeInstances(String runnable, int newCount) {
    // TODO: implement with AppsV1Api.patchNamespacedDeploymentAsync()
    CompletableFuture<Integer> f = new CompletableFuture<>();
    f.completeExceptionally(new UnsupportedOperationException("Change instances is currently not supported"));
    return f;
  }

  @Nullable
  @Override
  public ResourceReport getResourceReport() {
    return null;
  }

  @Override
  public Future<String> restartAllInstances(String runnable) {
    CompletableFuture<String> resultFuture = new CompletableFuture<>();
    CoreV1Api api = new CoreV1Api(apiClient);

    try {
      api.deleteCollectionNamespacedPodAsync(kubeNamespace, null, null, null, getLabelSelector(), null, null,
                                             null, null, createCallbackFutureAdapter(resultFuture, r -> runnable));
    } catch (ApiException e) {
      resultFuture.completeExceptionally(e);
    }

    return resultFuture;
  }

  @Override
  public Future<Set<String>> restartInstances(Map<String, ? extends Set<Integer>> runnableToInstanceIds) {
    CompletableFuture<Set<String>> future = new CompletableFuture<>();

    // Since we only support one TwillRunnable, the runnable name is ignored
    if (runnableToInstanceIds.isEmpty()) {
      future.complete(Collections.emptySet());
      return future;
    }
    if (runnableToInstanceIds.size() != 1) {
      future.completeExceptionally(new UnsupportedOperationException("Only one runnable is supported"));
      return future;
    }
    Map.Entry<String, ? extends Set<Integer>> entry = runnableToInstanceIds.entrySet().iterator().next();
    return doRestartInstances(entry.getKey(), entry.getValue()).thenApply(s -> Collections.singleton(entry.getKey()));
  }

  @Override
  public Future<String> restartInstances(String runnable, int instanceId, int... moreInstanceIds) {
    return restartInstances(runnable,
                            IntStream.concat(IntStream.of(instanceId),
                                             IntStream.of(moreInstanceIds)).boxed().collect(Collectors.toSet()));
  }

  @Override
  public Future<String> restartInstances(String runnable, Set<Integer> instanceIds) {
    return doRestartInstances(runnable, instanceIds);
  }

  @Override
  public Future<Map<String, LogEntry.Level>> updateLogLevels(Map<String, LogEntry.Level> logLevels) {
    CompletableFuture<Map<String, LogEntry.Level>> f = new CompletableFuture<>();
    f.completeExceptionally(new UnsupportedOperationException("updateLogLevels is currently not supported"));
    return f;
  }

  @Override
  public Future<Map<String, LogEntry.Level>> updateLogLevels(String runnableName,
                                                             Map<String, LogEntry.Level> logLevelsForRunnable) {
    CompletableFuture<Map<String, LogEntry.Level>> f = new CompletableFuture<>();
    f.completeExceptionally(new UnsupportedOperationException("updateLogLevels is currently not supported"));
    return f;
  }

  @Override
  public Future<String[]> resetLogLevels(String... loggerNames) {
    CompletableFuture<String[]> f = new CompletableFuture<>();
    f.completeExceptionally(new UnsupportedOperationException("resetLogLevels is currently not supported"));
    return f;
  }

  @Override
  public Future<String[]> resetRunnableLogLevels(String runnableName, String... loggerNames) {
    CompletableFuture<String[]> f = new CompletableFuture<>();
    f.completeExceptionally(new UnsupportedOperationException("resetLogLevels is currently not supported"));
    return f;
  }

  @Override
  public RunId getRunId() {
    return runId;
  }

  @Override
  public Future<Command> sendCommand(Command command) {
    CompletableFuture<Command> f = new CompletableFuture<>();
    f.completeExceptionally(new UnsupportedOperationException("sendCommand is currently not supported"));
    return f;
  }

  @Override
  public Future<Command> sendCommand(String runnableName, Command command) {
    CompletableFuture<Command> f = new CompletableFuture<>();
    f.completeExceptionally(new UnsupportedOperationException("sendCommand is currently not supported"));
    return f;
  }

  @Override
  public Future<? extends ServiceController> terminate() {
    if (completion.isDone()) {
      return completion;
    }

    CompletableFuture<KubeTwillController> resultFuture = new CompletableFuture<>();

    // delete the resource
    deleteResource().whenComplete((name, t) -> {
      if (t != null) {
        resultFuture.completeExceptionally(t);
      } else {
        resultFuture.complete(KubeTwillController.this);
      }
    });
    // If the deletion was successful, reflect the status into the completion future
    // If the deletion was failed, don't change the completion state so that caller can retry
    resultFuture.thenAccept(completion::complete);
    return resultFuture;
  }

  @Override
  public void kill() {
    terminate();
  }

  @Override
  public void onRunning(Runnable runnable, Executor executor) {
    executor.execute(runnable);
  }

  @Override
  public void onTerminated(Runnable runnable, Executor executor) {
    completion.whenCompleteAsync((controller, throwable) -> runnable.run(), executor);
  }

  @Override
  public void awaitTerminated() throws ExecutionException {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          completion.get();
          return;
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void awaitTerminated(long timeout, TimeUnit timeoutUnit) throws TimeoutException, ExecutionException {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          completion.get(timeout, timeoutUnit);
          return;
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Nullable
  @Override
  public TerminationStatus getTerminationStatus() {
    if (!completion.isDone()) {
      return null;
    }
    try {
      awaitTerminated();
      return TerminationStatus.SUCCEEDED;
    } catch (ExecutionException e) {
      return TerminationStatus.FAILED;
    }
  }

  /**
   * Creates a label selector that matches all labels in the meta object.
   */
  private String getLabelSelector() {
    return meta.getLabels().entrySet().stream()
      .map(e -> String.format("%s=%s", e.getKey(), e.getValue()))
      .collect(Collectors.joining(","));
  }

  /**
   * Restarts the given set of instances. This method currently only supports restart of stateful set.
   *
   * @param runnable name of the runnable to restart. This is currently unused
   * @param instanceIds the set of instance ids to restart
   * @return a {@link CompletableFuture} that will complete when the delete operation completed
   */
  private CompletableFuture<String> doRestartInstances(String runnable, Set<Integer> instanceIds) {
    CompletableFuture<String> resultFuture = new CompletableFuture<>();
    CoreV1Api api = new CoreV1Api(apiClient);

    // Only support for stateful set for now
    if (!V1StatefulSet.class.equals(resourceType)) {
      resultFuture.completeExceptionally(
        new UnsupportedOperationException("Instance restart by instance id is only supported for statefulsets"));
      return resultFuture;
    }

    if (instanceIds.size() > 1) {
      resultFuture.completeExceptionally(
        new UnsupportedOperationException("Only one instance can be restarted at a time."));
      return resultFuture;
    }

    int instanceId = instanceIds.iterator().next();
    // pod uid is passed as "runnable" parameter to the method.
    V1DeleteOptions deleteOptions = new V1DeleteOptions().preconditions(new V1Preconditions().uid(runnable));

    String podName = String.format("%s-%d", meta.getName(), instanceId);
    LOG.debug("Deleting pod with name {}", podName);

    try {
      api.deleteNamespacedPodAsync(podName, kubeNamespace, null, deleteOptions, null, null, null, null,
                                   createCallbackFutureAdapter(resultFuture, r -> runnable));
    } catch (ApiException e) {
      resultFuture.completeExceptionally(e);
    }
    return resultFuture;
  }

  /**
   * Creates a {@link ApiCallback} with the callback result adapted back to the provided {@link CompletableFuture}.
   *
   * @param <T> type of the result
   * @param <R> type of the result for the future
   */
  private <T, R> ApiCallback<T> createCallbackFutureAdapter(CompletableFuture<R> future, Function<T, R> func) {
    return new ApiCallbackAdapter<T>() {
      @Override
      public void onFailure(ApiException e, int statusCode, Map responseHeaders) {
        future.completeExceptionally(e);
      }

      @Override
      public void onSuccess(T result, int statusCode, Map<String, List<String>> responseHeaders) {
        future.complete(func.apply(result));
      }
    };
  }

  /**
   * Deletes the underlying resource in k8s represented by this controller.
   *
   * @return a {@link CompletionStage} that will complete when the delete operation completed
   */
  private CompletionStage<String> deleteResource() {
    if (V1Deployment.class.equals(resourceType)) {
      return deleteDeployment();
    }
    if (V1StatefulSet.class.equals(resourceType)) {
      return deleteStatefulSet();
    }
    // This shouldn't happen
    throw new UnsupportedOperationException("Cannot delete resource of type " + resourceType);
  }

  /**
   * Deletes the deployment controlled by this controller asynchronously.
   *
   * @return a {@link CompletionStage} that will complete when the delete operation completed
   */
  private CompletionStage<String> deleteDeployment() {
    AppsV1Api appsApi = new AppsV1Api(apiClient);

    // callback for the delete deployment call
    CompletableFuture<String> result = new CompletableFuture<>();
    try {
      String name = meta.getName();
      appsApi.deleteNamespacedDeploymentAsync(name, kubeNamespace, null, new V1DeleteOptions(),
                                              null, null, null, null, new ApiCallbackAdapter<V1Status>() {
          @Override
          public void onFailure(ApiException e, int statusCode, Map<String, List<String>> responseHeaders) {
            // Ignore the failure if the deployment is already deleted
            if (statusCode == 404) {
              result.complete(name);
            } else {
              result.completeExceptionally(e);
            }
          }

          @Override
          public void onSuccess(V1Status v1Status, int statusCode, Map<String, List<String>> responseHeaders) {
            result.complete(name);
          }
        });
    } catch (ApiException e) {
      result.completeExceptionally(e);
    }
    return result;
  }

  /**
   * Deletes the stateful set controlled by this controller asynchronously.

   * @return a {@link CompletionStage} that will complete when the delete operation completed
   */
  private CompletionStage<String> deleteStatefulSet() {
    AppsV1Api appsApi = new AppsV1Api(apiClient);
    CoreV1Api coreApi = new CoreV1Api(apiClient);

    // callback for the delete deployment call
    CompletableFuture<String> resultFuture = new CompletableFuture<>();
    try {
      String name = meta.getName();
      appsApi.deleteNamespacedStatefulSetAsync(name, kubeNamespace, null, new V1DeleteOptions(),
                                               null, null, null, null, new ApiCallbackAdapter<V1Status>() {
          @Override
          public void onFailure(ApiException e, int statusCode, Map<String, List<String>> responseHeaders) {
            // Ignore if the stateful set is already deleted
            if (statusCode == 404) {
              deletePVCs();
            } else {
              resultFuture.completeExceptionally(e);
            }
          }

          @Override
          public void onSuccess(V1Status v1Status, int statusCode, Map<String, List<String>> responseHeaders) {
            deletePVCs();
          }

          // Delete the PVCs used by the stateful set
          private void deletePVCs() {
            try {
              coreApi.deleteCollectionNamespacedPersistentVolumeClaimAsync(
                kubeNamespace, null, null, null, getLabelSelector(), null, null, null, false,
                createCallbackFutureAdapter(resultFuture, r -> name));
            } catch (ApiException e) {
              resultFuture.completeExceptionally(e);
            }
          }
        });

    } catch (ApiException e) {
      resultFuture.completeExceptionally(e);
    }
    return resultFuture;
  }
}
