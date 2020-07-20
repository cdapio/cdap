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
import io.kubernetes.client.models.V1DeleteOptions;
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
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import javax.annotation.Nullable;

/**
 * Kubernetes version of a TwillController.
 */
class KubeTwillController implements TwillController {

  private final String name;
  private final String kubeNamespace;
  private final RunId runId;
  private final ApiClient apiClient;
  private final CompletableFuture<KubeTwillController> completion;
  private final AtomicBoolean terminateCalled;
  private final DiscoveryServiceClient discoveryServiceClient;

  KubeTwillController(String name, String kubeNamespace, RunId runId, ApiClient apiClient,
                      DiscoveryServiceClient discoveryServiceClient, Location appLocation) {
    this.name = name;
    this.kubeNamespace = kubeNamespace;
    this.runId = runId;
    this.apiClient = apiClient;
    this.completion = new CompletableFuture<>();
    this.terminateCalled = new AtomicBoolean();
    this.discoveryServiceClient = discoveryServiceClient;

    completion.whenComplete((kubeTwillController, throwable) -> {
      // Cleanup the app location when the program completed
      try {
        appLocation.delete(true);
      } catch (IOException e) {
        throw new RuntimeException("Failed to delete location for " + name + "-" + runId + " at " + appLocation, e);
      }
    });
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
    throw new UnsupportedOperationException("Change instances is currently not supported");
  }

  @Nullable
  @Override
  public ResourceReport getResourceReport() {
    return null;
  }

  @Override
  public Future<String> restartAllInstances(String runnable) {
    throw new UnsupportedOperationException("restartAllInstances is currently not supported");
  }

  @Override
  public Future<Set<String>> restartInstances(Map<String, ? extends Set<Integer>> runnableToInstanceIds) {
    throw new UnsupportedOperationException("restartInstances is currently not supported");
  }

  @Override
  public Future<String> restartInstances(String runnable, int instanceId, int... moreInstanceIds) {
    throw new UnsupportedOperationException("restartInstances is currently not supported");
  }

  @Override
  public Future<String> restartInstances(String runnable, Set<Integer> instanceIds) {
    throw new UnsupportedOperationException("restartInstances is currently not supported");
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
    if (terminateCalled.compareAndSet(false, true)) {
      // delete the deployment, then delete the config map
      try {
        V1DeleteOptions deleteOptions = new V1DeleteOptions();
        AppsV1Api appsApi = new AppsV1Api(apiClient);

        // callback for the delete deployment call
        ApiCallback<V1Status> deleteDeploymentCallback = new CallbackAdapter(
          completion::completeExceptionally, () -> completion.complete(KubeTwillController.this));
        appsApi.deleteNamespacedDeploymentAsync(name, kubeNamespace, null, deleteOptions,
                                                null, null, null, null, deleteDeploymentCallback);
      } catch (ApiException e) {
        completion.completeExceptionally(e);
      }
    }
    return completion;
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
   * Allows more succinct creation of ApiCallback.
   */
  private static class CallbackAdapter implements ApiCallback<V1Status> {
    private final Consumer<ApiException> onFailure;
    private final Runnable onSuccess;

    CallbackAdapter(Consumer<ApiException> onFailure, Runnable onSuccess) {
      this.onFailure = onFailure;
      this.onSuccess = onSuccess;
    }

    @Override
    public void onFailure(ApiException e, int i, Map<String, List<String>> map) {
      onFailure.accept(e);
    }

    @Override
    public void onSuccess(V1Status v1Status, int i, Map<String, List<String>> map) {
      onSuccess.run();
    }

    @Override
    public void onUploadProgress(long l, long l1, boolean b) {
      // no-op
    }

    @Override
    public void onDownloadProgress(long l, long l1, boolean b) {
      // no-op
    }
  }
}
