/*
 * Copyright © 2021-2023 Cask Data, Inc.
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

package io.cdap.cdap.common.internal.remote;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Singleton;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.TaskWorker;
import io.cdap.cdap.common.utils.GcpMetadataTaskContextUtil;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.BodyProducer;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal {@link HttpHandler} for Task worker.
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3 + "/worker")
public class TaskWorkerHttpHandlerInternal extends AbstractHttpHandler {

  /**
   * Fraction of duration which will be used for calculating a range.
   */
  private static final double DURATION_FRACTION = 0.1;
  private static final Logger LOG = LoggerFactory.getLogger(
      TaskWorkerHttpHandlerInternal.class);
  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(
      BasicThrowable.class, new BasicThrowableCodec()).create();

  private final RunnableTaskLauncher runnableTaskLauncher;
  private final BiConsumer<Boolean, TaskDetails> taskCompletionConsumer;

  /**
   * Holds the total number of requests that have been executed by this handler
   * that should count toward max allowed.
   */
  private final AtomicInteger runningRequestCount = new AtomicInteger(0);
  private final AtomicInteger requestProcessedCount = new AtomicInteger(0);

  private final String metadataServiceEndpoint;
  private final MetricsCollectionService metricsCollectionService;
  private final CConfiguration cConf;

  /**
   * If true, pod will restart once an operation finish its execution.
   */
  private final AtomicBoolean mustRestart = new AtomicBoolean(false);
  private final int concurrentRequestLimit;

  /** Owns the namespace lease and credential; null unless running behind the task worker proxy. */
  @Nullable
  private final StickyLeaseManager stickyLeaseManager;

  /**
   * Constructs the {@link TaskWorkerHttpHandlerInternal}.
   */
  public TaskWorkerHttpHandlerInternal(CConfiguration cConf,
      DiscoveryService discoveryService,
      DiscoveryServiceClient discoveryServiceClient, Consumer<String> stopper,
      MetricsCollectionService metricsCollectionService) {
    this(cConf, new RunnableTaskLauncher(cConf, discoveryService, discoveryServiceClient,
        metricsCollectionService), stopper, metricsCollectionService);
  }

  /** Constructs the handler around an already built launcher, so tests can inject failures. */
  @VisibleForTesting
  TaskWorkerHttpHandlerInternal(CConfiguration cConf,
      RunnableTaskLauncher runnableTaskLauncher, Consumer<String> stopper,
      MetricsCollectionService metricsCollectionService) {
    this.cConf = cConf;
    final int killAfterRequestCount = cConf.getInt(
        Constants.TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 0);
    this.runnableTaskLauncher = runnableTaskLauncher;
    this.metricsCollectionService = metricsCollectionService;
    this.metadataServiceEndpoint = cConf.get(
        Constants.TaskWorker.METADATA_SERVICE_END_POINT);
    boolean enableUserCodeIsolationEnabled = cConf.getBoolean(
        TaskWorker.USER_CODE_ISOLATION_ENABLED);
    boolean stickyLeaseEnabled = TaskWorkerManager.isEnabled(cConf);

    // Isolation without the proxy runs one task at a time, since nothing coordinates the shared
    // credential. With the proxy, the lease does, so the configured limit applies.
    if (enableUserCodeIsolationEnabled && !stickyLeaseEnabled) {
      this.concurrentRequestLimit = 1;
    } else {
      this.concurrentRequestLimit = cConf.getInt(TaskWorker.REQUEST_LIMIT);
    }

    this.stickyLeaseManager = stickyLeaseEnabled
        ? new StickyLeaseManager(concurrentRequestLimit, new SidecarCredentialContext())
        : null;

    // Restart the service to clean up and re-claim resources after user code
    // execution.
    this.taskCompletionConsumer = (succeeded, taskDetails) -> {
      taskDetails.emitMetrics(succeeded);
      final int pendingRequests = runningRequestCount.decrementAndGet();
      requestProcessedCount.incrementAndGet();

      if (stickyLeaseManager != null) {
        // Released after the response is written, and before any restart so the credential is wiped.
        String namespace = taskDetails.getNamespace();
        if (namespace != null) {
          stickyLeaseManager.releaseTask(new NamespaceId(namespace));
        }
      }

      String className = taskDetails.getClassName();
      if (mustRestart.get() && pendingRequests == 0) {
        stopper.accept(className);
        return;
      }

      if (!enableUserCodeIsolationEnabled
          || !taskDetails.isTerminateOnComplete()
          || className == null || killAfterRequestCount <= 0) {
        // No need to restart.
        return;
      }

      if (requestProcessedCount.get() >= killAfterRequestCount) {
        // Drain instead of stopping now, which would kill sibling tasks still running.
        mustRestart.set(true);
        if (pendingRequests == 0) {
          stopper.accept(className);
        }
      }
    };

    enablePeriodicRestart(cConf, stopper);
  }

  /** Returns the number of tasks this pod runs at once. */
  @VisibleForTesting
  int getConcurrentRequestLimit() {
    return concurrentRequestLimit;
  }

  /**
   * Returns the lease manager, or null when this pod does not run behind the Task Worker Manager proxy.
   */
  @VisibleForTesting
  @Nullable
  StickyLeaseManager getStickyLeaseManager() {
    return stickyLeaseManager;
  }

  /** Returns the number of tasks currently holding a slot on this pod. */
  @VisibleForTesting
  int getRunningRequestCount() {
    return runningRequestCount.get();
  }

  /**
   * If there is no ongoing request, worker pod gets restarted after a random
   * duration is selected from the following range. Otherwise, worker pod can
   * only get restarted once the ongoing request finishes. range = [Duration -
   * DURATION_FRACTION * Duration, Duration + DURATION_FRACTION * Duration]
   * Reason: by randomizing the duration, it is guaranteed that pods do not get
   * restarted at the same time.
   */
  private void enablePeriodicRestart(CConfiguration cConf,
      Consumer<String> stopper) {
    int duration = cConf.getInt(
        Constants.TaskWorker.CONTAINER_KILL_AFTER_DURATION_SECOND, 0);
    int lowerBound = (int) (duration - duration * DURATION_FRACTION);
    int upperBound = (int) (duration + duration * DURATION_FRACTION);

    if (duration <= 0) {
      return;
    }
    int waitTime = (new Random()).nextInt(upperBound - lowerBound) + lowerBound;
    int finalTaskDeadlineSeconds = calculateFinalTaskDeadlineSeconds(duration);

    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(
    Threads.createDaemonThreadFactory("task-worker-restart"));

    executorService.scheduleWithFixedDelay(() -> {
      // we restart once all ongoing requests finish, i.e. runningRequestCount is 0.
      mustRestart.set(true);
      LOG.debug(
          "Task worker service is about to restart in {} seconds, no new tasks will be accepted.",
          finalTaskDeadlineSeconds);
      if (runningRequestCount.get() == 0) {
        stopAndShutdown(executorService, stopper);
      }
      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(finalTaskDeadlineSeconds));
      } catch (InterruptedException e) {
        LOG.warn(
            "Interrupted while waiting for task completion. Stopping immediately",
            e);
      }
      stopAndShutdown(executorService, stopper);
    }, waitTime, finalTaskDeadlineSeconds, TimeUnit.SECONDS);
  }

  private void stopAndShutdown(ScheduledExecutorService executorService, Consumer<String> stopper) {
    stopper.accept("");
    executorService.shutdown();
    System.exit(0);
  }

  /**
   * Run a new {@link io.cdap.cdap.api.service.worker.RunnableTask}.
   *
   * @param request   Information about the task to run
   * @param responder Responder to send back a http response.
   */
  @POST
  @Path("/run")
  public void run(FullHttpRequest request, HttpResponder responder) {
    if (mustRestart.get()) {
      responder.sendStatus(HttpResponseStatus.TOO_MANY_REQUESTS);
      return;
    }
    if (runningRequestCount.incrementAndGet() > concurrentRequestLimit) {
      responder.sendStatus(HttpResponseStatus.TOO_MANY_REQUESTS);
      runningRequestCount.decrementAndGet();
      return;
    }

    long startTime = System.currentTimeMillis();
    RunnableTaskRequest runnableTaskRequest;
    RunnableTaskContext runnableTaskContext;
    NamespaceId namespaceId;

    // Admission: nothing here runs user code, so a failure only needs to release the slot.
    try {
      runnableTaskRequest = GSON.fromJson(
          request.content().toString(StandardCharsets.UTF_8),
          RunnableTaskRequest.class);
      runnableTaskContext = new RunnableTaskContext(runnableTaskRequest);
      namespaceId = new NamespaceId(TaskDetails.extractNamespace(runnableTaskRequest));

      if (stickyLeaseManager != null) {
        StickyLeaseManager.AdmissionStatus status = stickyLeaseManager.admitTask(namespaceId);
        if (status != StickyLeaseManager.AdmissionStatus.SUCCESS) {
          // Report the lease holder so the proxy can correct its routing table.
          responder.sendStatus(HttpResponseStatus.TOO_MANY_REQUESTS, leaseRejectionHeaders());
          runningRequestCount.decrementAndGet();
          return;
        }
      }
    } catch (Exception ex) {
      LOG.error("Failed to admit task {}",
          request.content().toString(StandardCharsets.UTF_8), ex);
      // A null request tells the completion consumer there is no lease to release.
      failTask(responder, HttpResponseStatus.INTERNAL_SERVER_ERROR, ex, startTime, null, false);
      return;
    }

    // Launch: exactly one path below calls the completion consumer.
    try {
      if (stickyLeaseManager == null) {
        // Under a lease the lease manager provisions and wipes the credential instead.
        GcpMetadataTaskContextUtil.setGcpMetadataTaskContext(namespaceId, cConf);
      }
      runnableTaskLauncher.launchRunnableTask(runnableTaskContext);
      TaskDetails taskDetails = new TaskDetails(metricsCollectionService,
          startTime, runnableTaskContext.isTerminateOnComplete(),
          runnableTaskRequest);
      responder.sendContent(HttpResponseStatus.OK,
          new RunnableTaskBodyProducer(runnableTaskContext,
              taskCompletionConsumer, taskDetails),
          new DefaultHttpHeaders().add(HttpHeaders.CONTENT_TYPE,
              MediaType.APPLICATION_OCTET_STREAM));
    } catch (ClassNotFoundException | ClassCastException ex) {
      // Since the user class is not even loaded, no user code ran, hence it's ok to not terminate
      // the runner.
      failTask(responder, HttpResponseStatus.BAD_REQUEST, ex, startTime, runnableTaskRequest,
          false);
    } catch (Exception ex) {
      LOG.error("Failed to run task {}",
          request.content().toString(StandardCharsets.UTF_8), ex);
      // Potentially ran user code, hence terminate the runner.
      failTask(responder, HttpResponseStatus.INTERNAL_SERVER_ERROR, ex, startTime,
          runnableTaskRequest, true);
    } catch (Throwable t) {
      // An Error (e.g. NoClassDefFoundError): release the slot and lease, then rethrow.
      taskCompletionConsumer.accept(false,
          new TaskDetails(metricsCollectionService, startTime, true,
              runnableTaskRequest));
      throw t;
    } finally {
      if (stickyLeaseManager == null) {
        // Leased pods release in taskCompletionConsumer instead, once the response is written;
        // releasing here would let the proxy hand the pod to another namespace mid-response.
        clearTaskContextOrScheduleRestart();
      }
    }
  }

  /**
   * Reports a failed task and releases its slot, even if the response can't be sent.
   *
   * @param terminateOnComplete whether user code may have run
   * @param request the originating request, or null if it couldn't be read
   */
  private void failTask(HttpResponder responder, HttpResponseStatus status, Exception ex,
      long startTime, @Nullable RunnableTaskRequest request, boolean terminateOnComplete) {
    try {
      responder.sendString(status, exceptionToJson(ex),
          new DefaultHttpHeaders().set(HttpHeaders.CONTENT_TYPE, "application/json"));
    } finally {
      taskCompletionConsumer.accept(false,
          new TaskDetails(metricsCollectionService, startTime, terminateOnComplete, request));
    }
  }

  /** Wipes the credential on a pod without a lease, and restarts the pod if that fails. */
  private void clearTaskContextOrScheduleRestart() {
    try {
      GcpMetadataTaskContextUtil.clearGcpMetadataTaskContext(cConf);
    } catch (IOException e) {
      LOG.error("Failed to wipe the service account credential after the task finished. "
          + "Restarting the task worker so the credential does not outlive the namespace that "
          + "provisioned it.", e);
      mustRestart.set(true);
    }
  }

  /** Builds the rejection headers naming the namespace that holds this pod's lease. */
  private DefaultHttpHeaders leaseRejectionHeaders() {
    DefaultHttpHeaders headers = new DefaultHttpHeaders();
    NamespaceId leased = stickyLeaseManager.getCurrentLease();
    if (leased != null) {
      headers.set(Constants.Gateway.HEADER_LEASED_NAMESPACE, leased.getNamespace());
    }
    return headers;
  }

  /**
   * Returns a new token from metadata server.
   *
   * @param request   The {@link io.netty.handler.codec.http.HttpRequest}.
   * @param responder a {@link HttpResponder} for sending response.
   */
  @GET
  @Path("/token")
  public void token(io.netty.handler.codec.http.HttpRequest request,
      HttpResponder responder) {
    if (metadataServiceEndpoint == null) {
      responder.sendString(HttpResponseStatus.NOT_IMPLEMENTED,
          String.format("%s has not been set",
              Constants.TaskWorker.METADATA_SERVICE_END_POINT));
      return;
    }

    try {
      URL url = new URL(metadataServiceEndpoint);
      HttpRequest tokenRequest = HttpRequest.get(url)
          .addHeader("Metadata-Flavor", "Google").build();
      HttpResponse tokenResponse = HttpRequests.execute(tokenRequest);
      responder.sendByteArray(HttpResponseStatus.OK,
          tokenResponse.getResponseBody(), EmptyHttpHeaders.INSTANCE);
    } catch (Exception ex) {
      LOG.warn("Failed to fetch token from metadata service", ex);
      responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR,
          exceptionToJson(ex));
    }
  }

  /**
   * Return json representation of an exception. Used to propagate exception
   * across network for better surfacing errors and debuggability.
   */
  private String exceptionToJson(Exception ex) {
    BasicThrowable basicThrowable = new BasicThrowable(ex);
    return GSON.toJson(basicThrowable);
  }

  /**
   * Compute the final task Dead line in Seconds where if the config {@TaskWorker.TASK_EXECUTION_DEADLINE_SECOND}
   * is less than 0 which is not valid then use the duration instead.
   *
   * @param duration
   * @return
   */
  private int calculateFinalTaskDeadlineSeconds(int duration) {
    int taskDeadlineSeconds = cConf.getInt(
      TaskWorker.TASK_EXECUTION_DEADLINE_SECOND,
      0);

    if (taskDeadlineSeconds < 0) {
      LOG.info(
        "Task deadline is {}, using {} value {} as the deadline instead.",
        taskDeadlineSeconds,
        Constants.TaskWorker.CONTAINER_KILL_AFTER_DURATION_SECOND, duration);
      taskDeadlineSeconds = duration;
    }
    return taskDeadlineSeconds;
  }

  /**
   * By using BodyProducer instead of simply sending out response bytes, the
   * handler can get notified (through finished method) when sending the
   * response is done, so it can safely call the stopper to kill the worker
   * pod.
   */
  private static class RunnableTaskBodyProducer extends BodyProducer {

    private final RunnableTaskContext context;
    private final BiConsumer<Boolean, TaskDetails> taskCompletionConsumer;
    private final TaskDetails taskDetails;
    private boolean done;

    RunnableTaskBodyProducer(RunnableTaskContext context,
        BiConsumer<Boolean, TaskDetails> taskCompletionConsumer,
        TaskDetails taskDetails) {
      this.context = context;
      this.taskCompletionConsumer = taskCompletionConsumer;
      this.taskDetails = taskDetails;
    }

    @Override
    public ByteBuf nextChunk() {
      if (done) {
        return Unpooled.EMPTY_BUFFER;
      }

      done = true;
      return Unpooled.wrappedBuffer(context.getResult());
    }

    @Override
    public void finished() {
      context.executeCleanupTask();
      taskCompletionConsumer.accept(true, taskDetails);
    }

    @Override
    public void handleError(@Nullable Throwable cause) {
      LOG.error("Error when sending chunks", cause);
      context.executeCleanupTask();
      taskCompletionConsumer.accept(false, taskDetails);
    }
  }

  /** The pod's namespaced credential, held by the metadata sidecar. */
  private final class SidecarCredentialContext implements NamespaceCredentialContext {

    @Override
    public void provision(NamespaceId namespace) {
      try {
        GcpMetadataTaskContextUtil.setGcpMetadataTaskContext(namespace, cConf);
      } catch (IOException e) {
        // The task must not run under the wrong identity; the lease manager unwinds its claim.
        throw new UncheckedIOException(
            "Failed to provision the service account credential for namespace "
                + namespace.getNamespace(), e);
      }
    }

    @Override
    public void wipe() {
      try {
        GcpMetadataTaskContextUtil.clearGcpMetadataTaskContext(cConf);
      } catch (IOException e) {
        // Nothing upstream can handle this, so restart the idle pod rather than keep the credential.
        LOG.error("Failed to wipe the service account credential after the last task finished. "
            + "Restarting the task worker so the credential does not outlive the namespace that "
            + "provisioned it.", e);
        mustRestart.set(true);
      }
    }
  }
}
