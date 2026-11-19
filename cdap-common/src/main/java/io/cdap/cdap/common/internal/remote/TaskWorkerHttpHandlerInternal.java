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
  private final Consumer<String> stopper;

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
  private final AtomicBoolean drainStopTriggered = new AtomicBoolean(false);
  private final int concurrentRequestLimit;
  private final boolean userCodeIsolationEnabled;
  private final int killAfterRequestCount;

  /** Owns the namespace lease and credential; null unless running behind the task worker manager. */
  @Nullable
  private final StickyLeaseManager stickyLeaseManager;

  /**
   * Constructs the {@link TaskWorkerHttpHandlerInternal}.
   *
   * @param runnableTaskLauncher launches the tasks this handler admits
   * @param stopper stops the task worker
   */
  public TaskWorkerHttpHandlerInternal(CConfiguration cConf,
      RunnableTaskLauncher runnableTaskLauncher, Consumer<String> stopper,
      MetricsCollectionService metricsCollectionService) {
    this.cConf = cConf;
    this.killAfterRequestCount = cConf.getInt(
        Constants.TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 0);
    this.runnableTaskLauncher = runnableTaskLauncher;
    this.stopper = stopper;
    this.metricsCollectionService = metricsCollectionService;
    this.metadataServiceEndpoint = cConf.get(
        Constants.TaskWorker.METADATA_SERVICE_END_POINT);
    this.userCodeIsolationEnabled = cConf.getBoolean(TaskWorker.USER_CODE_ISOLATION_ENABLED);
    boolean stickyLeaseEnabled = TaskWorkerManagerUtil.isStickyLeaseEnabled(cConf);

    // Isolation without the manager runs one task at a time, since nothing coordinates the shared
    // credential. With the manager, the lease does, so the configured limit applies.
    if (userCodeIsolationEnabled && !stickyLeaseEnabled) {
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
      final String className = taskDetails.getClassName();
      final boolean stopDirectNow;
      try {
        taskDetails.emitMetrics(succeeded);
        requestProcessedCount.incrementAndGet();

        if (stickyLeaseManager != null) {
          // Released after the response is written, and before any restart so the credential is
          // wiped while this slot still holds off any concurrent drain stop.
          String namespace = taskDetails.getNamespace();
          if (namespace != null) {
            stickyLeaseManager.releaseTask(new NamespaceId(namespace));
          }
        }

        boolean hitKillCount = !mustRestart.get()
            && reachesKillCount(taskDetails, requestProcessedCount.get());
        if (hitKillCount) {
          // Refuse new tasks before the slot is released, since the stop is asynchronous.
          mustRestart.set(true);
        }
        // With a lease, drain so sibling tasks finish. Without one, one task runs at a time.
        stopDirectNow = hitKillCount && stickyLeaseManager == null;
      } finally {
        runningRequestCount.decrementAndGet();
      }

      if (stopDirectNow) {
        // Shares the latch with stopIfDrained so a rejected request can't stop the pod again.
        if (drainStopTriggered.compareAndSet(false, true)) {
          stopper.accept(className);
        }
      } else if (mustRestart.get()) {
        stopIfDrained(className);
      }
    };

    enablePeriodicRestart(cConf, stopper);
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
    if (stickyLeaseManager == null) {
      runDirect(request, responder);
      return;
    }
    runLeased(request, responder);
  }

  /**
   * Runs a task on a pod without a lease, which is any pod where RBAC or the task worker manager
   * is off.
   */
  private void runDirect(FullHttpRequest request, HttpResponder responder) {
    if (mustRestart.get()) {
      responder.sendStatus(HttpResponseStatus.TOO_MANY_REQUESTS);
      return;
    }
    int inFlight = runningRequestCount.incrementAndGet();
    if (mustRestart.get() || inFlight > concurrentRequestLimit) {
      rejectAndReleaseSlot(responder);
      return;
    }

    long startTime = System.currentTimeMillis();
    RunnableTaskRequest runnableTaskRequest;
    RunnableTaskContext runnableTaskContext;
    NamespaceId namespaceId;

    // Reading the request runs no user code, so a failure only needs to release the slot.
    try {
      runnableTaskRequest = GSON.fromJson(
          request.content().toString(StandardCharsets.UTF_8),
          RunnableTaskRequest.class);
      runnableTaskContext = new RunnableTaskContext(runnableTaskRequest);
      if (runnableTaskRequest.getParam().getEmbeddedTaskRequest() != null) {
        // For system app tasks
        namespaceId = new NamespaceId(
            runnableTaskRequest.getParam().getEmbeddedTaskRequest().getNamespace());
      } else {
        namespaceId = new NamespaceId(runnableTaskRequest.getNamespace());
      }
    } catch (Exception ex) {
      LOG.error("Failed to run task {}",
          request.content().toString(StandardCharsets.UTF_8), ex);
      failTask(responder, HttpResponseStatus.INTERNAL_SERVER_ERROR, ex, startTime, null, false);
      return;
    }

    // Exactly one path below calls the completion consumer.
    try {
      // set the GcpMetadataTaskContext before running the task.
      GcpMetadataTaskContextUtil.setGcpMetadataTaskContext(namespaceId, cConf);
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
      // Potentially ran user code, hence terminate the runner. The request names the task class,
      // which the restart check and the metrics need.
      failTask(responder, HttpResponseStatus.INTERNAL_SERVER_ERROR, ex, startTime,
          runnableTaskRequest, true);
    } catch (Throwable t) {
      // An Error (e.g. NoClassDefFoundError) from user code: give the slot back, then rethrow.
      taskCompletionConsumer.accept(false,
          new TaskDetails(metricsCollectionService, startTime, true, runnableTaskRequest));
      throw t;
    } finally {
      clearTaskContextOrScheduleRestart();
    }
  }

  /**
   * Runs a task on a pod leased to one namespace at a time. The lease manager provisions the
   * namespace's credential on admission and wipes it when the last task releases the lease.
   */
  private void runLeased(FullHttpRequest request, HttpResponder responder) {
    if (mustRestart.get()) {
      // A TERMINATING status tells the manager to back off this pod instead of retrying it.
      responder.sendStatus(HttpResponseStatus.TOO_MANY_REQUESTS,
          withStatus(new DefaultHttpHeaders(), TaskWorkerStatus.TERMINATING));
      return;
    }
    int inFlight = runningRequestCount.incrementAndGet();
    if (mustRestart.get() || inFlight > concurrentRequestLimit) {
      rejectAndReleaseSlot(responder);
      return;
    }

    long startTime = System.currentTimeMillis();
    RunnableTaskRequest runnableTaskRequest;
    RunnableTaskContext runnableTaskContext;
    StickyLeaseManager.AdmissionStatus status;

    // Admission: nothing here runs user code, so a failure only needs to release the slot.
    try {
      runnableTaskRequest = GSON.fromJson(
          request.content().toString(StandardCharsets.UTF_8),
          RunnableTaskRequest.class);
      runnableTaskContext = new RunnableTaskContext(runnableTaskRequest);
      NamespaceId namespaceId = new NamespaceId(TaskDetails.extractNamespace(runnableTaskRequest));
      synchronized (stickyLeaseManager) {
        status = mustRestart.get() ? null : stickyLeaseManager.admitTask(namespaceId);
      }
    } catch (Exception ex) {
      LOG.error("Failed to admit task {}",
          request.content().toString(StandardCharsets.UTF_8), ex);
      // A null request tells the completion consumer there is no lease to release.
      failTask(responder, HttpResponseStatus.INTERNAL_SERVER_ERROR, ex, startTime, null, false);
      return;
    }

    if (status != StickyLeaseManager.AdmissionStatus.SUCCESS) {
      // Report the lease holder (or draining state) so the manager can route away from this pod.
      rejectAndReleaseSlot(responder);
      return;
    }

    // Launch: exactly one path below calls the completion consumer, which releases the lease
    // once the response is written.
    try {
      runnableTaskLauncher.launchRunnableTask(runnableTaskContext);
      TaskDetails taskDetails = new TaskDetails(metricsCollectionService,
          startTime, runnableTaskContext.isTerminateOnComplete(),
          runnableTaskRequest);
      responder.sendContent(HttpResponseStatus.OK,
          new RunnableTaskBodyProducer(runnableTaskContext,
              taskCompletionConsumer, taskDetails),
          withStatus(new DefaultHttpHeaders().add(HttpHeaders.CONTENT_TYPE,
              MediaType.APPLICATION_OCTET_STREAM), statusAfter(taskDetails)));
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
    }
  }

  /**
   * Reports a failed task and releases its slot and lease, even if the response can't be sent.
   *
   * @param terminateOnComplete whether user code may have run
   * @param request the originating request, or null if it couldn't be read
   */
  private void failTask(HttpResponder responder, HttpResponseStatus status, Exception ex,
      long startTime, @Nullable RunnableTaskRequest request, boolean terminateOnComplete) {
    TaskDetails taskDetails =
        new TaskDetails(metricsCollectionService, startTime, terminateOnComplete, request);
    try {
      responder.sendString(status, exceptionToJson(ex), withStatus(
          new DefaultHttpHeaders().set(HttpHeaders.CONTENT_TYPE, "application/json"),
          statusAfter(taskDetails)));
    } finally {
      taskCompletionConsumer.accept(false, taskDetails);
    }
  }

  /**
   * Clears the GcpMetadataTaskContext after the task is completed, and restarts the pod if that
   * fails so the credential does not outlive the namespace that provisioned it.
   */
  private void clearTaskContextOrScheduleRestart() {
    try {
      GcpMetadataTaskContextUtil.clearGcpMetadataTaskContext(cConf);
    } catch (IOException e) {
      // The task's slot was already released; failing here must not release it again.
      LOG.error("Failed to wipe the service account credential after the task finished. "
          + "Restarting the task worker so the credential does not outlive the namespace that "
          + "provisioned it.", e);
      mustRestart.set(true);
      // A failed task already ran its completion, so stop here unless another task still runs.
      stopIfDrained("");
    }
  }

  private void stopIfDrained(@Nullable String className) {
    if (mustRestart.get()
        && runningRequestCount.get() == 0
        && drainStopTriggered.compareAndSet(false, true)) {
      stopper.accept(className);
    }
  }

  private void rejectAndReleaseSlot(HttpResponder responder) {
    try {
      if (stickyLeaseManager == null) {
        responder.sendStatus(HttpResponseStatus.TOO_MANY_REQUESTS);
      } else {
        // A terminating pod's lease is about to be wiped, so only a running pod names it.
        io.netty.handler.codec.http.HttpHeaders headers = mustRestart.get()
            ? withStatus(new DefaultHttpHeaders(), TaskWorkerStatus.TERMINATING)
            : withStatus(leaseRejectionHeaders(), TaskWorkerStatus.RUNNING);
        responder.sendStatus(HttpResponseStatus.TOO_MANY_REQUESTS, headers);
      }
    } finally {
      if (runningRequestCount.decrementAndGet() == 0) {
        stopIfDrained("");
      }
    }
  }

  /**
   * Returns whether finishing a task with these details brings the processed count to the
   * restart threshold of an isolated worker.
   *
   * @param processedCount the processed task count including this task
   */
  private boolean reachesKillCount(TaskDetails taskDetails, int processedCount) {
    return userCodeIsolationEnabled
        && taskDetails.isTerminateOnComplete()
        && taskDetails.getClassName() != null
        && killAfterRequestCount > 0
        && processedCount >= killAfterRequestCount;
  }

  /**
   * Returns the status for the response of a task that ran or failed: TERMINATING if a restart is
   * pending or this task reaches the restart count.
   */
  private TaskWorkerStatus statusAfter(TaskDetails taskDetails) {
    if (mustRestart.get() || reachesKillCount(taskDetails, requestProcessedCount.get() + 1)) {
      return TaskWorkerStatus.TERMINATING;
    }
    return TaskWorkerStatus.RUNNING;
  }

  /**
   * Adds the worker status to {@code headers} on a pod behind the task worker manager. Pods
   * without a lease answer as before, without it.
   */
  private io.netty.handler.codec.http.HttpHeaders withStatus(
      io.netty.handler.codec.http.HttpHeaders headers, TaskWorkerStatus status) {
    if (stickyLeaseManager != null) {
      headers.set(Constants.TaskWorkerManager.HEADER_TASK_WORKER_STATUS, status.name());
    }
    return headers;
  }

  /** Builds the rejection headers naming the namespace actively running tasks on this pod. */
  private DefaultHttpHeaders leaseRejectionHeaders() {
    DefaultHttpHeaders headers = new DefaultHttpHeaders();
    NamespaceId leased = stickyLeaseManager.getActiveLease();
    if (leased != null) {
      headers.set(Constants.TaskWorkerManager.HEADER_LEASED_NAMESPACE, leased.getNamespace());
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
