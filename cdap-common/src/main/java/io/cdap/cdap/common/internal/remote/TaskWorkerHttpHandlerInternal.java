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

  /**
   * Response header naming the namespace that currently holds this pod's lease. Read by
   * {@link ProxyBackendHandler} on a rejection to correct the proxy's routing table.
   */
  private static final String LEASED_NAMESPACE_HEADER = "X-Leased-Namespace";

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

  /**
   * Owns the namespace lease and the lifetime of the namespaced credential on this pod. Null on
   * instances that do not run behind the Task Worker Manager proxy, where there is either no per-task
   * credential at all or only ever one task in flight.
   */
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

  /**
   * Constructs the handler around an already built launcher.
   *
   * <p>Exists so that tests can drive the handler's failure paths, which is otherwise awkward: the
   * launcher loads and instantiates the task class, so provoking a specific failure would mean
   * shipping a class that fails in that exact way and relying on the container's classloading to
   * cooperate.
   */
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

    // Three deployment modes decide how many tasks may share this pod.
    //
    //   Non-RBAC: user code isolation is off and there is no per-task credential to protect, so
    //       the pod runs at the configured limit. Tasks from different namespaces may share it.
    //   RBAC without the proxy: one namespaced credential lives in a single mutable context on
    //       the pod and nothing coordinates ownership of it, so exactly one task may run.
    //   RBAC with the proxy: the lease coordinates ownership of that credential, which is what
    //       makes it safe to run the configured limit again. The proxy reads the same
    //       task.worker.request.limit for its own dispatch ceiling, so the two agree by
    //       construction.
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
        // The leased pod's counterpart to the clearTaskContextOrScheduleRestart() call in run()'s
        // finally. It runs here rather than there because the pod must keep counting as busy
        // until the response is off the wire, not because the credential is still in use. See
        // that finally for what goes wrong if the count drops earlier.
        //
        // Released before any restart decision below, so that a pod shutting down still wipes the
        // credential on its way out.
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
        // Stop accepting work and let the pod drain. Stopping outright here would kill any task
        // still running alongside this one: the HTTP service is given two seconds to wind down,
        // which is nothing against a task that can run for half a minute. This was unreachable
        // while isolation mode pinned concurrency to one, because there was never a sibling.
        mustRestart.set(true);
        if (pendingRequests == 0) {
          stopper.accept(className);
        }
      }
    };

    enablePeriodicRestart(cConf, stopper);
  }

  /**
   * Returns the number of tasks this pod will run at once, after the deployment mode has been
   * resolved.
   */
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

  /**
   * Returns the number of tasks currently occupying a slot on this pod.
   *
   * <p>Every admission path is gated on this count, so a slot that is taken and never released
   * takes capacity with it permanently. Tests assert it returns to zero on the failure paths.
   */
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

    // Admission. Everything that has to succeed before the pod commits to running the task, and
    // nothing that can run user code. Failing here means no lease is held and nothing executed,
    // so the slot is released without claiming otherwise.
    try {
      runnableTaskRequest = GSON.fromJson(
          request.content().toString(StandardCharsets.UTF_8),
          RunnableTaskRequest.class);
      runnableTaskContext = new RunnableTaskContext(runnableTaskRequest);
      namespaceId = new NamespaceId(TaskDetails.extractNamespace(runnableTaskRequest));

      if (stickyLeaseManager != null) {
        StickyLeaseManager.AdmissionStatus status = stickyLeaseManager.admitTask(namespaceId);
        if (status != StickyLeaseManager.AdmissionStatus.SUCCESS) {
          // The pod is busy with another namespace, or saturated. Name the namespace that actually
          // holds the lease so the proxy can correct its routing table instead of retrying here.
          responder.sendStatus(HttpResponseStatus.TOO_MANY_REQUESTS, leaseRejectionHeaders());
          runningRequestCount.decrementAndGet();
          return;
        }
      }
    } catch (Exception ex) {
      LOG.error("Failed to admit task {}",
          request.content().toString(StandardCharsets.UTF_8), ex);
      // The task never started, so nothing is owed to the lease. Naming no request is what says
      // so: the completion consumer releases a lease only for a named namespace, and an admission
      // that threw has already unwound its own claim.
      failTask(responder, HttpResponseStatus.INTERNAL_SERVER_ERROR, ex, startTime, null, false);
      return;
    }

    // Launch. Exactly one path below calls the completion consumer for this task: on success the
    // body producer does it once the response has been written, otherwise the matching catch does
    // it here. No path both hands off and cleans up, which is why none of them need to know what
    // the others did.
    try {
      if (stickyLeaseManager == null) {
        // set the GcpMetadataTaskContext before running the task. Under a lease the credential
        // belongs to the whole burst of tasks rather than to this one, so the lease manager owns
        // both ends of its lifetime and neither happens here.
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
      // An Error rather than an Exception, most plausibly a NoClassDefFoundError while loading a
      // user artifact. Nothing downstream exists to release the slot or the lease, and every
      // recovery path is gated on the active task count reaching zero, so leaving it incremented
      // would brick the pod permanently and disable the credential wipe with it. Release here,
      // then let the Error propagate: swallowing an OutOfMemoryError to return a tidy 500 is the
      // worse trade.
      taskCompletionConsumer.accept(false,
          new TaskDetails(metricsCollectionService, startTime, true,
              runnableTaskRequest));
      throw t;
    } finally {
      if (stickyLeaseManager == null) {
        // Without a lease the credential belongs to this one task, so it is wiped here, the moment
        // the task is done with it.
        //
        // The absence of an else branch is deliberate and is the one asymmetry in this method. A
        // leased pod releases in taskCompletionConsumer instead, which Netty invokes from
        // RunnableTaskBodyProducer once the response has been written and the context's cleanup
        // task has run. Releasing it here would drop the active task count to zero while the body
        // is still streaming, letting the proxy hand the pod to another namespace mid-response.
        // Tightening the lease manager's admission rules to compensate does not rescue it: the
        // proxy cannot observe in-flight responses, so a worker that refuses on that basis only
        // manufactures rejections the proxy will retry into forever.
        clearTaskContextOrScheduleRestart();
      }
    }
  }

  /**
   * Reports a failed task to the caller and gives the pod back its capacity.
   *
   * <p>The release happens in a finally because the pod's capacity must not depend on the caller
   * still being there to hear about the failure. A responder that throws, because the connection
   * went away or the response was already committed, would otherwise take a slot with it and, on a
   * leased pod, keep the namespaced credential alive with nothing running.
   *
   * @param terminateOnComplete whether user code may have run, which is what decides if the pod is
   *     a candidate for recycling
   * @param request the originating request, or null if the failure happened before it could be
   *     read, in which case no lease is released
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

  /**
   * Wipes the namespaced credential from the sidecar on a pod that runs without a lease, and
   * recycles the pod if the wipe fails.
   *
   * <p>Mirrors what the lease manager does on its own wipe, for the same reason: by the time this
   * runs the response is already on the wire, so there is nothing to report the failure to, and a
   * credential that outlives the task that provisioned it is not something to leave behind on a
   * pod that goes on to serve other namespaces. The restart costs one cold start.
   */
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

  /**
   * Builds the headers that tell the Task Worker Manager proxy who really owns this pod, so it can heal
   * its routing table after guessing wrong.
   *
   * <p>Only the leased namespace is reported. The active task count is deliberately omitted: it
   * counts work owned by connections the proxy does not hold, so the proxy could never decrement
   * it and adopting it would strand the pod.
   */
  private DefaultHttpHeaders leaseRejectionHeaders() {
    DefaultHttpHeaders headers = new DefaultHttpHeaders();
    NamespaceId leased = stickyLeaseManager.getCurrentLease();
    if (leased != null) {
      headers.set(LEASED_NAMESPACE_HEADER, leased.getNamespace());
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

  /**
   * The pod's namespaced credential, as held by the metadata sidecar running alongside this
   * container.
   *
   * <p>An inner class rather than a lambda pair because the two halves have genuinely different
   * failure policies, and both of those policies are about this handler's state rather than about
   * credentials.
   */
  private final class SidecarCredentialContext implements NamespaceCredentialContext {

    @Override
    public void provision(NamespaceId namespace) {
      try {
        GcpMetadataTaskContextUtil.setGcpMetadataTaskContext(namespace, cConf);
      } catch (IOException e) {
        // The task cannot run as the right identity, so it must not run at all. The lease manager
        // unwinds its claim and this surfaces as a 500 from the run handler.
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
        // Nothing further up can act on this: the task has already finished and its response is on
        // the wire. Rather than leave a namespaced credential sitting on an idle pod, recycle the
        // pod so the credential dies with the process. The pod is idle by definition at this
        // point, so the restart costs one cold start and no running work.
        LOG.error("Failed to wipe the service account credential after the last task finished. "
            + "Restarting the task worker so the credential does not outlive the namespace that "
            + "provisioned it.", e);
        mustRestart.set(true);
      }
    }
  }
}
