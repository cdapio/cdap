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
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
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
      BasicThrowable.class,
      new BasicThrowableCodec()).create();

  private final RunnableTaskLauncher runnableTaskLauncher;
  private final BiConsumer<Boolean, RunningTaskDetails> taskCompletionConsumer;

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
  private final int requestLimit;
  private final ConcurrentMap<RunningTaskDetails, Long> runningTasks;
  /**
   * If true, the task worker will not accept new tasks as it is about to shut down.
   */
  private final AtomicBoolean isInLameDuckMode = new AtomicBoolean(false);

  /**
   * Constructs the {@link TaskWorkerHttpHandlerInternal}.
   */
  public TaskWorkerHttpHandlerInternal(CConfiguration cConf,
      DiscoveryService discoveryService,
      DiscoveryServiceClient discoveryServiceClient, Consumer<String> stopper,
      MetricsCollectionService metricsCollectionService) {
    this.cConf = cConf;
    final int killAfterRequestCount = cConf.getInt(
        Constants.TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 0);
    this.runnableTaskLauncher = new RunnableTaskLauncher(cConf,
        discoveryService, discoveryServiceClient, metricsCollectionService);
    this.metricsCollectionService = metricsCollectionService;
    this.metadataServiceEndpoint = cConf.get(
        Constants.TaskWorker.METADATA_SERVICE_END_POINT);
    this.runningTasks = new ConcurrentHashMap<>();
    boolean enableUserCodeIsolationEnabled = cConf.getBoolean(
        TaskWorker.USER_CODE_ISOLATION_ENABLED);
    if (enableUserCodeIsolationEnabled) {
      // Run only one request at a time in user code isolation mode.
      this.requestLimit = 1;
      // Restart the service to clean up and re-claim resources after user code
      // execution.
      this.taskCompletionConsumer = (succeeded, runningTaskDetails) -> {
        TaskDetails taskDetails = runningTaskDetails.getTaskDetails();
        taskDetails.emitMetrics(succeeded);
        runningRequestCount.decrementAndGet();
        requestProcessedCount.incrementAndGet();
        runningTasks.remove(runningTaskDetails);

        String className = taskDetails.getClassName();

        if (mustRestart.get()) {
          stopper.accept(className);
          return;
        }

        if (!taskDetails.isTerminateOnComplete() || className == null
            || killAfterRequestCount <= 0) {
          // No need to restart.
          return;
        }

        if (requestProcessedCount.get() >= killAfterRequestCount) {
          stopper.accept(className);
        }
      };

      enablePeriodicRestart(cConf, stopper);
    } else {
      this.requestLimit = cConf.getInt(TaskWorker.REQUEST_LIMIT);
      this.taskCompletionConsumer = (succeeded, runningTaskDetails) -> {
        runningTaskDetails.getTaskDetails().emitMetrics(succeeded);
        int remainingTasks = runningRequestCount.decrementAndGet();
        runningTasks.remove(runningTaskDetails);
        // If the stuck task has completed while waiting, we can call off the pending shutdown.
        if (remainingTasks > 0 || !isInLameDuckMode.get()) {
          return;
        }
        if (isInLameDuckMode.compareAndSet(true, false)) {
          LOG.debug(
              "All pending tasks are completed, task worker can accept new requests");
        }
      };
      monitorStuckTasks(cConf, stopper);
    }
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
        TaskWorker.CONTAINER_KILL_AFTER_DURATION_SECOND, 0);
    int lowerBound = (int) (duration - duration * DURATION_FRACTION);
    int upperBound = (int) (duration + duration * DURATION_FRACTION);
    if (lowerBound <= 0) {
      return;
    }
    int waitTime = (new Random()).nextInt(upperBound - lowerBound) + lowerBound;
    Executors.newSingleThreadScheduledExecutor(
            Threads.createDaemonThreadFactory("task-worker-restart"))
        .scheduleWithFixedDelay(() -> {
          if (mustRestart.get()) {
            // We force pod restart as the ongoing request has not finished since last
            // periodic restart check.
            stopper.accept("");
            return;
          }
          // we restart once ongoing request (which has set runningRequestCount to 1)
          // finishes.
          mustRestart.set(true);
          if (runningRequestCount.compareAndSet(0, 1)) {
            // there is no ongoing request. pod gets restarted.
            stopper.accept("");
          }
        }, waitTime, waitTime, TimeUnit.SECONDS);
  }

  /**
   * Each task needs to complete withing a configured deadline. There can be
   * multiple tasks running concurrently when user code isolation is disabled.
   * If any task doesn't complete within the deadline, this method will try to
   * restart the service to kill it while ensuring healthy tasks get time to
   * complete gracefully. By randomizing the deadline, it is guaranteed that
   * pods do not get restarted at the same time.
   */
  private void monitorStuckTasks(CConfiguration cConf,
      Consumer<String> stopper) {
    int duration = cConf.getInt(
        TaskWorker.TASK_EXECUTION_DEADLINE_SECOND, 0);
    if (duration <= 0) {
      return;
    }
    int lowerBound = duration;
    int upperBound = (int) (duration + duration * DURATION_FRACTION);
    int executionDeadlineSeconds =
        lowerBound + (upperBound > lowerBound ? (new Random()).nextInt(
            upperBound - lowerBound) : 0);
    LOG.debug("Deadline for tasks is {} seconds", executionDeadlineSeconds);

    Executors.newSingleThreadScheduledExecutor(
            Threads.createDaemonThreadFactory("task-worker-stuck-tasks-monitor"))
        .scheduleWithFixedDelay(() -> {
          // Check if all the running tasks are withing deadline.
          boolean deadlineExceeded = isAnyTaskExceedingDeadline(
              executionDeadlineSeconds);
          if (!deadlineExceeded) {
            isInLameDuckMode.set(false);
            return;
          }

          // If no task was exceeding the deadline last time, stop accepting any new
          // tasks and wait for "deadline" seconds to elapse before stopping.
          // By not accepting new tasks and waiting, we ensure that only tasks
          // that have exceeded the deadline are terminated by the shutdown.
          if (isInLameDuckMode.compareAndSet(false, true)) {
            LOG.debug(
                "Task worker will not accept new tasks in preparation for a shutdown.");
            return;
          }

          // If we have already waited for the running task to complete, force the
          // service to restart. This will cause all the executing tasks to fail.
          if (isInLameDuckMode.get()) {
            LOG.debug(
                "Requesting task worker to be stopped to kill stuck task(s).");
            stopper.accept("");
          }
        }, executionDeadlineSeconds, executionDeadlineSeconds, TimeUnit.SECONDS);
  }

  private boolean isAnyTaskExceedingDeadline(long deadlineSeconds) {
    final long currentTimeMillis = System.currentTimeMillis();
    final long deadlineMillis = TimeUnit.SECONDS.toMillis(deadlineSeconds);
    for (Long startTimeMillis : runningTasks.values()) {
      if (currentTimeMillis - startTimeMillis > deadlineMillis) {
        return true;
      }
    }
    return false;
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
    if (isInLameDuckMode.get()
        || runningRequestCount.incrementAndGet() > requestLimit) {
      responder.sendStatus(HttpResponseStatus.TOO_MANY_REQUESTS);
      runningRequestCount.decrementAndGet();
      return;
    }

    long startTimeMillis = System.currentTimeMillis();
    // As tasks don't have a unique ID, assign an ephemeral ID to each task for
    // tracking its execution and completion status.
    String taskId = UUID.randomUUID().toString();
    try {
      RunnableTaskRequest runnableTaskRequest = GSON.fromJson(
          request.content().toString(StandardCharsets.UTF_8),
          RunnableTaskRequest.class);
      RunnableTaskContext runnableTaskContext = new RunnableTaskContext(
          runnableTaskRequest);
      try {
        NamespaceId namespaceId;
        if (runnableTaskRequest.getParam().getEmbeddedTaskRequest() != null) {
          // For system app tasks
          namespaceId = new NamespaceId(
              runnableTaskRequest.getParam().getEmbeddedTaskRequest()
                  .getNamespace());
        } else {
          namespaceId = new NamespaceId(runnableTaskRequest.getNamespace());
        }
        // set the GcpMetadataTaskContext before running the task.
        GcpMetadataTaskContextUtil.setGcpMetadataTaskContext(namespaceId,
            cConf);
        TaskDetails taskDetails = new TaskDetails(metricsCollectionService,
            startTimeMillis, runnableTaskContext.isTerminateOnComplete(),
            runnableTaskRequest);
        final RunningTaskDetails runningTaskDetails = new RunningTaskDetails(
            startTimeMillis, taskId, taskDetails);
        runningTasks.put(runningTaskDetails, startTimeMillis);
        runnableTaskLauncher.launchRunnableTask(runnableTaskContext);
        responder.sendContent(HttpResponseStatus.OK,
            new RunnableTaskBodyProducer(runnableTaskContext,
                taskCompletionConsumer, runningTaskDetails),
            new DefaultHttpHeaders().add(HttpHeaders.CONTENT_TYPE,
                MediaType.APPLICATION_OCTET_STREAM));
      } catch (ClassNotFoundException | ClassCastException ex) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST,
            exceptionToJson(ex),
            new DefaultHttpHeaders().set(HttpHeaders.CONTENT_TYPE,
                "application/json"));
        // Since the user class is not even loaded, no user code ran, hence it's ok to not terminate the runner
        final TaskDetails taskDetails = new TaskDetails(
            metricsCollectionService, startTimeMillis, false,
            runnableTaskRequest);
        taskCompletionConsumer.accept(false,
            new RunningTaskDetails(startTimeMillis, taskId, taskDetails));
      } finally {
        // clear the GcpMetadataTaskContext after the task is completed.
        GcpMetadataTaskContextUtil.clearGcpMetadataTaskContext(cConf);
      }
    } catch (Exception ex) {
      LOG.error("Failed to run task {}",
          request.content().toString(StandardCharsets.UTF_8), ex);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
          exceptionToJson(ex),
          new DefaultHttpHeaders().set(HttpHeaders.CONTENT_TYPE,
              "application/json"));
      // Potentially ran user code, hence terminate the runner.
      final TaskDetails taskDetails = new TaskDetails(metricsCollectionService,
          startTimeMillis, true, null);
      taskCompletionConsumer.accept(false,
          new RunningTaskDetails(startTimeMillis, taskId, taskDetails));
    }
  }

  /**
   * Returns a new token from metadata server.
   *
   * @param request The {@link io.netty.handler.codec.http.HttpRequest}.
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
          .addHeader("Metadata-Flavor", "Google")
          .build();
      HttpResponse tokenResponse = HttpRequests.execute(tokenRequest);
      responder.sendByteArray(HttpResponseStatus.OK,
          tokenResponse.getResponseBody(),
          EmptyHttpHeaders.INSTANCE);
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
   * By using BodyProducer instead of simply sending out response bytes, the
   * handler can get notified (through finished method) when sending the
   * response is done, so it can safely call the stopper to kill the worker
   * pod.
   */
  private static class RunnableTaskBodyProducer extends BodyProducer {

    private final RunnableTaskContext context;
    private final BiConsumer<Boolean, RunningTaskDetails> taskCompletionConsumer;
    private final RunningTaskDetails taskDetails;
    private boolean done;

    RunnableTaskBodyProducer(RunnableTaskContext context,
        BiConsumer<Boolean, RunningTaskDetails> taskCompletionConsumer,
        RunningTaskDetails taskDetails) {
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
   * Details for tracking presently running tasks.
   */
  private class RunningTaskDetails {

    private final long executionStartTimeMillis;
    private final String taskId;
    private final TaskDetails taskDetails;

    RunningTaskDetails(long executionStartTimeMillis, String taskId,
        TaskDetails taskDetails) {
      this.executionStartTimeMillis = executionStartTimeMillis;
      this.taskId = taskId;
      this.taskDetails = taskDetails;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RunningTaskDetails)) {
        return false;
      }
      RunningTaskDetails that = (RunningTaskDetails) o;
      return executionStartTimeMillis == that.executionStartTimeMillis
             && taskId.equals(that.taskId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(taskId);
    }

    public TaskDetails getTaskDetails() {
      return taskDetails;
    }
  }
}
