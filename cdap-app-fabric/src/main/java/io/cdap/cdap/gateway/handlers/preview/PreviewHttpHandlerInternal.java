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

package io.cdap.cdap.gateway.handlers.preview;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.app.preview.PreviewManager;
import io.cdap.cdap.app.preview.PreviewRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RunnableTaskLauncher;
import io.cdap.cdap.common.internal.remote.TaskDetails;
import io.cdap.cdap.common.internal.remote.TaskWorkerHttpHandlerInternal;
import io.cdap.cdap.common.utils.GcpMetadataTaskContextUtil;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.BodyProducer;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Internal {@link HttpHandler} for Preview system.
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3 + "/previews")
public class PreviewHttpHandlerInternal extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(PreviewHttpHandlerInternal.class);

  /**
   * Fraction of duration which will be used for calculating a range.
   */
  private static final double DURATION_FRACTION = 0.1;
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

  private final MetricsCollectionService metricsCollectionService;
  private final CConfiguration cConf;

  /**
   * If true, pod will restart once an operation finish its execution.
   */
  private final AtomicBoolean mustRestart = new AtomicBoolean(false);
  private final int concurrentRequestLimit;

//  @Inject
//  PreviewHttpHandlerInternal(PreviewManager previewManager) {
//    this.previewManager = previewManager;
//  }

  // TODO : dbshweta find the invocations for this constructor and make changes

  @Inject
  PreviewHttpHandlerInternal(CConfiguration cConf,
                             DiscoveryService discoveryService,
                             DiscoveryServiceClient discoveryServiceClient, Consumer<String> stopper,
                             MetricsCollectionService metricsCollectionService) {
    this.cConf = cConf;
    final int killAfterRequestCount = cConf.getInt(
      Constants.TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 0);
    this.runnableTaskLauncher = new RunnableTaskLauncher(cConf,
                                                         discoveryService, discoveryServiceClient, metricsCollectionService);
    this.metricsCollectionService = metricsCollectionService;
    boolean enableUserCodeIsolationEnabled = cConf.getBoolean(
      Constants.TaskWorker.USER_CODE_ISOLATION_ENABLED);
    if (enableUserCodeIsolationEnabled) {
      // Run only one request at a time in user code isolation mode.
      this.concurrentRequestLimit = 1;
    } else {
      this.concurrentRequestLimit = cConf.getInt(Constants.TaskWorker.REQUEST_LIMIT);
    }

    // Restart the service to clean up and re-claim resources after user code
    // execution.
    this.taskCompletionConsumer = (succeeded, taskDetails) -> {
      taskDetails.emitMetrics(succeeded);
      final int pendingRequests = runningRequestCount.decrementAndGet();
      requestProcessedCount.incrementAndGet();

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
        stopper.accept(className);
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
      Constants.Preview.CONTAINER_KILL_AFTER_DURATION_SECOND, 0);
    int lowerBound = (int) (duration - duration * DURATION_FRACTION);
    int upperBound = (int) (duration + duration * DURATION_FRACTION);

    if (duration <= 0) {
      return;
    }
    int waitTime = (new Random()).nextInt(upperBound - lowerBound) + lowerBound;
    int finalTaskDeadlineSeconds = calculateFinalTaskDeadlineSeconds(duration);

    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(
      Threads.createDaemonThreadFactory("preview-runner-restart"));

    executorService.scheduleWithFixedDelay(() -> {
      // we restart once all ongoing requests finish, i.e. runningRequestCount is 0.
      mustRestart.set(true);
      LOG.debug(
        "Preview runner service is about to restart in {} seconds, no new tasks will be accepted.",
        finalTaskDeadlineSeconds);
      if (runningRequestCount.get() == 0) {
        stopper.accept("");
        executorService.shutdown();
        return;
      }
      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(finalTaskDeadlineSeconds));
      } catch (InterruptedException e) {
        LOG.warn(
          "Interrupted while waiting for task completion. Stopping immediately",
          e);
      }
      stopper.accept("");
      executorService.shutdown();
    }, waitTime, finalTaskDeadlineSeconds, TimeUnit.SECONDS);
  }

  /**
    * Compute the final task deadline in Seconds where if the config {@Preview.TASK_EXECUTION_DEADLINE_SECOND}
   * is less than 0 which is not valid then use the duration instead.
   *
     * @param duration
   * @return
     */
  private int calculateFinalTaskDeadlineSeconds(int duration) {
    int previewDeadlineSeconds = cConf.getInt(
      Constants.Preview.PREVIEW_EXECUTION_DEADLINE_SECOND,
      0);

    if (previewDeadlineSeconds < 0) {
      LOG.info(
        "Preview deadline is {}, using {} value {} as the deadline instead.",
        previewDeadlineSeconds,
        Constants.Preview.CONTAINER_KILL_AFTER_DURATION_SECOND, duration);
      previewDeadlineSeconds = duration;
    }
    return previewDeadlineSeconds;
  }

  // TODO : dbshweta - check how to shift from pull to push
//  @POST
//  @Path("/requests/pull")
//  public void poll(FullHttpRequest request, HttpResponder responder) {
//    byte[] pollerInfo = Bytes.toBytes(request.content().nioBuffer());
//    PreviewRequest previewRequest = previewManager.poll(pollerInfo).orElse(null);
//
//    if (previewRequest != null) {
//      LOG.debug("Send preview request {} to poller {}", previewRequest.getProgram(),
//          Bytes.toString(pollerInfo));
//      responder.sendString(HttpResponseStatus.OK, GSON.toJson(previewRequest));
//    } else {
//      responder.sendStatus(HttpResponseStatus.OK);
//    }
//  }

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
        runnableTaskLauncher.launchRunnableTask(runnableTaskContext);
        TaskDetails taskDetails = new TaskDetails(metricsCollectionService,
                                                  startTime, runnableTaskContext.isTerminateOnComplete(),
                                                  runnableTaskRequest);
        responder.sendContent(HttpResponseStatus.OK,
                              new RunnableTaskBodyProducer(runnableTaskContext, taskCompletionConsumer, taskDetails),
                              new DefaultHttpHeaders().add(HttpHeaders.CONTENT_TYPE,
                                                           MediaType.APPLICATION_OCTET_STREAM));
      } catch (ClassNotFoundException | ClassCastException ex) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST,
                             exceptionToJson(ex),
                             new DefaultHttpHeaders().set(HttpHeaders.CONTENT_TYPE,
                                                          "application/json"));
        // Since the user class is not even loaded, no user code ran, hence it's ok to not terminate the runner
        taskCompletionConsumer.accept(false,
                                      new TaskDetails(metricsCollectionService, startTime, false,
                                                      runnableTaskRequest));
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
      taskCompletionConsumer.accept(false,
                                    new TaskDetails(metricsCollectionService, startTime, true, null));
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
}
