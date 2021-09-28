/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Singleton;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskParam;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
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
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
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
  private static final Logger LOG = LoggerFactory.getLogger(TaskWorkerHttpHandlerInternal.class);
  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(BasicThrowable.class,
                                                                         new BasicThrowableCodec()).create();
  private static final String SUCCESS = "success";
  private static final String FAILURE = "failure";

  private final RunnableTaskLauncher runnableTaskLauncher;
  private final BiConsumer<Boolean, TaskDetails> stopper;

  private final AtomicBoolean hasInflightRequest = new AtomicBoolean(false);

  /**
   * Holds the total number of requests that have been executed by this handler that should count toward max allowed.
   */
  private final AtomicInteger requestProcessedCount = new AtomicInteger(0);

  private final String metadataServiceEndpoint;
  private final MetricsCollectionService metricsCollectionService;

  /**
   * If true, pod will restart once an operation finish its execution.
   */
  private final AtomicBoolean mustRestart = new AtomicBoolean(false);

  public TaskWorkerHttpHandlerInternal(CConfiguration cConf, Consumer<String> stopper,
                                       MetricsCollectionService metricsCollectionService) {
    int killAfterRequestCount = cConf.getInt(Constants.TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 0);
    this.runnableTaskLauncher = new RunnableTaskLauncher(cConf);
    this.metricsCollectionService = metricsCollectionService;
    this.metadataServiceEndpoint = cConf.get(Constants.TaskWorker.METADATA_SERVICE_END_POINT);
    this.stopper = (terminate, taskDetails) -> {
      emitMetrics(taskDetails);

      if (mustRestart.get()) {
        stopper.accept(taskDetails.getClassName());
        return;
      }

      if (!terminate || taskDetails.getClassName() == null || killAfterRequestCount <= 0) {
        // No need to restart.
        requestProcessedCount.decrementAndGet();
        hasInflightRequest.set(false);
        return;
      }

      if (requestProcessedCount.get() >= killAfterRequestCount) {
        stopper.accept(taskDetails.getClassName());
      } else {
        hasInflightRequest.set(false);
      }
    };

    enablePeriodicRestart(cConf, stopper);
  }

  /**
   * If there is no ongoing request, worker pod gets restarted after a random duration is selected from the following
   * range. Otherwise, worker pod can only get restarted once the ongoing request finishes.
   * range = [Duration - DURATION_FRACTION * Duration, Duration + DURATION_FRACTION * Duration]
   * Reason: by randomizing the duration, it is guaranteed that pods do not get restarted at the same time.
   */
  private void enablePeriodicRestart(CConfiguration cConf, Consumer<String> stopper) {
    int duration = cConf.getInt(Constants.TaskWorker.CONTAINER_KILL_AFTER_DURATION_SECOND, 0);
    int lowerBound = (int) (duration - duration * DURATION_FRACTION);
    int upperBound = (int) (duration + duration * DURATION_FRACTION);
    if (lowerBound > 0) {
      int waitTime = (new Random()).nextInt(upperBound - lowerBound) + lowerBound;
      Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("task-worker-restart"))
        .schedule(
          () -> {
            if (hasInflightRequest.compareAndSet(false, true)) {
              // there is no ongoing request. pod gets restarted.
              stopper.accept("");
            }
            // we restart once a request finishes.
            // TODO: this might delay the pod restart to after executing a new request if the
            // ongoing request is already in the stopper.
            mustRestart.set(true);
          }, waitTime, TimeUnit.SECONDS);
    }
  }

  private void emitMetrics(TaskDetails taskDetails) {
    long time = System.currentTimeMillis() - taskDetails.getStartTime();
    Map<String, String> metricTags = new HashMap<>();
    metricTags.put(Constants.Metrics.Tag.CLASS, taskDetails.getClassName());
    metricTags.put(Constants.Metrics.Tag.STATUS, taskDetails.isSuccess() ? SUCCESS : FAILURE);
    metricsCollectionService.getContext(metricTags).increment(Constants.Metrics.TaskWorker.REQUEST_COUNT, 1L);
    metricsCollectionService.getContext(metricTags).gauge(Constants.Metrics.TaskWorker.REQUEST_LATENCY_MS, time);
  }

  @POST
  @Path("/run")
  public void run(FullHttpRequest request, HttpResponder responder) {
    if (!hasInflightRequest.compareAndSet(false, true)) {
      responder.sendStatus(HttpResponseStatus.TOO_MANY_REQUESTS);
      return;
    }
    requestProcessedCount.incrementAndGet();

    long startTime = System.currentTimeMillis();
    String className = null;
    try {
      RunnableTaskRequest runnableTaskRequest =
        GSON.fromJson(request.content().toString(StandardCharsets.UTF_8), RunnableTaskRequest.class);
      className = getTaskClassName(runnableTaskRequest);
      RunnableTaskContext runnableTaskContext = runnableTaskLauncher.launchRunnableTask(runnableTaskRequest);

      responder.sendContent(HttpResponseStatus.OK,
                            new RunnableTaskBodyProducer(runnableTaskContext, stopper,
                                                         new TaskDetails(true, className, startTime)),
                            new DefaultHttpHeaders().add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM));
    } catch (ClassNotFoundException | ClassCastException ex) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, exceptionToJson(ex), EmptyHttpHeaders.INSTANCE);
      // Since the user class is not even loaded, no user code ran, hence it's ok to not terminate the runner
      stopper.accept(false, new TaskDetails(false, className, startTime));
    } catch (Exception ex) {
      LOG.error("Failed to run task {}", request.content().toString(StandardCharsets.UTF_8), ex);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, exceptionToJson(ex), EmptyHttpHeaders.INSTANCE);
      // Potentially ran user code, hence terminate the runner.
      stopper.accept(true, new TaskDetails(false, className, startTime));
    }
  }

  private String getTaskClassName(RunnableTaskRequest runnableTaskRequest) {
    return Optional.ofNullable(runnableTaskRequest.getParam())
      .map(RunnableTaskParam::getEmbeddedTaskRequest)
      .map(RunnableTaskRequest::getClassName)
      .orElse(runnableTaskRequest.getClassName());
  }

  @GET
  @Path("/token")
  public void token(io.netty.handler.codec.http.HttpRequest request, HttpResponder responder) {
    if (metadataServiceEndpoint == null) {
      responder.sendString(HttpResponseStatus.NOT_IMPLEMENTED,
                           String.format("%s has not been set", Constants.TaskWorker.METADATA_SERVICE_END_POINT));
      return;
    }

    try {
      URL url = new URL(metadataServiceEndpoint);
      HttpRequest tokenRequest = HttpRequest.get(url).addHeader("Metadata-Flavor", "Google").build();
      HttpResponse tokenResponse = HttpRequests.execute(tokenRequest);
      responder.sendByteArray(HttpResponseStatus.OK, tokenResponse.getResponseBody(), EmptyHttpHeaders.INSTANCE);
    } catch (Exception ex) {
      LOG.warn("Failed to fetch token from metadata service", ex);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, exceptionToJson(ex), EmptyHttpHeaders.INSTANCE);
    }
  }

  /**
   * Return json representation of an exception.
   * Used to propagate exception across network for better surfacing errors and debuggability.
   */
  private String exceptionToJson(Exception ex) {
    BasicThrowable basicThrowable = new BasicThrowable(ex);
    return GSON.toJson(basicThrowable);
  }

  /**
   * By using BodyProducer instead of simply sending out response bytes,
   * the handler can get notified (through finished method) when sending the response is done,
   * so it can safely call the stopper to kill the worker pod.
   */
  private static class RunnableTaskBodyProducer extends BodyProducer {
    private final ByteBuffer response;
    private final BiConsumer<Boolean, TaskDetails> stopper;
    private final TaskDetails taskDetails;
    private final boolean terminateOnComplete;
    private boolean done = false;

    RunnableTaskBodyProducer(RunnableTaskContext context, BiConsumer<Boolean, TaskDetails> stopper,
                             TaskDetails taskDetails) {
      this.response = context.getResult();
      this.stopper = stopper;
      this.taskDetails = taskDetails;
      this.terminateOnComplete = context.isTerminateOnComplete();
    }

    @Override
    public ByteBuf nextChunk() {
      if (done) {
        return Unpooled.EMPTY_BUFFER;
      }

      done = true;
      return Unpooled.wrappedBuffer(response);
    }

    @Override
    public void finished() {
      stopper.accept(terminateOnComplete, taskDetails);
    }

    @Override
    public void handleError(@Nullable Throwable cause) {
      LOG.error("Error when sending chunks", cause);
      stopper.accept(terminateOnComplete, taskDetails);
    }
  }
}
