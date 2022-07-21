/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker.system;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskParam;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.worker.RunnableTaskLauncher;
import io.cdap.cdap.internal.app.worker.TaskDetails;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

/**
 * Internal {@link HttpHandler} for System worker.
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3 + "/system")
public class SystemWorkerHttpHandlerInternal extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(SystemWorkerHttpHandlerInternal.class);
  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(BasicThrowable.class,
      new BasicThrowableCodec()).create();
  private static final String SUCCESS = "success";
  private static final String FAILURE = "failure";
  private final int requestLimit;

  private final RunnableTaskLauncher runnableTaskLauncher;

  /**
   * Holds the total number of requests that have been executed by this handler that should count toward max allowed.
   */
  private final AtomicInteger requestProcessedCount = new AtomicInteger(0);

  private final MetricsCollectionService metricsCollectionService;

  public SystemWorkerHttpHandlerInternal(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
                                         Injector injector) {
    this.runnableTaskLauncher = new RunnableTaskLauncher(injector);
    this.metricsCollectionService = metricsCollectionService;
    this.requestLimit = cConf.getInt(Constants.SystemWorker.REQUEST_LIMIT);
  }

  private void emitMetrics(TaskDetails taskDetails) {
    long time = System.currentTimeMillis() - taskDetails.getStartTime();
    Map<String, String> metricTags = new HashMap<>();
    metricTags.put(Constants.Metrics.Tag.CLASS, taskDetails.getClassName());
    metricTags.put(Constants.Metrics.Tag.STATUS, taskDetails.isSuccess() ? SUCCESS : FAILURE);
    metricsCollectionService.getContext(metricTags).increment(Constants.Metrics.SystemWorker.REQUEST_COUNT, 1L);
    metricsCollectionService.getContext(metricTags).gauge(Constants.Metrics.SystemWorker.REQUEST_LATENCY_MS, time);
  }

  @POST
  @Path("/run")
  public void run(FullHttpRequest request, HttpResponder responder) {
    if (requestProcessedCount.incrementAndGet() > requestLimit) {
      responder.sendStatus(HttpResponseStatus.TOO_MANY_REQUESTS);
      return;
    }


    long startTime = System.currentTimeMillis();
    String className = null;
    try {
      RunnableTaskRequest runnableTaskRequest =
          GSON.fromJson(request.content().toString(StandardCharsets.UTF_8), RunnableTaskRequest.class);
      className = getTaskClassName(runnableTaskRequest);
      RunnableTaskContext runnableTaskContext = runnableTaskLauncher.launchRunnableTask(runnableTaskRequest);
      TaskDetails taskDetails = new TaskDetails(true, className, startTime);
      emitMetrics(taskDetails);
      responder.sendContent(HttpResponseStatus.OK,
          new RunnableTaskBodyProducer(runnableTaskContext, taskDetails),
          new DefaultHttpHeaders().add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM));
    } catch (ClassNotFoundException | ClassCastException ex) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, exceptionToJson(ex), EmptyHttpHeaders.INSTANCE);
    } catch (Exception ex) {
      LOG.error("Failed to run task {}", request.content().toString(StandardCharsets.UTF_8), ex);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, exceptionToJson(ex), EmptyHttpHeaders.INSTANCE);
    }
    requestProcessedCount.decrementAndGet();
  }

  private String getTaskClassName(RunnableTaskRequest runnableTaskRequest) {
    return Optional.ofNullable(runnableTaskRequest.getParam())
        .map(RunnableTaskParam::getEmbeddedTaskRequest)
        .map(RunnableTaskRequest::getClassName)
        .orElse(runnableTaskRequest.getClassName());
  }

  /**
   * Return json representation of an exception.
   * Used to propagate exception across network for better surfacing errors and debuggability.
   */
  private String exceptionToJson(Exception ex) {
    BasicThrowable basicThrowable = new BasicThrowable(ex);
    return GSON.toJson(basicThrowable);
  }

  private static class RunnableTaskBodyProducer extends BodyProducer {
    private final ByteBuffer response;
    private final TaskDetails taskDetails;
    private boolean done;

    RunnableTaskBodyProducer(RunnableTaskContext context, TaskDetails taskDetails) {
      this.response = context.getResult();
      this.taskDetails = taskDetails;
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
    public void finished() {}

    @Override
    public void handleError(@Nullable Throwable cause) {
      LOG.error("Error when sending chunks", cause);
    }
  }
}
