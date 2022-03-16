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

package io.cdap.cdap.internal.app.worker;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Injector;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskParam;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.http.AbstractHttpHandler;
import io.netty.handler.codec.http.FullHttpRequest;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractWorkerHttpHandlerInternal extends AbstractHttpHandler {

  private static final String SUCCESS = "success";
  private static final String FAILURE = "failure";
  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(BasicThrowable.class,
      new BasicThrowableCodec()).create();

  /**
   * Holds the total number of requests that have been executed by this handler that should count
   * toward max allowed.
   */
  protected final AtomicInteger requestProcessedCount = new AtomicInteger(0);
  protected final MetricsCollectionService metricsCollectionService;
  private final RunnableTaskLauncher runnableTaskLauncher;

  protected AbstractWorkerHttpHandlerInternal(MetricsCollectionService metricsCollectionService,
      Injector injector) {
    this.metricsCollectionService = metricsCollectionService;
    this.runnableTaskLauncher = new RunnableTaskLauncher(injector);
  }

  protected String getTaskClassName(RunnableTaskRequest runnableTaskRequest) {
    return Optional.ofNullable(runnableTaskRequest.getParam())
        .map(RunnableTaskParam::getEmbeddedTaskRequest)
        .map(RunnableTaskRequest::getClassName)
        .orElse(runnableTaskRequest.getClassName());
  }

  protected void emitMetrics(TaskDetails taskDetails, String requestCountMetricName,
      String requestLatencyMetricName) {
    long time = System.currentTimeMillis() - taskDetails.getStartTime();
    Map<String, String> metricTags = new HashMap<>();
    metricTags.put(Constants.Metrics.Tag.CLASS, taskDetails.getClassName());
    metricTags.put(Constants.Metrics.Tag.STATUS, taskDetails.isSuccess() ? SUCCESS : FAILURE);
    metricsCollectionService.getContext(metricTags).increment(requestCountMetricName, 1L);
    metricsCollectionService.getContext(metricTags).gauge(requestLatencyMetricName, time);
  }

  /**
   * Return json representation of an exception. Used to propagate exception across network for
   * better surfacing errors and debuggability.
   */
  protected String exceptionToJson(Exception ex) {
    BasicThrowable basicThrowable = new BasicThrowable(ex);
    return GSON.toJson(basicThrowable);
  }

  protected RunnableTaskRequest getRunnableTaskRequest(FullHttpRequest httpRequest) {
    return GSON.fromJson(httpRequest.content().toString(StandardCharsets.UTF_8),
        RunnableTaskRequest.class);
  }

  protected RunnableTaskContext launchRunnableTask(RunnableTaskRequest runnableTaskRequest)
      throws Exception {
    return runnableTaskLauncher.launchRunnableTask(runnableTaskRequest);
  }

}
