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

import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RunnableTaskParam;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.Constants;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Class for holding details of a task
 */
public class TaskDetails {

  private static final String SUCCESS = "success";
  private static final String FAILURE = "failure";

  private final MetricsCollectionService metricsCollectionService;
  private final long startTime;
  private final RunnableTaskRequest request;
  private final boolean terminateOnComplete;

  public TaskDetails(MetricsCollectionService metricsCollectionService, long startTime,
      boolean terminateOnComplete,
      @Nullable RunnableTaskRequest request) {
    this.metricsCollectionService = metricsCollectionService;
    this.startTime = startTime;
    this.terminateOnComplete = terminateOnComplete;
    this.request = request;
  }

  public void emitMetrics(boolean succeeded) {
    long time = System.currentTimeMillis() - startTime;
    Map<String, String> metricTags = new HashMap<>();
    metricTags.put(Constants.Metrics.Tag.CLASS, Optional.ofNullable(getClassName()).orElse(""));
    metricTags.put(Constants.Metrics.Tag.STATUS, succeeded ? SUCCESS : FAILURE);
    metricsCollectionService.getContext(metricTags)
        .increment(Constants.Metrics.TaskWorker.REQUEST_COUNT, 1L);
    metricsCollectionService.getContext(metricTags)
        .gauge(Constants.Metrics.TaskWorker.REQUEST_LATENCY_MS, time);
  }

  public boolean isTerminateOnComplete() {
    return terminateOnComplete;
  }

  @Nullable
  public String getClassName() {
    if (request == null) {
      return null;
    }
    return Optional.ofNullable(request.getParam())
        .map(RunnableTaskParam::getEmbeddedTaskRequest)
        .map(RunnableTaskRequest::getClassName)
        .orElse(request.getClassName());
  }
}
