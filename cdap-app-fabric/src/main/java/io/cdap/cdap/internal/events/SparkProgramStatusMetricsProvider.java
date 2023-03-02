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

package io.cdap.cdap.internal.events;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.MetricRetrievalException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.internal.events.http.SparkApplicationsResponse;
import io.cdap.cdap.internal.events.http.SparkApplicationsStagesResponse;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.events.ExecutionMetrics;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation for {@link MetricsProvider}. Retrieves metrics for a Spark program execution.
 */
public class SparkProgramStatusMetricsProvider implements MetricsProvider {

  private static final Logger LOG = LoggerFactory.getLogger(
      SparkProgramStatusMetricsProvider.class);
  private static final Gson gson = new Gson();

  private final String sparkApplicationsEndpoint = "/api/v1/applications";

  private final CConfiguration cConf;
  private final HttpRequestConfig httpRequestConfig;
  private final Integer maxTerminationMinutes;
  private MetricsCollectionService metricsCollectionService;

  @Inject
  public SparkProgramStatusMetricsProvider(CConfiguration cConf,
      MetricsCollectionService metricsCollectionService) {
    int connectionTimeoutMs = cConf.getInt(Constants.HTTP_CLIENT_CONNECTION_TIMEOUT_MS);
    int readTimeoutMs = cConf.getInt(Constants.HTTP_CLIENT_READ_TIMEOUT_MS);

    this.httpRequestConfig = new HttpRequestConfig(connectionTimeoutMs, readTimeoutMs, false);
    this.maxTerminationMinutes = cConf.getInt(
        Constants.Spark.SPARK_METRICS_PROVIDER_MAX_TERMINATION_MINUTES, 5);
    this.cConf = cConf;
    this.metricsCollectionService = metricsCollectionService;
  }

  @Override
  public ExecutionMetrics[] retrieveMetrics(ProgramRunId runId) throws MetricRetrievalException {
    if (!runId.getType().equals(ProgramType.SPARK)) {
      return new ExecutionMetrics[]{ExecutionMetrics.emptyMetrics()};
    }
    long startTime = System.currentTimeMillis();
    String runIdStr = runId.getRun();
    String sparkHistoricBaseURL = cConf.get(Constants.Spark.SPARK_METRICS_PROVIDER_HOST);
    if (Strings.isNullOrEmpty(sparkHistoricBaseURL)) {
      emitMetrics(runId, startTime, false);
      LOG.warn("The '{}' configuration is missing, no spark metrics will be provided",
          Constants.Spark.SPARK_METRICS_PROVIDER_HOST);
      return new ExecutionMetrics[]{};
    }
    String applicationsURL = String.format("%s%s?minEndDate=%s", sparkHistoricBaseURL,
        sparkApplicationsEndpoint, generateMaxTerminationDateParam());
    try {
      return Retries.supplyWithRetries(
          () -> {
            ExecutionMetrics[] metrics;
            HttpResponse applicationResponse;
            try {
              applicationResponse = doGet(applicationsURL);
              String attemptId = extractAttemptId(applicationResponse.getResponseBodyAsString(),
                  runIdStr);
              if (Objects.nonNull(attemptId) && !attemptId.isEmpty()) {
                HttpResponse stagesResponse;
                String stagesURL = String.format("%s%s/%s/%s/stages", sparkHistoricBaseURL,
                    sparkApplicationsEndpoint,
                    runIdStr, attemptId);
                stagesResponse = doGet(stagesURL);
                metrics = extractMetrics(stagesResponse.getResponseBodyAsString());
                if (Objects.isNull(metrics)) {
                  emitMetrics(runId, startTime, false);
                  LOG.warn("Error during metrics extraction, received null response");
                  return new ExecutionMetrics[]{};
                } else {
                  emitMetrics(runId, startTime, true);
                  return metrics;
                }
              } else {
                throw new RetryableException("Error during attemptId extraction");
              }
            } catch (IOException e) {
              LOG.warn("Error retrieving application response", e);
              throw new RetryableException(e);
            }
          }, RetryStrategies.fromConfiguration(this.cConf,
              Constants.Spark.SPARK_METRICS_PROVIDER_RETRY_STRATEGY_PREFIX),
          t -> t instanceof RetryableException);
    } catch (Exception e) {
      emitMetrics(runId, startTime, false);
      throw new MetricRetrievalException(e);
    }
  }

  private void emitMetrics(ProgramRunId runId, long startTime, boolean success) {
    Map<String, String> metricTags = new HashMap<>();
    metricTags.put(Constants.Metrics.Tag.NAMESPACE, runId.getNamespace());
    metricTags.put(Constants.Metrics.Tag.PROGRAM, runId.getProgram());
    metricTags.put(Constants.Metrics.Tag.APP, runId.getApplication());
    metricTags.put(Constants.Metrics.Tag.STATUS, success ? "success" : "failure");
    metricsCollectionService.getContext(metricTags)
        .gauge(Constants.Metrics.ProgramEvent.SPARK_METRICS_FETCH_LATENCY_MS,
            System.currentTimeMillis() - startTime);
  }

  private HttpResponse doGet(String url) throws IOException {
    URL requestURL = new URL(url);
    HttpRequest httpRequest = HttpRequest.get(requestURL).build();
    return HttpRequests.execute(httpRequest, httpRequestConfig);
  }

  @VisibleForTesting
  protected String extractAttemptId(String responseBody, String runId) {
    SparkApplicationsResponse[] responses = gson.fromJson(responseBody,
        SparkApplicationsResponse[].class);
    Optional<SparkApplicationsResponse.Attempt> attempt = Arrays.stream(responses)
        .filter(app -> runId.equals(app.getId()))
        .findFirst().flatMap(app -> Arrays.stream(app.getAttempts())
            .sorted(Comparator.comparingLong(SparkApplicationsResponse.Attempt::getEndTimeEpoch))
            .filter(SparkApplicationsResponse.Attempt::isCompleted)
            .findFirst());

    if (attempt.isPresent()) {
      return attempt.get().getAttemptId();
    } else {
      return null;
    }
  }

  @VisibleForTesting
  protected ExecutionMetrics[] extractMetrics(String responseBody) {
    SparkApplicationsStagesResponse[] stagesResponse = gson.fromJson(responseBody,
        SparkApplicationsStagesResponse[].class);
    List<ExecutionMetrics> metrics = Arrays.stream(stagesResponse)
        .filter(stage -> "COMPLETE".equals(stage.getStatus()))
        .map(stage ->
            new ExecutionMetrics(stage.getStageId(), stage.getInputRecords(),
                stage.getOutputRecords(),
                stage.getInputBytes(), stage.getOutputBytes(), stage.getShuffleReadRecords(),
                stage.getShuffleReadBytes(), stage.getShuffleWriteRecords(),
                stage.getShuffleWriteBytes())
        ).collect(Collectors.toList());
    return metrics.toArray(new ExecutionMetrics[metrics.size()]);
  }

  private String generateMaxTerminationDateParam() {
    LocalDateTime targetDate = LocalDateTime.now()
        .minus(Duration.from(Duration.ofMinutes(this.maxTerminationMinutes)));
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSz")
        .withZone(ZoneId.systemDefault());
    return targetDate.format(formatter);
  }
}
