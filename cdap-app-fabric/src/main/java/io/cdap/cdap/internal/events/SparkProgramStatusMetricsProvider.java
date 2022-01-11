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
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.internal.events.http.SparkApplicationsResponse;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;
import javax.inject.Inject;

/**
 * Implementation for {@link MetricsProvider}. Retrieves metrics for a Spark program execution.
 */
public class SparkProgramStatusMetricsProvider implements MetricsProvider {

  //Pending on actual location
  private final String sparkBaseUrlConfiguration = "spark.metrics.host";
  private final String sparkApplicationsEndpoint = "/api/v1/applications";
  //2 min max for retries strategies expressed in seconds
  private final long maxTimeRetries = 120;

  private final Logger logger = LoggerFactory.getLogger(SparkProgramStatusMetricsProvider.class);
  private final JsonParser jsonParser = new JsonParser();
  private final Gson gson = new Gson();

  private final CConfiguration cConf;
  private final HttpRequestConfig httpRequestConfig;

  @Inject
  public SparkProgramStatusMetricsProvider(CConfiguration cConf) {
    int connectionTimeout = cConf.getInt(Constants.HTTP_CLIENT_CONNECTION_TIMEOUT_MS);
    int readTimeout = cConf.getInt(Constants.HTTP_CLIENT_READ_TIMEOUT_MS);
    httpRequestConfig = new HttpRequestConfig(connectionTimeout, readTimeout, false);

    this.cConf = cConf;
  }

  @Override
  public ExecutionMetrics retrieveMetrics(ProgramRunId runId) {
    if (!runId.getType().equals(ProgramType.SPARK)) {
      return ExecutionMetrics.emptyMetrics();
    }
    String runIdStr = runId.getRun();
    String sparkHistoricBaseURL = cConf.get(sparkBaseUrlConfiguration);
    String applicationsURL = String.format("%s%s?minEndDate=%s", sparkHistoricBaseURL,
                                           sparkApplicationsEndpoint, generateMaxTerminationDateParam());
    return Retries.supplyWithRetries(() -> {
      ExecutionMetrics metrics;
      HttpResponse applicationResponse;
      try {
        applicationResponse = doGet(applicationsURL);
      } catch (IOException e) {
        logger.warn("Error retrieving application response, retrying...", e);
        throw new RetryableException(e);
      }
      String attemptId = extractAttemptId(applicationResponse.getResponseBodyAsString(), runIdStr);
      if (Objects.nonNull(attemptId) && !attemptId.isEmpty()) {
        HttpResponse stagesResponse;
        String stagesURL = String.format("%s/%s/%s/%s/stages", sparkHistoricBaseURL,
                                         sparkApplicationsEndpoint, runIdStr, attemptId);
        try {
          stagesResponse = doGet(stagesURL);
        } catch (IOException e) {
          logger.warn("Error retrieving stages response, retrying...", e);
          throw new RetryableException(e);
        }
        metrics = extractMetrics(stagesResponse.getResponseBodyAsString());
        if (Objects.isNull(metrics)) {
          logger.warn("Error during metrics extraction, retrying...");
          throw new RetryableException();
        } else {
          return metrics;
        }
      } else {
        logger.warn("Error during attemptId extraction, retrying...");
        throw new RetryableException();
      }
    }, RetryStrategies.timeLimit(maxTimeRetries, TimeUnit.SECONDS,
                                 RetryStrategies.exponentialDelay(
                                   5, 20, TimeUnit.SECONDS)));
  }

  private HttpResponse doGet(String url) throws IOException {
    URL requestURL = new URL(url);
    HttpRequest httpRequest = HttpRequest.get(requestURL).build();
    return HttpRequests.execute(httpRequest, httpRequestConfig);
  }

  @VisibleForTesting
  protected String extractAttemptId(String responseBody, String runId) {
    final String[] attemptId = new String[1];
    SparkApplicationsResponse[] responses = gson.fromJson(responseBody, SparkApplicationsResponse[].class);
    Arrays.stream(responses).filter(app -> runId.equals(app.getId()))
      .findFirst().flatMap(app -> Arrays.stream(app.getAttempts())
        .sorted(Comparator.comparingLong(SparkApplicationsResponse.Attempt::getEndTimeEpoch))
        .filter(SparkApplicationsResponse.Attempt::isCompleted)
        .findFirst()).ifPresent(attempt -> attemptId[0] = attempt.getAttemptId());

    return attemptId[0];
  }

  @VisibleForTesting
  protected ExecutionMetrics extractMetrics(String responseBody) {
    JsonArray jsonStagesResponse = jsonParser.parse(responseBody)
      .getAsJsonArray();

    return StreamSupport.stream(jsonStagesResponse.spliterator(), false)
      .filter(stage -> "COMPLETED".equals(stage.getAsJsonObject().get("status").getAsString()))
      .map(stage -> new ExecutionMetrics(
        stage.getAsJsonObject().get("inputRecords").getAsInt(),
        stage.getAsJsonObject().get("outputRecords").getAsInt(),
        stage.getAsJsonObject().get("inputBytes").getAsLong(),
        stage.getAsJsonObject().get("outputBytes").getAsLong()
      )).findFirst().orElseGet(ExecutionMetrics::nullMetrics);
  }

  private String generateMaxTerminationDateParam() {
    LocalDateTime targetDate = LocalDateTime.now().minus(Duration.from(Duration.ofMinutes(5)));
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSz")
      .withZone(ZoneId.systemDefault());
    return targetDate.format(formatter);
  }
}
