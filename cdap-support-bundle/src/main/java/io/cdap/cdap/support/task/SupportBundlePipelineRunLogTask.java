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

package io.cdap.cdap.support.task;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.lib.cube.TimeValue;
import io.cdap.cdap.api.metrics.MetricTimeSeries;
import io.cdap.cdap.logging.gateway.handlers.RemoteProgramLogsFetcher;
import io.cdap.cdap.metadata.RemoteApplicationDetailFetcher;
import io.cdap.cdap.metrics.process.RemoteMetricsSystemClient;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramId;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Collects pipeline run info
 */
public class SupportBundlePipelineRunLogTask implements SupportBundleTask {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundlePipelineRunLogTask.class);
  private final String appFolderPath;
  private final RemoteProgramLogsFetcher remoteProgramLogsFetcher;
  private final String namespaceId;
  private final String appId;
  private final String workflowName;
  private final List<RunRecord> runRecordList;
  private final Map<String, RunRecord> runRecordMap;
  private final RemoteApplicationDetailFetcher remoteApplicationDetailFetcher;
  private final RemoteMetricsSystemClient remoteMetricsSystemClient;

  @Inject
  public SupportBundlePipelineRunLogTask(String appFolderPath, String namespaceId, String appId,
                                         String workflowName,
                                         RemoteProgramLogsFetcher remoteProgramLogsFetcher,
                                         List<RunRecord> runRecordList,
                                         RemoteApplicationDetailFetcher remoteApplicationDetailFetcher,
                                         RemoteMetricsSystemClient remoteMetricsSystemClient) {
    this.appFolderPath = appFolderPath;
    this.remoteProgramLogsFetcher = remoteProgramLogsFetcher;
    this.namespaceId = namespaceId;
    this.appId = appId;
    this.workflowName = workflowName;
    this.runRecordList = runRecordList;
    this.runRecordMap = new ConcurrentHashMap<>();
    this.remoteApplicationDetailFetcher = remoteApplicationDetailFetcher;
    this.remoteMetricsSystemClient = remoteMetricsSystemClient;
  }

  public void initializeCollection() {
    for (RunRecord runRecord : runRecordList) {
      String runId = runRecord.getPid();
      runRecordMap.put(runId, runRecord);
      try {
        long currentTimeMillis = System.currentTimeMillis();
        long fromMillis = currentTimeMillis - TimeUnit.DAYS.toMillis(1);
        FileWriter file = new FileWriter(new File(appFolderPath, runId + "-log.txt"));
        ProgramId programId =
            new ProgramId(
                namespaceId, appId, ProgramType.valueOfCategoryName("workflows"), workflowName);
        String runLog =
            remoteProgramLogsFetcher.getProgramRunLogs(
                programId, runId, fromMillis / 1000, currentTimeMillis / 1000);
        file.write(runLog);
      } catch (Exception e) {
        LOG.warn("Retried three times for this metrics with run id {} ", runId, e);
      }
    }
  }

  public Map<RunRecord, JsonObject> collectMetrics() {
    Map<RunRecord, JsonObject> runMetricsMap = new ConcurrentHashMap<>();
    for (RunRecord runRecord : runRecordList) {
      String runId = runRecord.getPid();
      runRecordMap.put(runId, runRecord);
      try {
        ApplicationDetail applicationDetail =
            remoteApplicationDetailFetcher.get(new ApplicationId(namespaceId, appId));
        JsonObject metrics =
            queryMetrics(
                runId,
                applicationDetail.getConfiguration(),
                runRecord != null ? runRecord.getStartTs() : 0,
                runRecord != null && runRecord.getStopTs() != null
                    ? runRecord.getStopTs()
                    : DateTime.now().getMillis());
        runMetricsMap.put(runRecord, metrics);
      } catch (Exception e) {
        LOG.warn("Retried three times for this metrics with run id {} ", runId, e);
      }
    }
    return runMetricsMap;
  }

  public JsonObject queryMetrics(String runId, String configuration, long startTs, long stopTs) {
    JsonObject metrics = new JsonObject();
    try {
      JSONObject appConf =
          configuration != null && configuration.length() > 0
              ? new JSONObject(configuration)
              : new JSONObject();
      List<String> metricsList = new ArrayList<>();
      JSONArray stages = appConf.has("stages") ? appConf.getJSONArray("stages") : new JSONArray();
      for (int i = 0; i < stages.length(); i++) {
        JSONObject stageName = stages.getJSONObject(i);
        metricsList.add(String.format("user.%s.records.out", stageName.getString("name")));
        metricsList.add(String.format("user.%s.records.in", stageName.getString("name")));
        metricsList.add(String.format("user.%s.process.time.avg", stageName.getString("name")));
      }
      Map<String, String> queryTags = new HashMap<>();
      queryTags.put("namespace", namespaceId);
      queryTags.put("app", appId);
      queryTags.put("run", runId);
      queryTags.put("workflow", workflowName);
      List<MetricTimeSeries> metricTimeSeriesList =
          new ArrayList<>(remoteMetricsSystemClient.query((int) (startTs
              - 5000), (int) (stopTs), queryTags, metricsList));
      for (MetricTimeSeries timeSeries : metricTimeSeriesList) {
        if (!metrics.has(timeSeries.getMetricName())) {
          metrics.add(timeSeries.getMetricName(), new JsonArray());
        }
        for (TimeValue timeValue : timeSeries.getTimeValues()) {
          JsonObject time = new JsonObject();
          time.addProperty("time", timeValue.getTimestamp());
          time.addProperty("value", timeValue.getValue());
          metrics.getAsJsonArray(timeSeries.getMetricName()).add(time);
        }
      }
    } catch (Exception e) {
      LOG.warn("Json error ", e);
    }
    return metrics;
  }
}
