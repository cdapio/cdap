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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.lib.cube.TimeValue;
import io.cdap.cdap.api.metrics.MetricTimeSeries;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.metrics.process.RemoteMetricsSystemClient;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.RunRecord;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Collects pipeline run info.
 */
public class SupportBundleRuntimeInfoTask implements SupportBundleTask {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleRuntimeInfoTask.class);
  private static final Gson gson = new GsonBuilder().create();
  private final String namespaceId;
  private final String appId;
  private final String programType;
  private final String programName;
  private final RemoteMetricsSystemClient remoteMetricsSystemClient;
  private final String appPath;
  private final List<RunRecord> runRecordList;
  private final ApplicationDetail applicationDetail;

  @Inject
  public SupportBundleRuntimeInfoTask(String appPath, String namespaceId, String appId, String programType,
                                      String programName, RemoteMetricsSystemClient remoteMetricsSystemClient,
                                      List<RunRecord> runRecordList, ApplicationDetail applicationDetail) {
    this.namespaceId = namespaceId;
    this.appId = appId;
    this.programType = programType;
    this.programName = programName;
    this.appPath = appPath;
    this.remoteMetricsSystemClient = remoteMetricsSystemClient;
    this.runRecordList = runRecordList;
    this.applicationDetail = applicationDetail;
  }

  @Override
  public void collect() throws IOException, NotFoundException, JSONException {
    for (RunRecord runRecord : runRecordList) {
      String runId = runRecord.getPid();
      try (FileWriter file = new FileWriter(new File(appPath, runId + ".json"))) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("status", runRecord.getStatus().toString());
        jsonObject.addProperty("start", runRecord.getStartTs());
        jsonObject.addProperty("end", runRecord.getStopTs());
        jsonObject.addProperty("profileName", runRecord.getProfileId().getProfile());
        jsonObject.addProperty("runtimeArgs", runRecord.getProperties().get("runtimeArgs"));
        JsonObject metrics =
          queryMetrics(runId, applicationDetail.getConfiguration(), runRecord != null ? runRecord.getStartTs() : 0,
                       runRecord != null && runRecord.getStopTs() != null ? runRecord.getStopTs() : DateTime.now()
                         .getMillis());
        jsonObject.add("metrics", metrics);
        gson.toJson(jsonObject, file);
      } catch (IOException e) {
        LOG.error("Failed to write file with run {} ", runId, e);
        throw new IOException("Failed to write run info file ", e);
      }
    }
  }

  public JsonObject queryMetrics(String runId, String configuration, long startTs, long stopTs) {
    JsonObject metrics = new JsonObject();
    try {
      JSONObject appConf =
        configuration != null && configuration.length() > 0 ? new JSONObject(configuration) : new JSONObject();
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
      queryTags.put(programType, programName);
      List<MetricTimeSeries> metricTimeSeriesList = new ArrayList<>(
        remoteMetricsSystemClient.query((int) (startTs - 5000), (int) (stopTs), queryTags, metricsList));
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
    } catch (IOException e) {
      LOG.warn("Failed to find metrics with run {} ", runId, e);
      return null;
    } catch (JSONException e) {
      LOG.warn("JSON format error with run {} ", runId, e);
      return null;
    }
    return metrics;
  }
}
