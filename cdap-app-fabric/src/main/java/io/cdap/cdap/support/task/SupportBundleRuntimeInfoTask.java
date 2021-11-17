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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.lib.cube.TimeValue;
import io.cdap.cdap.api.metrics.MetricTimeSeries;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.metrics.process.RemoteMetricsSystemClient;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import org.joda.time.DateTime;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Collects pipeline run info.
 */
public class SupportBundleRuntimeInfoTask implements SupportBundleTask {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleRuntimeInfoTask.class);
  private static final Gson GSON = new GsonBuilder().create();
  private final NamespaceId namespaceId;
  private final ApplicationId appId;
  private final ProgramType programType;
  private final ProgramId programName;
  private final RemoteMetricsSystemClient remoteMetricsSystemClient;
  private final File appPath;
  private final Iterable<RunRecord> runRecordList;
  private final ApplicationDetail applicationDetail;

  @Inject
  public SupportBundleRuntimeInfoTask(File appPath, NamespaceId namespaceId, ApplicationId appId,
                                      ProgramType programType, ProgramId programName,
                                      RemoteMetricsSystemClient remoteMetricsSystemClient,
                                      Iterable<RunRecord> runRecordList, ApplicationDetail applicationDetail) {
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
        JsonElement jsonElement = GSON.toJsonTree(runRecord);
        JsonObject jsonObject = (JsonObject) jsonElement;
        JsonObject metrics =
          queryMetrics(runId, applicationDetail.getConfiguration(), runRecord != null ? runRecord.getStartTs() : 0,
                       runRecord != null && runRecord.getStopTs() != null ? runRecord.getStopTs() : DateTime.now()
                         .getMillis());
        jsonObject.add("metrics", metrics);
        GSON.toJson(jsonObject, file);
      }
    }
  }

  public JsonObject queryMetrics(String runId, String configuration, long startTs, long stopTs) {
    //startTs from run time but metrics already starts before that time
    int startQueryTime = (int) (startTs - 5000);
    JsonObject metrics = new JsonObject();
    try {
      //This program type tag can be null
      String typeTag = getMetricsTag(programType);
      Map<String, String> queryTags = new HashMap<>();
      queryTags.put("namespace", namespaceId.getNamespace());
      queryTags.put("app", appId.getApplication());
      queryTags.put("run", runId);
      if (typeTag != null) {
        queryTags.put(typeTag, programName.getProgram());
      }
      Collection<String> metricsNameList = remoteMetricsSystemClient.search(queryTags);
      List<MetricTimeSeries> metricTimeSeriesList = new ArrayList<>(
        remoteMetricsSystemClient.query(startQueryTime, (int) (stopTs), queryTags, metricsNameList));
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
    }
    return metrics;
  }

  @Nullable
  private String getMetricsTag(ProgramType type) {
    switch (type) {
      case MAPREDUCE:
        return Constants.Metrics.Tag.MAPREDUCE;
      case WORKFLOW:
        return Constants.Metrics.Tag.WORKFLOW;
      case SERVICE:
        return Constants.Metrics.Tag.SERVICE;
      case SPARK:
        return Constants.Metrics.Tag.SPARK;
      case WORKER:
        return Constants.Metrics.Tag.WORKER;
      default:
        return null;
    }
  }
}
