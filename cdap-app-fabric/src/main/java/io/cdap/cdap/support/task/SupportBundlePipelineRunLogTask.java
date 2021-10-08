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
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.client.MetricsClient;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.MetricQueryResult;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.support.status.SupportBundleStatus;
import io.cdap.cdap.support.status.SupportBundleStatusTask;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/** Collects pipeline run info */
public class SupportBundlePipelineRunLogTask implements SupportBundleTask {
  private static final Logger LOG = LoggerFactory.getLogger(SupportBundlePipelineRunLogTask.class);
  private static final Gson gson = new GsonBuilder().create();
  private final String basePath;
  private final String appFolderPath;
  private final SupportBundleStatus supportBundleStatus;
  private final ProgramClient programClient;
  private final String namespaceId;
  private final String appId;
  private final String workflowName;
  private final ConcurrentHashMap<String, Integer> retryServiceMap;
  private final ArrayBlockingQueue<RunRecord> runRecordDetailQueue;
  private final SupportBundleStatusTask subEachPipelineTask;
  private final List<RunRecord> runRecordList;
  private final Map<String, RunRecord> runRecordMap;
  private final ApplicationClient applicationClient;
  private final MetricsClient metricsClient;
  private SupportBundleStatusTask subAllRunLogTask;

  @Inject
  public SupportBundlePipelineRunLogTask(
      SupportBundleStatus supportBundleStatus,
      String basePath,
      String appFolderPath,
      String namespaceId,
      String appId,
      String workflowName,
      ProgramClient programClient,
      SupportBundleStatusTask subEachPipelineTask,
      List<RunRecord> runRecordList,
      ApplicationClient applicationClient,
      MetricsClient metricsClient) {
    int queueSize = 100;
    this.supportBundleStatus = supportBundleStatus;
    this.basePath = basePath;
    this.appFolderPath = appFolderPath;
    this.programClient = programClient;
    this.namespaceId = namespaceId;
    this.appId = appId;
    this.workflowName = workflowName;
    this.retryServiceMap = new ConcurrentHashMap<>();
    this.runRecordDetailQueue = new ArrayBlockingQueue<>(queueSize);
    this.subEachPipelineTask = subEachPipelineTask;
    this.runRecordList = runRecordList;
    this.runRecordMap = new ConcurrentHashMap<>();
    this.applicationClient = applicationClient;
    this.metricsClient = metricsClient;
    this.subAllRunLogTask = new SupportBundleStatusTask();
  }

  public SupportBundleStatusTask initializeCollection() {
    subAllRunLogTask = initializeTask(appId + "-run-log", "SupportBundlePipelineRunLogTask");
    return subAllRunLogTask;
  }

  public void generateRunLog() {
    for (RunRecord runRecord : runRecordList) {
      if (runRecordDetailQueue.size() >= 100) {
        break;
      }
      runRecordDetailQueue.offer(runRecord);
    }

    try {
      while (!runRecordDetailQueue.isEmpty()) {
        RunRecord runRecord = runRecordDetailQueue.take();
        String runId = runRecord.getPid();
        runRecordMap.put(runId, runRecord);
        SupportBundleStatusTask subEachRunLogTask = new SupportBundleStatusTask();
        subEachRunLogTask.setName(runId + "-log");
        subEachRunLogTask.setType("PipelineRunLogSupportBundleTask");
        subEachRunLogTask.setStartTimestamp(System.currentTimeMillis());
        subAllRunLogTask.getSubTasks().add(subEachRunLogTask);
        updateTask(subEachRunLogTask, basePath, "IN_PROGRESS");
        try {
          long currentTimeMillis = System.currentTimeMillis();
          long fromMillis = currentTimeMillis - TimeUnit.DAYS.toMillis(1);
          FileWriter file = new FileWriter(new File(appFolderPath, runId + "-log.txt"));
          ProgramId programId =
              new ProgramId(
                  namespaceId, appId, ProgramType.valueOfCategoryName("workflows"), workflowName);
          String runLog =
              programClient.getProgramRunLogs(
                  programId, runId, fromMillis / 1000, currentTimeMillis / 1000);
          file.write(runLog);
          file.flush();
          retryServiceMap.remove(runId);
          subEachRunLogTask.setFinishTimestamp(System.currentTimeMillis());
          updateTask(subEachRunLogTask, basePath, "FINISHED");
        } catch (Exception e) {
          LOG.warn(String.format("Retried three times for this metrics with run id %s ", runId), e);
          queueTaskAfterFailed(runId, subEachRunLogTask);
        }
      }
    } catch (InterruptedException e) {
      LOG.warn("Can not process pipeline info into queue ", e);
    }
    subAllRunLogTask.setFinishTimestamp(System.currentTimeMillis());
    updateTask(subAllRunLogTask, basePath, "FINISHED");
  }

  public ConcurrentHashMap<RunRecord, JsonObject> collectMetrics() {
    ConcurrentHashMap<RunRecord, JsonObject> runMetricsMap = new ConcurrentHashMap<>();
    for (RunRecord runRecord : runRecordList) {
      if (runRecordDetailQueue.size() >= 100) {
        break;
      }
      runRecordDetailQueue.offer(runRecord);
    }
    try {
      while (!runRecordDetailQueue.isEmpty()) {
        RunRecord runRecord = runRecordDetailQueue.take();
        String runId = runRecord.getPid();
        runRecordMap.put(runId, runRecord);
        SupportBundleStatusTask subMetricCalculateTask = new SupportBundleStatusTask();
        subMetricCalculateTask.setName(runId + "-metrics-collection");
        subMetricCalculateTask.setType("PipelineRunLogSupportBundleTask");
        subMetricCalculateTask.setStartTimestamp(System.currentTimeMillis());
        subAllRunLogTask.getSubTasks().add(subMetricCalculateTask);
        updateTask(subMetricCalculateTask, basePath, "IN_PROGRESS");
        try {
          ApplicationDetail applicationDetail =
              applicationClient.get(new ApplicationId(namespaceId, appId));
          JsonObject metrics =
              queryMetrics(
                  runId,
                  applicationDetail.getConfiguration(),
                  runRecord != null ? runRecord.getStartTs() : 0,
                  runRecord != null && runRecord.getStopTs() != null
                      ? runRecord.getStopTs()
                      : DateTime.now().getMillis());
          retryServiceMap.remove(runId);
          runMetricsMap.put(runRecord, metrics);
          subMetricCalculateTask.setFinishTimestamp(System.currentTimeMillis());
          updateTask(subMetricCalculateTask, basePath, "FINISHED");
        } catch (Exception e) {
          LOG.warn(String.format("Retried three times for this metrics with run id %s ", runId), e);
          queueTaskAfterFailed(runId, subMetricCalculateTask);
        }
      }
    } catch (InterruptedException e) {
      LOG.warn("Can not process pipeline info into queue ", e);
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
      Map<String, String> queryParams = new HashMap<>();
      queryParams.put(Constants.AppFabric.QUERY_PARAM_START_TIME, String.valueOf(startTs - 5000));
      queryParams.put(Constants.AppFabric.QUERY_PARAM_END_TIME, String.valueOf(stopTs));
      MetricQueryResult metricQueryResult =
          metricsClient.query(queryTags, metricsList, new ArrayList<>(), queryParams);
      for (MetricQueryResult.TimeSeries timeSeries : metricQueryResult.getSeries()) {
        if (!metrics.has(timeSeries.getMetricName())) {
          metrics.add(timeSeries.getMetricName(), new JsonArray());
        }
        for (MetricQueryResult.TimeValue timeValue : timeSeries.getData()) {
          JsonObject time = new JsonObject();
          time.addProperty("time", timeValue.getTime());
          time.addProperty("value", timeValue.getValue());
          metrics.getAsJsonArray(timeSeries.getMetricName()).add(time);
        }
      }
    } catch (Exception e) {
      LOG.warn("Json error ", e);
    }
    return metrics;
  }

  /** Queue the task again after exception */
  private synchronized void queueTaskAfterFailed(String serviceName, SupportBundleStatusTask task) {
    if (retryServiceMap.getOrDefault(serviceName, 0) >= 3) {
      updateTask(task, basePath, "FAILED");
    } else {
      try {
        runRecordDetailQueue.put(runRecordMap.get(serviceName));
      } catch (InterruptedException e) {
        LOG.warn("failed to queue put ", e);
      }
      retryServiceMap.put(serviceName, retryServiceMap.getOrDefault(serviceName, 0) + 1);
      task.setRetries(retryServiceMap.get(serviceName));
      updateTask(task, basePath, "QUEUED");
    }
  }

  /** Update status task */
  public synchronized void updateTask(
      SupportBundleStatusTask task, String basePath, String status) {
    try {
      task.setStatus(status);
      addToStatus(basePath);
    } catch (Exception e) {
      LOG.warn("failed to update the status file ", e);
    }
  }

  /** Update status file */
  public synchronized void addToStatus(String basePath) {
    try (FileWriter statusFile = new FileWriter(new File(basePath, "status.json"))) {
      statusFile.write(gson.toJson(supportBundleStatus));
      statusFile.flush();
    } catch (Exception e) {
      LOG.error("Can not update status file ", e);
    }
  }

  /** Start a new status task */
  public SupportBundleStatusTask initializeTask(String name, String type) {
    SupportBundleStatusTask supportBundleStatusTask = new SupportBundleStatusTask();
    supportBundleStatusTask.setName(name);
    supportBundleStatusTask.setType(type);
    Long startTs = System.currentTimeMillis();
    supportBundleStatusTask.setStartTimestamp(startTs);
    subEachPipelineTask.getSubTasks().add(supportBundleStatusTask);
    return supportBundleStatusTask;
  }
}
