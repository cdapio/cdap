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
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.client.MetricsClient;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.MetricQueryResult;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ApplicationId;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Collects metric from single pipeline run */
public class SupportBundleMetricsCollectionTask implements SupportBundleTask {
  private static final int maxThreadNumber = 5;
  private static final ExecutorService executor = Executors.newFixedThreadPool(maxThreadNumber);
  private static final Logger LOG =
      LoggerFactory.getLogger(SupportBundleMetricsCollectionTask.class);
  private static final int queueSize = 10;
  private final String basePath;
  private final ConcurrentHashMap<String, Integer> retryServiceMap;
  private final Gson gson;
  private final ArrayBlockingQueue<RunRecord> runRecordDetailQueue;
  private final SupportBundleStatus supportBundleStatus;
  private final String namespace;
  private final String application;
  private final String workflow;
  private final SupportBundleStatusTask supportBundleStatusTask;
  private final List<RunRecord> runRecordList;
  private final Map<String, RunRecord> runRecordMap;
  private final ApplicationClient applicationClient;
  private final MetricsClient metricsClient;

  @Inject
  public SupportBundleMetricsCollectionTask(
      SupportBundleStatus supportBundleStatus,
      String namespace,
      String application,
      Gson gson,
      String basePath,
      SupportBundleStatusTask supportBundleStatusTask,
      String workflow,
      List<RunRecord> runRecordList,
      ApplicationClient applicationClient,
      MetricsClient metricsClient) {
    this.supportBundleStatus = supportBundleStatus;
    this.gson = gson;
    this.basePath = basePath;
    this.namespace = namespace;
    this.application = application;
    this.supportBundleStatusTask = supportBundleStatusTask;
    this.workflow = workflow;
    this.runRecordList = runRecordList;
    this.runRecordMap = new ConcurrentHashMap<>();
    this.applicationClient = applicationClient;
    retryServiceMap = new ConcurrentHashMap<>();
    runRecordDetailQueue = new ArrayBlockingQueue<>(queueSize);
    this.metricsClient = metricsClient;
  }
  /** Gets the metrics details for the run id */
  public void initializeCollection() {
    for (RunRecord runRecord : runRecordList) {
      try {
        runRecordDetailQueue.put(runRecord);
      } catch (Exception e) {
        LOG.warn("fail to add into queue ");
      }
    }
  }

  public ConcurrentHashMap<RunRecord, JsonObject> collectMetrics() {
    ConcurrentHashMap<RunRecord, JsonObject> runMetricsMap = new ConcurrentHashMap<>();
    List<Future> futureMetricsList = new ArrayList<>();
    SupportBundleStatusTask subMetricCalculateTask = new SupportBundleStatusTask();
    subMetricCalculateTask.setName(application + "-metrics-collection");
    subMetricCalculateTask.setType("MetricCollectionTask");
    subMetricCalculateTask.setStartTimestamp(System.currentTimeMillis());
    supportBundleStatusTask.getSubTasks().add(subMetricCalculateTask);
    try {
      while (!runRecordDetailQueue.isEmpty()) {
        RunRecord runRecord = runRecordDetailQueue.take();
        String runId = runRecord.getPid();
        runRecordMap.put(runId, runRecord);
        Future<SupportBundleStatusTask> futureMetricsService =
            executor.submit(
                () -> {
                  updateTask(subMetricCalculateTask, basePath, "IN_PROGRESS");
                  try {
                    ApplicationDetail applicationDetail =
                        applicationClient.get(new ApplicationId(namespace, application));
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
                  } catch (Exception e) {
                    LOG.warn(
                        String.format(
                            "Retried three times for this metrics with run id %s ", runId),
                        e);
                    queueTaskAfterFailed(runId, subMetricCalculateTask);
                  }
                  return subMetricCalculateTask;
                });
        if (!retryServiceMap.containsKey(runId)) {
          futureMetricsList.add(futureMetricsService);
        }
      }
    } catch (InterruptedException e) {
      LOG.warn("Can not process pipeline info into queue ", e);
    }
    completeProcessing(futureMetricsList, basePath, "metrics-processing");
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
      queryTags.put("namespace", namespace);
      queryTags.put("app", application);
      queryTags.put("run", runId);
      queryTags.put("workflow", workflow);
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
  public synchronized void queueTaskAfterFailed(String serviceName, SupportBundleStatusTask task) {
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

  /** Execute all processing */
  public void completeProcessing(List<Future> futureList, String basePath, String processName) {
    for (Future future : futureList) {
      SupportBundleStatusTask supportBundleStatusTask = null;
      try {
        supportBundleStatusTask = (SupportBundleStatusTask) future.get(5, TimeUnit.MINUTES);
        supportBundleStatusTask.setFinishTimestamp(System.currentTimeMillis());
        updateTask(supportBundleStatusTask, basePath, "FINISHED");
      } catch (Exception e) {
        LOG.warn(
            String.format(
                "This task for %s has failed or timeout more than five minutes ", processName),
            e);
        if (supportBundleStatusTask != null) {
          supportBundleStatusTask.setFinishTimestamp(System.currentTimeMillis());
          updateTask(supportBundleStatusTask, basePath, "FAILED");
        }
      }
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
    supportBundleStatus.getTasks().add(supportBundleStatusTask);
    return supportBundleStatusTask;
  }
}
