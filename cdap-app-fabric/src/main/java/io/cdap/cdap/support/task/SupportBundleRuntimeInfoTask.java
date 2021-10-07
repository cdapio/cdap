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
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.support.status.SupportBundleStatus;
import io.cdap.cdap.support.status.SupportBundleStatusTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Collects pipeline run info */
public class SupportBundleRuntimeInfoTask implements SupportBundleTask {
  private static final int maxThreadNumber = 5;
  private static final ExecutorService executor = Executors.newFixedThreadPool(maxThreadNumber);
  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleRuntimeInfoTask.class);
  private static final int queueSize = 10;
  private final String basePath;
  private final String appFolderPath;
  private final ConcurrentHashMap<String, Integer> retryServiceMap;
  private final Gson gson;
  private final ArrayBlockingQueue<RunRecord> runRecordDetailQueue;
  private final SupportBundleStatus supportBundleStatus;
  private final ConcurrentHashMap<String, RunRecord> runRecordMap;
  private final ConcurrentHashMap<RunRecord, JsonObject> runMetricsMap;

  @Inject
  public SupportBundleRuntimeInfoTask(
      SupportBundleStatus supportBundleStatus,
      Gson gson,
      String basePath,
      String appFolderPath,
      ConcurrentHashMap<RunRecord, JsonObject> runMetricsMap) {
    this.runRecordMap = new ConcurrentHashMap<>();
    this.supportBundleStatus = supportBundleStatus;
    this.gson = gson;
    this.basePath = basePath;
    this.appFolderPath = appFolderPath;
    this.runMetricsMap = runMetricsMap;
    retryServiceMap = new ConcurrentHashMap<>();
    runRecordDetailQueue = new ArrayBlockingQueue<>(queueSize);
  }

  public void initializeCollection() {
    List<Future> futureRunInfoList = new ArrayList<>();
    try {
      for (RunRecord runRecord : runMetricsMap.keySet()) {
        runRecordDetailQueue.put(runRecord);
      }
      while (!runRecordDetailQueue.isEmpty()) {
        RunRecord runRecord = runRecordDetailQueue.take();
        String runId = runRecord.getPid();
        runRecordMap.put(runId, runRecord);
        SupportBundleStatusTask task =
            initializeTask(runId + "-info", "SupportBundleRuntimeInfoTask");
        Future<SupportBundleStatusTask> futureRunInfoService =
            executor.submit(
                () -> {
                  try {
                    updateTask(task, basePath, "IN_PROGRESS");
                    generateRuntimeInfo(
                        appFolderPath,
                        runRecord.getPid(),
                        runRecord,
                        runMetricsMap.get(runRecord),
                        gson);
                    retryServiceMap.remove(runId);
                  } catch (Exception e) {
                    LOG.warn(
                        String.format(
                            "Retried three times for this run id %s to generate run info ", runId),
                        e);
                    queueTaskAfterFailed(runId, task);
                  }
                  return task;
                });
        if (!retryServiceMap.containsKey(runId)) {
          futureRunInfoList.add(futureRunInfoService);
        }
      }
      completeProcessing(futureRunInfoList, basePath, "runInfo");
    } catch (InterruptedException e) {
      LOG.warn("Can not process run info into queue ", e);
    }
  }
  /** Generates pipeline run info */
  public void generateRuntimeInfo(
      String appPath, String latestRunId, RunRecord latestRunRecord, JsonObject metrics, Gson gson)
      throws Exception {
    FileWriter file = new FileWriter(new File(appPath, latestRunId + ".json"));
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("status", latestRunRecord.getStatus().toString());
    jsonObject.addProperty("start", latestRunRecord.getStartTs());
    jsonObject.addProperty("end", latestRunRecord.getStopTs());
    jsonObject.addProperty("profileName", latestRunRecord.getProfileId().getProfile());
    jsonObject.addProperty("runtimeArgs", latestRunRecord.getProperties().get("runtimeArgs"));
    jsonObject.add("metrics", metrics);
    file.write(gson.toJson(jsonObject));
    file.flush();
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
