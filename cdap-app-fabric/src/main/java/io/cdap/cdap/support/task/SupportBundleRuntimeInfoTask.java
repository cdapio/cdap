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
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.support.status.SupportBundleStatus;
import io.cdap.cdap.support.status.SupportBundleStatusTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** Collects pipeline run info */
public class SupportBundleRuntimeInfoTask implements SupportBundleTask {
  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleRuntimeInfoTask.class);
  private static final Gson gson = new GsonBuilder().create();
  private final String basePath;
  private final SupportBundleStatus supportBundleStatus;
  private final SupportBundleStatusTask subEachPipelineTask;
  private final String namespaceId;
  private final String appId;
  private final ProgramClient programClient;
  private final String workflow;
  private final int numOfRunNeeded;
  private final ArrayBlockingQueue<Object> queue;
  private final ConcurrentHashMap<String, Integer> retryServiceMap;
  private SupportBundleStatusTask subAllRunRecordsTask;

  @Inject
  public SupportBundleRuntimeInfoTask(String basePath,
                                      SupportBundleStatus supportBundleStatus,
                                      SupportBundleStatusTask subEachPipelineTask,
                                      String namespaceId, String appId, ProgramClient programClient,
                                      String workflow, int numOfRunNeeded) {
    int queueSize = 100;
    this.basePath = basePath;
    this.supportBundleStatus = supportBundleStatus;
    this.subEachPipelineTask = subEachPipelineTask;
    this.namespaceId = namespaceId;
    this.appId = appId;
    this.programClient = programClient;
    this.workflow = workflow;
    this.numOfRunNeeded = numOfRunNeeded;
    this.queue = new ArrayBlockingQueue<>(queueSize);
    this.retryServiceMap = new ConcurrentHashMap<>();
    this.subAllRunRecordsTask = new SupportBundleStatusTask();
  }

  public SupportBundleStatusTask initializeCollection() {
    subAllRunRecordsTask = initializeTask(appId + "-run-collection", "SupportBundleRuntimeInfoTask");
    return subAllRunRecordsTask;
  }

  /** Generates pipeline run info */
  public void generateRuntimeInfo(
      String appPath, ConcurrentHashMap<RunRecord, JsonObject> runMetricsMap) {
    for (RunRecord runRecord : runMetricsMap.keySet()) {
      if (queue.size() >= 100) {
        break;
      }
      queue.offer(runRecord);
    }
    while (!queue.isEmpty()) {
      try {
        RunRecord runRecord = (RunRecord) queue.take();
        String runId = runRecord.getPid();
        SupportBundleStatusTask subEachRunInfoTask = new SupportBundleStatusTask();
        subEachRunInfoTask.setName(runId + "-info");
        subEachRunInfoTask.setType("SupportBundleRuntimeInfoTask");
        subEachRunInfoTask.setStartTimestamp(System.currentTimeMillis());
        subAllRunRecordsTask.getSubTasks().add(subEachRunInfoTask);
        try {
          FileWriter file = new FileWriter(new File(appPath, runId + ".json"));
          JsonObject jsonObject = new JsonObject();
          jsonObject.addProperty("status", runRecord.getStatus().toString());
          jsonObject.addProperty("start", runRecord.getStartTs());
          jsonObject.addProperty("end", runRecord.getStopTs());
          jsonObject.addProperty("profileName", runRecord.getProfileId().getProfile());
          jsonObject.addProperty("runtimeArgs", runRecord.getProperties().get("runtimeArgs"));
          jsonObject.add("metrics", runMetricsMap.get(runRecord));
          file.write(gson.toJson(jsonObject));
          file.flush();
          subEachRunInfoTask.setFinishTimestamp(System.currentTimeMillis());
          updateTask(subEachRunInfoTask, basePath, "FINISHED");
        } catch (Exception e) {
          queueTaskAfterFailed(runId, subEachRunInfoTask);
        }
      } catch (Exception e) {
        LOG.warn("Can not generate run info file ", e);
      }
    }

    subAllRunRecordsTask.setFinishTimestamp(System.currentTimeMillis());
    updateTask(subAllRunRecordsTask, basePath, "FINISHED");
  }

  public List<RunRecord> getRunRecords() {
    String runRecordDetailProcessing = "runRecordDetailProcessing";
    List<RunRecord> runRecordList = new ArrayList<>();
    queue.offer(runRecordDetailProcessing);
    while (!queue.isEmpty()) {
      try {
        queue.take();
        updateTask(subAllRunRecordsTask, basePath, "IN_PROGRESS");
        runRecordList.addAll(sortRunRecords());
        retryServiceMap.remove(runRecordDetailProcessing);
      } catch (Exception e) {
        LOG.warn("Retried three times for this run record collection ", e);
        queueTaskAfterFailed(runRecordDetailProcessing, subAllRunRecordsTask);
      }
    }
    return runRecordList;
  }

  /** Queue the task again after exception */
  private synchronized void queueTaskAfterFailed(String serviceName, SupportBundleStatusTask task) {
    if (retryServiceMap.getOrDefault(serviceName, 0) >= 3) {
      updateTask(task, basePath, "FAILED");
    } else {
      try {
        queue.put(serviceName);
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

  private List<RunRecord> sortRunRecords() {
    List<RunRecord> runRecordList = new ArrayList<>();
    try {
      ProgramId programId =
        new ProgramId(
          namespaceId, appId, ProgramType.valueOfCategoryName("workflows"), workflow);
      List<RunRecord> allRunRecordList =
        programClient.getAllProgramRuns(programId, 0, Long.MAX_VALUE, 100);

      List<RunRecord> sortedRunRecordList =
        allRunRecordList.stream()
          .filter(run -> run.getStatus().isEndState())
          .sorted(
            Collections.reverseOrder(
              (a, b) -> {
                if (a.getStartTs() <= b.getStartTs()) {
                  return 1;
                }
                return -1;
              }))
          .collect(Collectors.toList());
      // Gets the last N runs info
      for (RunRecord runRecord : sortedRunRecordList) {
        if (runRecordList.size() < numOfRunNeeded) {
          runRecordList.add(runRecord);
        }
      }
    } catch (Exception e) {
      LOG.warn("Failed to get program runs ", e);
    }
    return runRecordList;
  }
}
