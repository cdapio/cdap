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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Collects pipeline run details */
public class SupportBundlePipelineRunCollectionTask implements SupportBundleTask {
  private static final int maxThreadNumber = 5;
  private static final ExecutorService executor = Executors.newFixedThreadPool(maxThreadNumber);
  private static final Logger LOG =
      LoggerFactory.getLogger(SupportBundlePipelineRunCollectionTask.class);
  private static final int queueSize = 10;
  private final String basePath;
  private final ProgramClient programClient;
  private final Map<String, Integer> retryServiceMap;
  private final Gson gson;
  private final ArrayBlockingQueue<String> queue;
  private final SupportBundleStatus supportBundleStatus;
  private final String namespace;
  private final String application;
  private final String workflow;
  private final int numOfRunNeeded;
  private final SupportBundleStatusTask supportBundleStatusTask;

  @Inject
  public SupportBundlePipelineRunCollectionTask(
      SupportBundleStatus supportBundleStatus,
      String namespace,
      String application,
      Gson gson,
      String basePath,
      ProgramClient programClient,
      SupportBundleStatusTask supportBundleStatusTask,
      String workflow,
      int numOfRunNeeded) {
    this.supportBundleStatus = supportBundleStatus;
    this.gson = gson;
    this.basePath = basePath;
    this.namespace = namespace;
    this.application = application;
    this.programClient = programClient;
    this.supportBundleStatusTask = supportBundleStatusTask;
    this.workflow = workflow;
    this.numOfRunNeeded = numOfRunNeeded;
    retryServiceMap = new HashMap<>();
    queue = new ArrayBlockingQueue<>(queueSize);
  }

  public void initializeCollection() {
    String runRecordDetailProcessing = "runRecordDetailProcessing";
    try {
      queue.put(runRecordDetailProcessing);
    } catch (Exception e) {
      LOG.warn("fail to add into queue ", e);
    }
  }

  public List<RunRecord> getRunRecordDetails() {
    String runRecordDetailProcessing = "runRecordDetailProcessing";
    List<Future> futureRunCollectionList = new ArrayList<>();
    List<RunRecord> runRecordList = new ArrayList<>();
    SupportBundleStatusTask subRunInfoCollectionTask = new SupportBundleStatusTask();
    subRunInfoCollectionTask.setName(application + "-run-collection");
    subRunInfoCollectionTask.setType("PipelineRunCollectionTask");
    subRunInfoCollectionTask.setStartTimestamp(System.currentTimeMillis());
    supportBundleStatusTask.getSubTasks().add(subRunInfoCollectionTask);
    try {
      while (!queue.isEmpty()) {
        queue.take();
        Future<SupportBundleStatusTask> futureRunRecordService =
            executor.submit(
                () -> {
                  updateTask(subRunInfoCollectionTask, basePath, "IN_PROGRESS");
                  try {
                    runRecordList.addAll(sortRunRecords());
                    retryServiceMap.remove(runRecordDetailProcessing);
                  } catch (Exception e) {
                    LOG.warn("Retried three times for this run record collection ", e);
                    queueTaskAfterFailed(runRecordDetailProcessing, subRunInfoCollectionTask);
                  }
                  return subRunInfoCollectionTask;
                });
        // Wait for the runrecord finish to calculate the metrics
        if (!retryServiceMap.containsKey(runRecordDetailProcessing)) {
          futureRunCollectionList.add(futureRunRecordService);
        }
      }
      completeProcessing(futureRunCollectionList, basePath, runRecordDetailProcessing);
    } catch (Exception e) {
      LOG.warn("Failed to add run record processing into queue ", e);
    }
    return runRecordList;
  }

  private List<RunRecord> sortRunRecords() {
    List<RunRecord> runRecordList = new ArrayList<>();
    try {
      ProgramId programId =
          new ProgramId(
              namespace, application, ProgramType.valueOfCategoryName("workflows"), workflow);
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

  /** Queue the task again after exception */
  public synchronized void queueTaskAfterFailed(String serviceName, SupportBundleStatusTask task) {
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
