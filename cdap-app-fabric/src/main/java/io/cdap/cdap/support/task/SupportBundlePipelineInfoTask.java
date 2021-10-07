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
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.client.MetricsClient;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ApplicationId;
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

/** Collects pipeline details */
public class SupportBundlePipelineInfoTask implements SupportBundleTask {
  private static final int maxThreadNumber = 5;
  private static final ExecutorService executor = Executors.newFixedThreadPool(maxThreadNumber);
  private static final Logger LOG = LoggerFactory.getLogger(SupportBundlePipelineInfoTask.class);
  private static final int queueSize = 10;
  private final String basePath;
  private final ApplicationClient applicationClient;
  private final ProgramClient programClient;
  private final MetricsClient metricsClient;
  private final ConcurrentHashMap<String, Integer> retryServiceMap;
  private final Gson gson;
  private final ArrayBlockingQueue<String> queue;
  private final SupportBundleStatus supportBundleStatus;
  private final String namespaceId;
  private final String appId;
  private final String workflowName;
  private final int numOfRunNeeded;
  private final String appFolderPath;

  @Inject
  public SupportBundlePipelineInfoTask(
      SupportBundleStatus supportBundleStatus,
      String namespaceId,
      String appId,
      Gson gson,
      String basePath,
      String appFolderPath,
      ApplicationClient applicationClient,
      ProgramClient programClient,
      int numOfRunNeeded,
      String workflowName,
      MetricsClient metricsClient) {
    this.supportBundleStatus = supportBundleStatus;
    this.gson = gson;
    this.basePath = basePath;
    this.namespaceId = namespaceId;
    this.appId = appId;
    this.appFolderPath = appFolderPath;
    this.applicationClient = applicationClient;
    this.programClient = programClient;
    this.numOfRunNeeded = numOfRunNeeded;
    this.workflowName = workflowName;
    this.metricsClient = metricsClient;
    retryServiceMap = new ConcurrentHashMap<>();
    queue = new ArrayBlockingQueue<>(queueSize);
  }

  public void initializeCollection() {
    String pipelineProcessing = "pipelineProcessing";
    try {
      queue.put(pipelineProcessing);
    } catch (Exception e) {
      LOG.warn("failed to add the status file ", e);
    }
  }

  /** Collects sub tasks */
  public ConcurrentHashMap<RunRecord, JsonObject> collectPipelineInfo() {
    ConcurrentHashMap<RunRecord, JsonObject> runMetricsMap = new ConcurrentHashMap<>();
    ApplicationId applicationId = new ApplicationId(namespaceId, appId);
    String pipelineProcessing = "pipelineProcessing";
    List<Future> futurePipelineInfoList = new ArrayList<>();
    try {
      queue.put(pipelineProcessing);
      SupportBundleStatusTask task = initializeTask(appId, "PipelineSupportBundleTask");
      while (!queue.isEmpty()) {
        queue.take();
        Future<SupportBundleStatusTask> futureService =
            executor.submit(
                () -> {
                  updateTask(task, basePath, "IN_PROGRESS");
                  try {
                    FileWriter file = new FileWriter(new File(appFolderPath, appId + ".json"));
                    ApplicationDetail applicationDetail = applicationClient.get(applicationId);
                    file.write(gson.toJson(applicationDetail));
                    file.flush();
                    retryServiceMap.remove(pipelineProcessing);
                  } catch (Exception e) {
                    LOG.warn("Retried three times for this pipeline info generate ", e);
                    queueTaskAfterFailed(pipelineProcessing, task);
                  }
                  return task;
                });
        if (!retryServiceMap.containsKey(pipelineProcessing)) {
          futurePipelineInfoList.add(futureService);
        }
      }
      completeProcessing(futurePipelineInfoList, basePath, "pipelineinfo");
      List<RunRecord> runRecordList = getAllRunRecordDetails(task, numOfRunNeeded);
      runMetricsMap = queryMetrics(task, runRecordList);
    } catch (Exception e) {
      LOG.warn("failed to add the status file ", e);
    }
    return runMetricsMap;
  }

  /** Collects all pipeline runs */
  public List<RunRecord> getAllRunRecordDetails(
      SupportBundleStatusTask task, Integer numOfRunNeeded) throws Exception {
    SupportBundlePipelineRunCollectionTask pipelineRunCollectionTask =
        new SupportBundlePipelineRunCollectionTask(
            supportBundleStatus,
            namespaceId,
            appId,
            gson,
            basePath,
            programClient,
            task,
            workflowName,
            numOfRunNeeded);
    pipelineRunCollectionTask.initializeCollection();
    return pipelineRunCollectionTask.getRunRecordDetails();
  }

  /** Collects pipeline run metrics */
  public ConcurrentHashMap<RunRecord, JsonObject> queryMetrics(
      SupportBundleStatusTask task, List<RunRecord> runRecordList) {
    SupportBundleMetricsCollectionTask metricCollectionTask =
        new SupportBundleMetricsCollectionTask(
            supportBundleStatus,
            namespaceId,
            appId,
            gson,
            basePath,
            task,
            workflowName,
            runRecordList,
            applicationClient,
            metricsClient);
    metricCollectionTask.initializeCollection();
    return metricCollectionTask.collectMetrics();
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
