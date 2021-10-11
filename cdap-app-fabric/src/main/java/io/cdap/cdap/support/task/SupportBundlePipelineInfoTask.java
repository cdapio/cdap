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
import com.google.inject.assistedinject.Assisted;
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.client.MetricsClient;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.support.status.SupportBundleStatus;
import io.cdap.cdap.support.status.SupportBundleStatusTask;
import io.cdap.cdap.support.status.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/** Collects pipeline details */
public class SupportBundlePipelineInfoTask implements SupportBundleTask {
  private static final Logger LOG = LoggerFactory.getLogger(SupportBundlePipelineInfoTask.class);
  private static final Gson gson = new GsonBuilder().create();
  private final String basePath;
  private final ApplicationClient applicationClient;
  private final ProgramClient programClient;
  private final MetricsClient metricsClient;
  private final SupportBundleStatus supportBundleStatus;
  private final String namespaceId;
  private final String workflowName;
  private final int numOfRunNeeded;
  private final List<ApplicationRecord> apps;
  private final ConcurrentLinkedQueue<String> queue;
  private final ConcurrentHashMap<String, Integer> retryServiceMap;
  private final int maxRetryTimes;
  private final CConfiguration cConf;

  @Inject
  public SupportBundlePipelineInfoTask(
      @Assisted SupportBundleStatus supportBundleStatus,
      @Assisted String namespaceId,
      @Assisted String basePath,
      @Assisted ApplicationClient applicationClient,
      @Assisted ProgramClient programClient,
      @Assisted int numOfRunNeeded,
      @Assisted String workflowName,
      @Assisted MetricsClient metricsClient,
      @Assisted List<ApplicationRecord> apps,
      CConfiguration cConf) {
    this.supportBundleStatus = supportBundleStatus;
    this.basePath = basePath;
    this.namespaceId = namespaceId;
    this.applicationClient = applicationClient;
    this.programClient = programClient;
    this.numOfRunNeeded = numOfRunNeeded;
    this.workflowName = workflowName;
    this.metricsClient = metricsClient;
    this.apps = apps;
    this.queue = new ConcurrentLinkedQueue<>();
    this.retryServiceMap = new ConcurrentHashMap<>();
    this.cConf = cConf;
    this.maxRetryTimes = cConf.getInt(Constants.SupportBundle.MAX_RETRY_TIMES);
  }

  public SupportBundleStatusTask initializeCollection() {
    SupportBundleStatusTask allPipelineTask =
        initializeTask("whole-pipleine-process", "PipelineSupportBundleTask");
    for (ApplicationRecord app : apps) {
      String pipelineProcessingTaskName = "pipelineProcessing-" + app.getName();
      String appId = app.getName();
      ApplicationId applicationId = new ApplicationId(namespaceId, appId);
      File appFolderPath = new File(basePath, app.getName());
      DirUtils.mkdirs(appFolderPath);
      queue.offer(pipelineProcessingTaskName);
      SupportBundleStatusTask subEachPipelineTask = new SupportBundleStatusTask();
      subEachPipelineTask.setName(appId);
      subEachPipelineTask.setType("PipelineSupportBundleTask");
      subEachPipelineTask.setStartTimestamp(System.currentTimeMillis());
      allPipelineTask.getSubTasks().add(subEachPipelineTask);
      while (!queue.isEmpty()) {
        queue.poll();
        updateTask(subEachPipelineTask, basePath, TaskStatus.IN_PROGRESS);
        try {
          FileWriter file = new FileWriter(new File(appFolderPath, appId + ".json"));
          ApplicationDetail applicationDetail = applicationClient.get(applicationId);
          file.write(gson.toJson(applicationDetail));
          file.flush();
          retryServiceMap.remove(pipelineProcessingTaskName);
          subEachPipelineTask.setFinishTimestamp(System.currentTimeMillis());
          updateTask(subEachPipelineTask, basePath, TaskStatus.FINISHED);
        } catch (Exception e) {
          LOG.warn("Retried three times for this pipeline info generate ", e);
          queueTaskAfterFailed(pipelineProcessingTaskName, subEachPipelineTask);
        }
      }
      SupportBundleRuntimeInfoTask supportBundleRuntimeInfoTask =
          new SupportBundleRuntimeInfoTask(
              basePath,
              supportBundleStatus,
              subEachPipelineTask,
              namespaceId,
              appId,
              programClient,
              workflowName,
              numOfRunNeeded,
              cConf);
      List<RunRecord> runRecordList = getAllRunRecordDetails(supportBundleRuntimeInfoTask);
      SupportBundlePipelineRunLogTask supportBundlePipelineRunLogTask =
          new SupportBundlePipelineRunLogTask(
              supportBundleStatus,
              basePath,
              appFolderPath.getPath(),
              namespaceId,
              appId,
              workflowName,
              programClient,
              subEachPipelineTask,
              runRecordList,
              applicationClient,
              metricsClient,
              cConf);
      ConcurrentHashMap<RunRecord, JsonObject> runMetricsMap =
          queryMetrics(supportBundlePipelineRunLogTask);
      supportBundleRuntimeInfoTask.generateRuntimeInfo(appFolderPath.getPath(), runMetricsMap);
      supportBundlePipelineRunLogTask.generateRunLog();
    }
    return allPipelineTask;
  }

  /** Queue the task again after exception */
  private synchronized void queueTaskAfterFailed(String serviceName, SupportBundleStatusTask task) {
    if (retryServiceMap.getOrDefault(serviceName, 0) >= maxRetryTimes) {
      updateTask(task, basePath, TaskStatus.FAILED);
    } else {
      queue.offer(serviceName);
      retryServiceMap.put(serviceName, retryServiceMap.getOrDefault(serviceName, 0) + 1);
      task.setRetries(retryServiceMap.get(serviceName));
      updateTask(task, basePath, TaskStatus.QUEUED);
    }
  }

  /** Collects all pipeline runs */
  public List<RunRecord> getAllRunRecordDetails(
      SupportBundleRuntimeInfoTask supportBundleRuntimeInfoTask) {
    supportBundleRuntimeInfoTask.initializeCollection();
    return supportBundleRuntimeInfoTask.getRunRecords();
  }

  /** Collects pipeline run metrics */
  public ConcurrentHashMap<RunRecord, JsonObject> queryMetrics(
      SupportBundlePipelineRunLogTask supportBundlePipelineRunLogTask) {
    supportBundlePipelineRunLogTask.initializeCollection();
    return supportBundlePipelineRunLogTask.collectMetrics();
  }

  /** Update status task */
  public synchronized void updateTask(
      SupportBundleStatusTask task, String basePath, TaskStatus status) {
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
