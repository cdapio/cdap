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
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.support.status.SupportBundleStatus;
import io.cdap.cdap.support.status.SupportBundleStatusTask;
import io.cdap.cdap.support.status.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public class SupportBundleSystemLogTask implements SupportBundleTask {
  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleSystemLogTask.class);
  private static final Gson gson = new GsonBuilder().create();
  private final String basePath;
  private final String systemLogPath;
  private final ProgramClient programClient;
  private final SupportBundleStatus supportBundleStatus;
  private final List<String> serviceList;
  private final ConcurrentLinkedQueue<String> queue;
  private final ConcurrentHashMap<String, Integer> retryServiceMap;
  private final int maxRetryTimes;

  @Inject
  public SupportBundleSystemLogTask(
      @Assisted SupportBundleStatus supportBundleStatus,
      @Assisted String basePath,
      @Assisted String systemLogPath,
      @Assisted ProgramClient programClient,
      CConfiguration cConf) {
    this.supportBundleStatus = supportBundleStatus;
    this.basePath = basePath;
    this.systemLogPath = systemLogPath;
    this.programClient = programClient;
    this.retryServiceMap = new ConcurrentHashMap<>();
    this.maxRetryTimes = cConf.getInt(Constants.SupportBundle.MAX_RETRY_TIMES);
    this.serviceList =
        Arrays.asList(
            Constants.Service.APP_FABRIC_HTTP,
            Constants.Service.DATASET_EXECUTOR,
            Constants.Service.EXPLORE_HTTP_USER_SERVICE,
            Constants.Service.LOGSAVER,
            Constants.Service.MESSAGING_SERVICE,
            Constants.Service.METADATA_SERVICE,
            Constants.Service.METRICS,
            Constants.Service.METRICS_PROCESSOR,
            Constants.Service.RUNTIME,
            Constants.Service.TRANSACTION,
            "pipeline");
    this.queue = new ConcurrentLinkedQueue<>();
    for (String serviceId : serviceList) {
      queue.offer(serviceId);
    }
  }

  /** Adds system logs into file */
  public SupportBundleStatusTask initializeCollection() throws Exception {
    String componentId = "services";
    SupportBundleStatusTask task = null;
    String serviceId = null;
    while (!queue.isEmpty()) {
      serviceId = queue.poll();
      task = initializeTask("systemlog-" + serviceId, "SystemLogSupportBundleTask");
      updateTask(task, basePath, TaskStatus.IN_PROGRESS);
      long currentTimeMillis = System.currentTimeMillis();
      long fromMillis = currentTimeMillis - TimeUnit.DAYS.toMillis(1);
      FileWriter file = new FileWriter(new File(systemLogPath, serviceId + "-system-log.txt"));
      String systemLog =
          programClient.getProgramSystemLog(
              componentId, serviceId, fromMillis / 1000, currentTimeMillis / 1000);
      file.write(systemLog);
      file.flush();
      retryServiceMap.remove(serviceId);
    }
    return task;
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
