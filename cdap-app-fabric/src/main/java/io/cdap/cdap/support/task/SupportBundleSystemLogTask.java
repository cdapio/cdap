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

public class SupportBundleSystemLogTask implements SupportBundleTask {
  private static final int maxThreadNumber = 5;
  private static final ExecutorService executor = Executors.newFixedThreadPool(maxThreadNumber);
  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleSystemLogTask.class);
  private static final int queueSize = 12;
  private final String basePath;
  private final String systemLogPath;
  private final ProgramClient programClient;
  private final ConcurrentHashMap<String, Integer> retryServiceMap;
  private final Gson gson;
  private final ArrayBlockingQueue<String> queue;
  private final SupportBundleStatus supportBundleStatus;
  private final List<String> serviceList;

  @Inject
  public SupportBundleSystemLogTask(
      SupportBundleStatus supportBundleStatus,
      List<String> serviceList,
      Gson gson,
      String basePath,
      String systemLogPath,
      ProgramClient programClient) {
    this.serviceList = serviceList;
    this.supportBundleStatus = supportBundleStatus;
    this.gson = gson;
    this.basePath = basePath;
    this.systemLogPath = systemLogPath;
    this.programClient = programClient;
    retryServiceMap = new ConcurrentHashMap<>();
    queue = new ArrayBlockingQueue<>(queueSize);
  }

  /** Adds system logs into file */
  public void initializeCollection() {
    String componentId = "services";
    List<Future> futureList = new ArrayList<>();
    for (String serviceId : serviceList) {
      try {
        queue.put(serviceId);
      } catch (InterruptedException e) {
        LOG.warn("Fail to add service into queue ", e);
      }
    }

    while (!queue.isEmpty()) {
      try {
        String serviceId = queue.take();
        Future<SupportBundleStatusTask> futureService =
            executor.submit(
                () -> {
                  SupportBundleStatusTask task = null;
                  try {
                    task = initializeTask("systemlog-" + serviceId, "SystemLogSupportBundleTask");
                    updateTask(task, basePath, "IN_PROGRESS");
                    long currentTimeMillis = System.currentTimeMillis();
                    long fromMillis = currentTimeMillis - TimeUnit.DAYS.toMillis(1);
                    FileWriter file =
                        new FileWriter(new File(systemLogPath, serviceId + "-system-log.txt"));
                    String systemLog =
                        programClient.getProgramSystemLog(
                            componentId, serviceId, fromMillis / 1000, currentTimeMillis / 1000);
                    file.write(systemLog);
                    file.flush();
                    retryServiceMap.remove(serviceId);
                  } catch (Exception e) {
                    LOG.warn(String.format("Retried three times for system log %s ", serviceId), e);
                    queueTaskAfterFailed(serviceId, task);
                  }
                  return task;
                });
        if (!retryServiceMap.containsKey(serviceId)) {
          futureList.add(futureService);
        }
      } catch (InterruptedException e) {
        LOG.warn("Fail to add service into queue ", e);
      }
    }
    completeProcessing(futureList, basePath, "systemlog");
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
