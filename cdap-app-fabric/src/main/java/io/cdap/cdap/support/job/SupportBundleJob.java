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

package io.cdap.cdap.support.job;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.SupportBundle;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.support.SupportBundleState;
import io.cdap.cdap.support.status.CollectionState;
import io.cdap.cdap.support.status.SupportBundleStatus;
import io.cdap.cdap.support.status.SupportBundleTaskStatus;
import io.cdap.cdap.support.task.SupportBundleTask;
import io.cdap.cdap.support.task.factory.SupportBundleTaskFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Support bundle job to parallel process the support bundle tasks, store file to local storage and
 * setup timeout for executor
 */
public class SupportBundleJob {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleJob.class);
  private static final Gson gson = new GsonBuilder().create();
  private final ExecutorService executor;
  private final SupportBundleStatus supportBundleStatus;
  private final List<Future> futureList;
  private final List<SupportBundleTaskFactory> supportBundleTaskFactoryList;
  private final List<SupportBundleTask> supportBundleTaskList;
  private final int maxRetryTimes;
  private final Map<Future, Long> trackTimeMap;
  private final Map<Future, SupportBundleTaskStatus> futureSupportBundleTaskMap;
  private final int maxThreadTimeout;

  @Inject
  public SupportBundleJob(ExecutorService executor, CConfiguration cConf,
                          SupportBundleStatus supportBundleStatus,
                          List<SupportBundleTaskFactory> supportBundleTaskFactoryList) {
    this.supportBundleStatus = supportBundleStatus;
    this.futureList = new ArrayList<>();
    this.supportBundleTaskFactoryList = supportBundleTaskFactoryList;
    this.supportBundleTaskList = new ArrayList<>();
    this.executor = executor;
    this.maxRetryTimes = cConf.getInt(Constants.SupportBundle.MAX_RETRY_TIMES);
    this.trackTimeMap = new ConcurrentHashMap<>();
    this.futureSupportBundleTaskMap = new ConcurrentHashMap<>();
    this.maxThreadTimeout = cConf.getInt(SupportBundle.MAX_THREAD_TIMEOUT);
  }

  /**
   * parallel processing tasks and generate support bundle
   */
  public void generateBundle(SupportBundleState supportBundleState) {
    String basePath = supportBundleState.getBasePath();
    File systemLogPath = new File(basePath, "system-log");
    DirUtils.mkdirs(systemLogPath);
    supportBundleState.setSystemLogPath(systemLogPath.getPath());
    supportBundleTaskList.addAll(
        supportBundleTaskFactoryList.stream()
            .map(factory -> factory.create(supportBundleState))
            .collect(Collectors.toList()));
    for (SupportBundleTask supportBundleTask : supportBundleTaskList) {
      String className = supportBundleTask.getClass().getName();
      String taskName = supportBundleState.getUuid().concat(": ").concat(className);
      executeTask(supportBundleTask, basePath, className, taskName);
    }
    completeProcessing(futureList, basePath);
  }

  public void executeTask(
      SupportBundleTask supportBundleTask, String basePath, String className, String taskName) {
    executeTask(supportBundleTask, basePath, className, taskName, 0);
  }

  /**
   * Execute each task to generate support bundle files
   */
  private void executeTask(
      SupportBundleTask supportBundleTask, String basePath, String className, String taskName,
      int retryCounts) {
    for (Future future : trackTimeMap.keySet()) {
      Long currentTime = System.currentTimeMillis();
      if (currentTime - trackTimeMap.get(future) > TimeUnit.MINUTES.toMillis(maxThreadTimeout)) {
        SupportBundleTaskStatus supportBundleTaskStatus =
            futureSupportBundleTaskMap.getOrDefault(future, null);
        if (supportBundleTaskStatus != null) {
          supportBundleTaskStatus.setFinishTimestamp(System.currentTimeMillis());
          updateTask(supportBundleTaskStatus, basePath, CollectionState.FAILED);
        }
        future.cancel(true);
        trackTimeMap.remove(future);
        futureSupportBundleTaskMap.remove(future);
      }
    }
    SupportBundleTaskStatus taskStatus = initializeTask(taskName, className);
    Long startTs = System.currentTimeMillis();
    Future<SupportBundleTaskStatus> futureService =
        executor.submit(
            () -> {
              try {
                updateTask(taskStatus, basePath, CollectionState.IN_PROGRESS);
                supportBundleTask.initializeCollection();
                updateTask(taskStatus, basePath, CollectionState.FINISHED);
              } catch (Exception e) {
                LOG.warn(
                    String.format("Retried three times for this supportBundleTask %s ", taskName),
                    e);
                executeTaskAgainAfterFailed(supportBundleTask, className, taskName,
                                            taskStatus, basePath, retryCounts + 1);
              }
              return taskStatus;
            });
    trackTimeMap.put(futureService, startTs);
    futureSupportBundleTaskMap.put(futureService, taskStatus);
    futureList.add(futureService);
  }

  /**
   * Execute all processing
   */
  public void completeProcessing(List<Future> futureList, String basePath) {
    for (Future future : futureList) {
      SupportBundleTaskStatus supportBundleTaskStatus = null;
      try {
        Long futureStartTime = trackTimeMap.get(future);
        Long currentTime = System.currentTimeMillis();
        Long timeLeftBeforeTimeout =
            TimeUnit.MINUTES.toMillis(maxThreadTimeout) - (currentTime - futureStartTime);
        supportBundleTaskStatus = (SupportBundleTaskStatus) future.get(timeLeftBeforeTimeout,
                                                                       TimeUnit.MILLISECONDS);
        supportBundleTaskStatus.setFinishTimestamp(System.currentTimeMillis());
        updateTask(supportBundleTaskStatus, basePath, CollectionState.FINISHED);
      } catch (Exception e) {
        LOG.warn(String.format("The task for has failed or timeout more than five minutes "), e);
        future.cancel(true);
        if (supportBundleTaskStatus != null) {
          supportBundleTaskStatus.setFinishTimestamp(System.currentTimeMillis());
          updateTask(supportBundleTaskStatus, basePath, CollectionState.FAILED);
        }
      }
      trackTimeMap.remove(future);
      futureSupportBundleTaskMap.remove(future);
    }
    supportBundleStatus.setStatus(CollectionState.FINISHED);
    supportBundleStatus.setFinishTimestamp(System.currentTimeMillis());
    addToStatus(basePath);
    executor.shutdown();
  }

  /**
   * Update status task
   */
  private void updateTask(
      SupportBundleTaskStatus taskStatus, String basePath, CollectionState status) {
    try {
      taskStatus.setStatus(status);
      addToStatus(basePath);
    } catch (Exception e) {
      LOG.warn("failed to update the status file ", e);
    }
  }

  /**
   * Update status file
   */
  private void addToStatus(String basePath) {
    try (FileWriter statusFile = new FileWriter(new File(basePath, "status.json"))) {
      statusFile.write(gson.toJson(supportBundleStatus));
      statusFile.flush();
    } catch (Exception e) {
      LOG.error("Can not update status file ", e);
    }
  }

  /**
   * Start a new status task
   */
  private SupportBundleTaskStatus initializeTask(String name, String type) {
    SupportBundleTaskStatus supportBundleTaskStatus = new SupportBundleTaskStatus();
    supportBundleTaskStatus.setName(name);
    supportBundleTaskStatus.setType(type);
    Long startTs = System.currentTimeMillis();
    supportBundleTaskStatus.setStartTimestamp(startTs);
    supportBundleStatus.getTasks().add(supportBundleTaskStatus);
    return supportBundleTaskStatus;
  }

  /**
   * Queue the task again after exception
   */
  private void executeTaskAgainAfterFailed(SupportBundleTask supportBundleTask, String className,
                                           String taskName,
                                           SupportBundleTaskStatus taskStatus,
                                           String basePath, int retryCounts) {
    if (retryCounts >= maxRetryTimes) {
      updateTask(taskStatus, basePath, CollectionState.FAILED);
    } else {
      executeTask(supportBundleTask, basePath, className, taskName, retryCounts);
      taskStatus.setRetries(retryCounts);
      updateTask(taskStatus, basePath, CollectionState.QUEUED);
    }
  }
}
