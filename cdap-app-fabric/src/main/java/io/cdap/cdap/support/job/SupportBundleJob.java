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
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.SupportBundle;
import io.cdap.cdap.support.SupportBundleTaskConfiguration;
import io.cdap.cdap.support.lib.SupportBundleFileNames;
import io.cdap.cdap.support.status.CollectionState;
import io.cdap.cdap.support.status.SupportBundleStatus;
import io.cdap.cdap.support.status.SupportBundleTaskStatus;
import io.cdap.cdap.support.task.SupportBundleTask;
import io.cdap.cdap.support.task.factory.SupportBundleTaskFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Support bundle job to parallel process the support bundle tasks, store file to local storage and setup timeout for
 * executor.
 */
public class SupportBundleJob {
  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleJob.class);
  private static final Gson GSON = new GsonBuilder().create();
  private final ExecutorService executor;
  private final SupportBundleStatus supportBundleStatus;
  private final Set<SupportBundleTaskFactory> supportBundleTaskFactories;
  private final List<SupportBundleTask> supportBundleTasks;
  private final int maxRetries;
  private final int maxThreadTimeout;
  private final Queue<RunningTaskState> runningTaskStateQueue;

  public SupportBundleJob(Set<SupportBundleTaskFactory> supportBundleTaskFactories, ExecutorService executor,
                          CConfiguration cConf, SupportBundleStatus supportBundleStatus) {
    this.supportBundleStatus = supportBundleStatus;
    this.supportBundleTaskFactories = supportBundleTaskFactories;
    this.supportBundleTasks = new ArrayList<>();
    this.executor = executor;
    this.maxRetries = cConf.getInt(Constants.SupportBundle.MAX_RETRY_TIMES);
    this.maxThreadTimeout = cConf.getInt(SupportBundle.MAX_THREAD_TIMEOUT);
    this.runningTaskStateQueue = new ConcurrentLinkedQueue<>();
  }

  /**
   * parallel processing tasks and generate support bundle
   */
  public void generateBundle(SupportBundleTaskConfiguration bundleTaskConfig) {
    try {
      File basePath = bundleTaskConfig.getBasePath();
      supportBundleTasks.addAll(supportBundleTaskFactories.stream()
                                  .map(factory -> factory.create(bundleTaskConfig))
                                  .collect(Collectors.toList()));
      for (SupportBundleTask supportBundleTask : supportBundleTasks) {
        String className = supportBundleTask.getClass().getName();
        String taskName = bundleTaskConfig.getUuid().concat(": ").concat(className);
        executeTask(supportBundleTask, basePath.getPath(), taskName, className);
      }
      completeProcessing(basePath.getPath());
    } catch (Exception e) {
      LOG.warn("Failed to execute the tasks ", e);
    }
  }

  /**
   * Execute each task to generate support bundle files
   */
  public void executeTask(SupportBundleTask supportBundleTask, String basePath, String taskName, String taskType) {
    SupportBundleTaskStatus taskStatus = initializeTask(taskName, taskType, basePath);
    executeTask(taskStatus, supportBundleTask, basePath, taskName, taskType, 0);
  }

  /**
   * Execute all processing
   */
  public void completeProcessing(String basePath) {
    while (!runningTaskStateQueue.isEmpty()) {
      RunningTaskState runningTaskState = runningTaskStateQueue.poll();
      Future<SupportBundleTaskStatus> future = runningTaskState.getFuture();
      try {
        long currentTime = System.currentTimeMillis();
        long futureStartTime = runningTaskState.getStartTime().get();
        long maxThreadTimeoutToMill = TimeUnit.MINUTES.toMillis(maxThreadTimeout);
        long timeLeftBeforeTimeout =
          futureStartTime == 0L ? maxThreadTimeoutToMill : maxThreadTimeoutToMill - (currentTime - futureStartTime);
        future.get(timeLeftBeforeTimeout, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        LOG.error("The task for has failed or timeout more than five minutes ", e);
        updateFailedTask(runningTaskState.getTaskStatus(), future, basePath);
      }
    }
    SupportBundleStatus finishBundleStatus = SupportBundleStatus.builder(supportBundleStatus)
      .setStatus(CollectionState.FINISHED)
      .setFinishTimestamp(System.currentTimeMillis())
      .build();
    addToStatus(finishBundleStatus, basePath);
  }

  /**
   * Start a new status task
   */
  private SupportBundleTaskStatus initializeTask(String name, String type, String basePath) {
    SupportBundleTaskStatus supportBundleTaskStatus = SupportBundleTaskStatus.builder()
      .setName(name)
      .setType(type)
      .setStartTimestamp(System.currentTimeMillis())
      .setStatus(CollectionState.QUEUED)
      .build();
    supportBundleStatus.getTasks().add(supportBundleTaskStatus);
    addToStatus(supportBundleStatus, basePath);
    return supportBundleTaskStatus;
  }

  /**
   * Execute each task to generate support bundle files with accumulate retryCount
   */
  private void executeTask(SupportBundleTaskStatus taskStatus, SupportBundleTask supportBundleTask, String basePath,
                           String taskName, String taskType, int retryCount) {
    AtomicLong startTimeStore = new AtomicLong(0L);
    AtomicReference<SupportBundleTaskStatus> latestTaskStatus = new AtomicReference<>(taskStatus);
    Future<SupportBundleTaskStatus> futureService = executor.submit(() -> {
      try {
        long startTime = System.currentTimeMillis();
        startTimeStore.set(startTime);
        latestTaskStatus.set(updateTask(latestTaskStatus.get(), basePath, CollectionState.IN_PROGRESS));
        supportBundleTask.collect();
        latestTaskStatus.set(updateTask(latestTaskStatus.get(), basePath, CollectionState.FINISHED));
      } catch (Exception e) {
        LOG.warn("Failed to execute task with supportBundleTask {} ", taskName, e);
        executeTaskAgainAfterFailed(supportBundleTask, taskName, taskType, latestTaskStatus.get(), basePath,
                                    retryCount + 1);
      }
      return latestTaskStatus.get();
    });
    RunningTaskState runningTaskState = new RunningTaskState(futureService, startTimeStore, taskStatus);
    runningTaskStateQueue.offer(runningTaskState);
  }

  /**
   * Update status task
   */
  private SupportBundleTaskStatus updateTask(SupportBundleTaskStatus taskStatus, String basePath,
                                             CollectionState status) {
    SupportBundleTaskStatus newTaskStatus;
    if (status == CollectionState.IN_PROGRESS) {
      newTaskStatus = SupportBundleTaskStatus.builder(taskStatus).setStatus(status).build();
    } else {
      newTaskStatus = SupportBundleTaskStatus.builder(taskStatus)
        .setFinishTimestamp(System.currentTimeMillis())
        .setStatus(status)
        .build();
    }

    supportBundleStatus.getTasks().remove(taskStatus);
    supportBundleStatus.getTasks().add(newTaskStatus);
    addToStatus(supportBundleStatus, basePath);
    return newTaskStatus;
  }

  /**
   * Update status file
   */
  private void addToStatus(SupportBundleStatus updatedBundleStatus, String basePath) {
    try (FileWriter statusFile = new FileWriter(new File(basePath, SupportBundleFileNames.STATUS_FILE_NAME))) {
      GSON.toJson(updatedBundleStatus, statusFile);
    } catch (IOException e) {
      LOG.error("Failed to update status file ", e);
    }
  }

  /**
   * Queue the task again after exception
   */
  private void executeTaskAgainAfterFailed(SupportBundleTask supportBundleTask, String taskName, String taskType,
                                           SupportBundleTaskStatus taskStatus, String basePath, int retryCount) {
    if (retryCount >= maxRetries) {
      LOG.error("The task has reached maximum times of retries {} ", taskName);
      updateTask(taskStatus, basePath, CollectionState.FAILED);
    } else {
      SupportBundleTaskStatus updatedTaskStatus =
        SupportBundleTaskStatus.builder(taskStatus).setRetries(retryCount).setStatus(CollectionState.QUEUED).build();
      supportBundleStatus.getTasks().remove(taskStatus);
      supportBundleStatus.getTasks().add(updatedTaskStatus);
      addToStatus(supportBundleStatus, basePath);
      executeTask(taskStatus, supportBundleTask, basePath, taskType, taskName, retryCount);
    }
  }

  /**
   * Update failed status
   */
  private void updateFailedTask(SupportBundleTaskStatus supportBundleTaskStatus, Future<SupportBundleTaskStatus> future,
                                String basePath) {
    LOG.error("The task for has failed or timeout more than five minutes ");
    future.cancel(true);
    if (supportBundleTaskStatus != null) {
      updateTask(supportBundleTaskStatus, basePath, CollectionState.FAILED);
    }
  }
}
