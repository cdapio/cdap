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
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.support.status.SupportBundleStatus;
import io.cdap.cdap.support.status.SupportBundleStatusTask;
import io.cdap.cdap.support.task.factory.SupportBundleTaskFactory;
import io.cdap.cdap.support.task.SupportBundleTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SupportBundleJob {
  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleJob.class);
  private static final int maxThreadNumber = 5;
  private static final ExecutorService executor = Executors.newFixedThreadPool(maxThreadNumber);
  private final SupportBundleStatus supportBundleStatus;
  private final Gson gson = new GsonBuilder().create();
  private final ArrayBlockingQueue<Object> queue;
  private final List<Future> futureList;
  private final List<SupportBundleTaskFactory> supportBundleTaskFactoryList;
  private final List<SupportBundleTask> supportBundleTaskList;

  @Inject
  public SupportBundleJob(
    SupportBundleStatus supportBundleStatus,
    List<SupportBundleTaskFactory> supportBundleTaskFactoryList) {
    int queueSize = 22;
    this.supportBundleStatus = supportBundleStatus;
    this.queue = new ArrayBlockingQueue<>(queueSize);
    this.futureList = new ArrayList<>();
    this.supportBundleTaskFactoryList = supportBundleTaskFactoryList;
    this.supportBundleTaskList = new ArrayList<>();
  }

  public void executeTasks(
      String namespaceId,
      String workflowName,
      String basePath,
      String systemLogPath,
      List<ApplicationRecord> apps,
      int numOfRunNeeded) {
    supportBundleTaskList.addAll(supportBundleTaskFactoryList.stream()
                                   .map(factory -> factory.create(supportBundleStatus, namespaceId,
                                                                  basePath, systemLogPath,
                                                                  numOfRunNeeded, workflowName, apps))
                                   .collect(Collectors.toList()));
    for (SupportBundleTask supportBundleTask : supportBundleTaskList) {
      executeTask(supportBundleTask);
    }
    completeProcessing(futureList, basePath);
  }

  public void executeTask(SupportBundleTask supportBundleTask) {
    Future<SupportBundleStatusTask> futureService =
      executor.submit(
        () -> {
          SupportBundleStatusTask task = supportBundleTask.initializeCollection();
          return task;
        });
    futureList.add(futureService);
  }

  /** Execute all processing */
  public void completeProcessing(List<Future> futureList, String basePath) {
    for (Future future : futureList) {
      SupportBundleStatusTask supportBundleStatusTask = null;
      try {
        supportBundleStatusTask = (SupportBundleStatusTask) future.get(5, TimeUnit.MINUTES);
        supportBundleStatusTask.setFinishTimestamp(System.currentTimeMillis());
        updateTask(supportBundleStatusTask, basePath, "FINISHED");
      } catch (Exception e) {
        LOG.warn(
          String.format(
            "The task for has failed or timeout more than five minutes "),
          e);
        if (supportBundleStatusTask != null) {
          supportBundleStatusTask.setFinishTimestamp(System.currentTimeMillis());
          updateTask(supportBundleStatusTask, basePath, "FAILED");
        }
      }
    }
    supportBundleStatus.setStatus("FINISHED");
    supportBundleStatus.setFinishTimestamp(System.currentTimeMillis());
    addToStatus(basePath);
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
}
