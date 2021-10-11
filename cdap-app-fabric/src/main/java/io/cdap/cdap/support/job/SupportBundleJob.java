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
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.support.conf.SupportBundleConfiguration;
import io.cdap.cdap.support.status.SupportBundleStatus;
import io.cdap.cdap.support.status.SupportBundleStatusTask;
import io.cdap.cdap.support.status.TaskStatus;
import io.cdap.cdap.support.task.SupportBundleTask;
import io.cdap.cdap.support.task.factory.SupportBundleTaskFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class SupportBundleJob {
  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleJob.class);
  private static final int maxThreadNumber = 5;
  private static final ExecutorService executor = Executors.newFixedThreadPool(maxThreadNumber);
  private final SupportBundleStatus supportBundleStatus;
  private final Gson gson = new GsonBuilder().create();
  private final List<Future> futureList;
  private final List<SupportBundleTaskFactory> supportBundleTaskFactoryList;
  private final List<SupportBundleTask> supportBundleTaskList;
  private final ApplicationClient applicationClient;

  @Inject
  public SupportBundleJob(
      SupportBundleStatus supportBundleStatus,
      List<SupportBundleTaskFactory> supportBundleTaskFactoryList,
      ApplicationClient applicationClient) {
    this.supportBundleStatus = supportBundleStatus;
    this.futureList = new ArrayList<>();
    this.supportBundleTaskFactoryList = supportBundleTaskFactoryList;
    this.supportBundleTaskList = new ArrayList<>();
    this.applicationClient = applicationClient;
  }

  public void executeTasks(SupportBundleConfiguration supportBundleConfiguration) {
    List<String> namespaceList = supportBundleConfiguration.getNamespaceList();
    String namespaceId = supportBundleConfiguration.getNamespaceId();
    String appId = supportBundleConfiguration.getAppId();
    String basePath = supportBundleConfiguration.getBasePath();
    for (String namespacesId : namespaceList) {
      try {
        NamespaceId namespace = new NamespaceId(namespacesId);
        List<ApplicationRecord> apps = new ArrayList<>();
        if (appId == null) {
          apps.addAll(applicationClient.list(namespace));
        } else {
          apps.add(
              new ApplicationRecord(applicationClient.get(new ApplicationId(namespaceId, appId))));
        }
        File systemLogPath = new File(basePath, "system-log");
        DirUtils.mkdirs(systemLogPath);
        // Generates system log for user request
        supportBundleTaskList.addAll(
            supportBundleTaskFactoryList.stream()
                .map(factory -> factory.create(supportBundleConfiguration))
                .collect(Collectors.toList()));
        for (SupportBundleTask supportBundleTask : supportBundleTaskList) {
          executeTask(supportBundleTask);
        }
        completeProcessing(futureList, basePath);
      } catch (Exception e) {
        LOG.warn("Can not get applications ", e);
      }
    }
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
        supportBundleStatusTask = (SupportBundleStatusTask) future.get();
        supportBundleStatusTask.setFinishTimestamp(System.currentTimeMillis());
        updateTask(supportBundleStatusTask, basePath, TaskStatus.FINISHED);
      } catch (Exception e) {
        LOG.warn(String.format("The task for has failed or timeout more than five minutes "), e);
        if (supportBundleStatusTask != null) {
          supportBundleStatusTask.setFinishTimestamp(System.currentTimeMillis());
          updateTask(supportBundleStatusTask, basePath, TaskStatus.FAILED);
        }
      }
    }
    supportBundleStatus.setStatus(TaskStatus.FINISHED);
    supportBundleStatus.setFinishTimestamp(System.currentTimeMillis());
    addToStatus(basePath);
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
}
