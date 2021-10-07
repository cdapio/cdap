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
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.client.MetricsClient;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.support.status.SupportBundleStatus;
import io.cdap.cdap.support.task.SupportBundlePipelineInfoTask;
import io.cdap.cdap.support.task.SupportBundlePipelineRunLogTask;
import io.cdap.cdap.support.task.SupportBundleRuntimeInfoTask;
import io.cdap.cdap.support.task.SupportBundleSystemLogTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class SupportBundleJob {
  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleJob.class);
  private final SupportBundleStatus supportBundleStatus;
  private final Gson gson;
  private final MetricsClient metricsClient;
  private final ProgramClient programClient;
  private final ApplicationClient applicationClient;

  @Inject
  public SupportBundleJob(
      Gson gson,
      SupportBundleStatus supportBundleStatus,
      MetricsClient metricsClient,
      ProgramClient programClient,
      ApplicationClient applicationClient) {
    this.gson = gson;
    this.supportBundleStatus = supportBundleStatus;
    this.metricsClient = metricsClient;
    this.programClient = programClient;
    this.applicationClient = applicationClient;
  }

  public void executeTasks(
      String namespaceId,
      String appId,
      List<String> serviceList,
      String workflowName,
      String basePath,
      String systemLogPath,
      List<ApplicationRecord> apps,
      int numOfRunNeeded) {
    SupportBundleSystemLogTask supportBundleSystemLogTask =
        new SupportBundleSystemLogTask(
            supportBundleStatus, serviceList, gson, basePath, systemLogPath, programClient);
    supportBundleSystemLogTask.initializeCollection();
    for (ApplicationRecord app : apps) {
      File appFolderPath = new File(basePath, app.getName());
      DirUtils.mkdirs(appFolderPath);
      SupportBundlePipelineInfoTask supportBundlePipelineInfoTask =
          new SupportBundlePipelineInfoTask(
              supportBundleStatus,
              namespaceId,
              appId,
              gson,
              basePath,
              appFolderPath.getPath(),
              applicationClient,
              programClient,
              numOfRunNeeded,
              workflowName,
              metricsClient);
      supportBundlePipelineInfoTask.initializeCollection();
      ConcurrentHashMap<RunRecord, JsonObject> runMetricsMap = supportBundlePipelineInfoTask.collectPipelineInfo();
      SupportBundleRuntimeInfoTask supportBundleRuntimeInfoTask =
          new SupportBundleRuntimeInfoTask(
              supportBundleStatus, gson, basePath, appFolderPath.getPath(), runMetricsMap);
      supportBundleRuntimeInfoTask.initializeCollection();
      SupportBundlePipelineRunLogTask supportBundlePipelineRunLogTask =
          new SupportBundlePipelineRunLogTask(
              supportBundleStatus,
              gson,
              basePath,
              appFolderPath.getPath(),
              namespaceId,
              appId,
              workflowName,
              programClient,
              runMetricsMap);
      supportBundlePipelineRunLogTask.initializeCollection();
    }
  }
}
