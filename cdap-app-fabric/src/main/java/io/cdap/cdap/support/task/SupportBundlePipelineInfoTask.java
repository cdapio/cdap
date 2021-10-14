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
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.logging.gateway.handlers.RemoteProgramLogsFetcher;
import io.cdap.cdap.logging.gateway.handlers.RemoteProgramRunRecordsFetcher;
import io.cdap.cdap.metadata.RemoteApplicationDetailFetcher;
import io.cdap.cdap.metrics.process.RemoteMetricsSystemClient;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.support.job.SupportBundleJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Collects pipeline details
 */
public class SupportBundlePipelineInfoTask implements SupportBundleTask {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundlePipelineInfoTask.class);
  private static final Gson gson = new GsonBuilder().create();
  private final String basePath;
  private final RemoteApplicationDetailFetcher remoteApplicationDetailFetcher;
  private final RemoteProgramRunRecordsFetcher remoteProgramRunRecordsFetcher;
  private final RemoteProgramLogsFetcher remoteProgramLogsFetcher;
  private final RemoteMetricsSystemClient remoteMetricsSystemClient;
  private final List<String> namespaceList;
  private final String uuid;
  private final String appId;
  private final String workflowName;
  private final int numOfRunNeeded;
  private final SupportBundleJob supportBundleJob;

  @Inject
  public SupportBundlePipelineInfoTask(@Assisted String uuid,
                                       @Assisted List<String> namespaceList,
                                       @Assisted String appId,
                                       @Assisted String basePath,
                                       RemoteApplicationDetailFetcher remoteApplicationDetailFetcher,
                                       RemoteProgramRunRecordsFetcher remoteProgramRunRecordsFetcher,
                                       RemoteProgramLogsFetcher remoteProgramLogsFetcher,
                                       @Assisted int numOfRunNeeded,
                                       @Assisted String workflowName,
                                       RemoteMetricsSystemClient remoteMetricsSystemClient,
                                       @Assisted SupportBundleJob supportBundleJob) {
    this.uuid = uuid;
    this.basePath = basePath;
    this.namespaceList = namespaceList;
    this.appId = appId;
    this.remoteApplicationDetailFetcher = remoteApplicationDetailFetcher;
    this.remoteProgramRunRecordsFetcher = remoteProgramRunRecordsFetcher;
    this.remoteProgramLogsFetcher = remoteProgramLogsFetcher;
    this.numOfRunNeeded = numOfRunNeeded;
    this.workflowName = workflowName;
    this.remoteMetricsSystemClient = remoteMetricsSystemClient;
    this.supportBundleJob = supportBundleJob;
  }

  public void initializeCollection() {
    for (String namespaceId : namespaceList) {
      List<ApplicationRecord> apps = new ArrayList<>();
      try {
        if (appId == null) {
          apps.addAll(
              remoteApplicationDetailFetcher.list(namespaceId).stream()
                  .map(applicationDetail -> new ApplicationRecord(applicationDetail))
                  .collect(Collectors.toList()));
        } else {
          apps.add(
              new ApplicationRecord(
                  remoteApplicationDetailFetcher.get(new ApplicationId(namespaceId, appId))));
        }

      } catch (Exception e) {
        LOG.warn(String.format("Can not process the task with namespace %s ", namespaceId), e);
      }

      for (ApplicationRecord app : apps) {
        String appId = app.getName();
        ApplicationId applicationId = new ApplicationId(namespaceId, appId);
        File appFolderPath = new File(basePath, app.getName());
        DirUtils.mkdirs(appFolderPath);
        try {
          FileWriter file = new FileWriter(new File(appFolderPath, appId + ".json"));
          ApplicationDetail applicationDetail = remoteApplicationDetailFetcher.get(applicationId);
          file.write(gson.toJson(applicationDetail));
          file.flush();
        } catch (Exception e) {
          LOG.warn("Retried three times for this pipeline info generate ", e);
        }
        SupportBundleRuntimeInfoTask supportBundleRuntimeInfoTask =
            new SupportBundleRuntimeInfoTask(
                appFolderPath.getPath(),
                namespaceId,
                appId,
                remoteProgramRunRecordsFetcher,
                workflowName,
                numOfRunNeeded);
        List<RunRecord> runRecordList = getAllRunRecordDetails(supportBundleRuntimeInfoTask);
        SupportBundlePipelineRunLogTask supportBundlePipelineRunLogTask =
            new SupportBundlePipelineRunLogTask(
                appFolderPath.getPath(),
                namespaceId,
                appId,
                workflowName,
                remoteProgramLogsFetcher,
                runRecordList,
                remoteApplicationDetailFetcher,
                remoteMetricsSystemClient);
        Map<RunRecord, JsonObject> runMetricsMap = queryMetrics(supportBundlePipelineRunLogTask);
        supportBundleRuntimeInfoTask.setRunMetricsMap(runMetricsMap);
        String runtimeInfoClassName = supportBundleRuntimeInfoTask.getClass().getName();
        String runtimeInfoTaskName = uuid.concat(": ").concat(runtimeInfoClassName);
        supportBundleJob.executeTask(supportBundleRuntimeInfoTask, basePath, runtimeInfoClassName,
                                     runtimeInfoTaskName);
        String runtimeLogClassName = supportBundlePipelineRunLogTask.getClass().getName();
        String runtimeLogTaskName = uuid.concat(": ").concat(runtimeInfoClassName);
        supportBundleJob.executeTask(supportBundlePipelineRunLogTask, basePath, runtimeLogClassName,
                                     runtimeLogTaskName);
      }
    }
  }

  /**
   * Collects all pipeline runs
   */
  public List<RunRecord> getAllRunRecordDetails(
      SupportBundleRuntimeInfoTask supportBundleRuntimeInfoTask) {
    return supportBundleRuntimeInfoTask.getRunRecords();
  }

  /**
   * Collects pipeline run metrics
   */
  public Map<RunRecord, JsonObject> queryMetrics(
      SupportBundlePipelineRunLogTask supportBundlePipelineRunLogTask) {
    return supportBundlePipelineRunLogTask.collectMetrics();
  }
}
