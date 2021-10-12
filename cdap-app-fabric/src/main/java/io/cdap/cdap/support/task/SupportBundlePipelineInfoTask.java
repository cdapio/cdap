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
import java.util.List;
import java.util.Map;

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
  private final String namespaceId;
  private final String workflowName;
  private final int numOfRunNeeded;
  private final List<ApplicationRecord> apps;
  private final SupportBundleJob supportBundleJob;

  @Inject
  public SupportBundlePipelineInfoTask(@Assisted String namespaceId, @Assisted String basePath,
                                       RemoteApplicationDetailFetcher remoteApplicationDetailFetcher,
                                       RemoteProgramRunRecordsFetcher remoteProgramRunRecordsFetcher,
                                       RemoteProgramLogsFetcher remoteProgramLogsFetcher,
                                       @Assisted int numOfRunNeeded,
                                       @Assisted String workflowName,
                                       RemoteMetricsSystemClient remoteMetricsSystemClient,
                                       @Assisted List<ApplicationRecord> apps,
                                       @Assisted SupportBundleJob supportBundleJob) {
    this.basePath = basePath;
    this.namespaceId = namespaceId;
    this.remoteApplicationDetailFetcher = remoteApplicationDetailFetcher;
    this.remoteProgramRunRecordsFetcher = remoteProgramRunRecordsFetcher;
    this.remoteProgramLogsFetcher = remoteProgramLogsFetcher;
    this.numOfRunNeeded = numOfRunNeeded;
    this.workflowName = workflowName;
    this.remoteMetricsSystemClient = remoteMetricsSystemClient;
    this.apps = apps;
    this.supportBundleJob = supportBundleJob;
  }

  public void initializeCollection() {
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
      supportBundleJob.executeTask(supportBundleRuntimeInfoTask, namespaceId, basePath);
      supportBundleJob.executeTask(supportBundlePipelineRunLogTask, namespaceId, basePath);
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
