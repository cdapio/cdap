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
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.logging.gateway.handlers.RemoteLogsFetcher;
import io.cdap.cdap.logging.gateway.handlers.RemoteProgramRunRecordFetcher;
import io.cdap.cdap.logging.gateway.handlers.RemoteProgramRunRecordsFetcher;
import io.cdap.cdap.metadata.RemoteApplicationDetailFetcher;
import io.cdap.cdap.metrics.process.RemoteMetricsSystemClient;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.support.job.SupportBundleJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.StreamSupport;

/**
 * Collects pipeline details.
 */
public class SupportBundlePipelineInfoTask implements SupportBundleTask {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundlePipelineInfoTask.class);
  private static final Gson GSON = new GsonBuilder().create();
  private final File basePath;
  private final RemoteApplicationDetailFetcher remoteApplicationDetailFetcher;
  private final RemoteProgramRunRecordsFetcher remoteProgramRunRecordsFetcher;
  private final RemoteProgramRunRecordFetcher remoteProgramRunRecordFetcher;
  private final RemoteLogsFetcher remoteLogsFetcher;
  private final RemoteMetricsSystemClient remoteMetricsSystemClient;
  private final List<NamespaceId> namespaces;
  private final String runId;
  private final String uuid;
  private final String requestApplication;
  private final ProgramType programType;
  private final String programName;
  private final SupportBundleJob supportBundleJob;
  private final int maxRunsPerProgram;

  @Inject
  public SupportBundlePipelineInfoTask(String uuid, List<NamespaceId> namespaces, String requestApplication,
                                       String runId, File basePath,
                                       RemoteApplicationDetailFetcher remoteApplicationDetailFetcher,
                                       RemoteProgramRunRecordsFetcher remoteProgramRunRecordsFetcher,
                                       RemoteLogsFetcher remoteLogsFetcher, ProgramType programType, String programName,
                                       RemoteMetricsSystemClient remoteMetricsSystemClient,
                                       SupportBundleJob supportBundleJob, int maxRunsPerProgram,
                                       RemoteProgramRunRecordFetcher remoteProgramRunRecordFetcher) {
    this.uuid = uuid;
    this.basePath = basePath;
    this.namespaces = namespaces;
    this.requestApplication = requestApplication;
    this.runId = runId;
    this.remoteApplicationDetailFetcher = remoteApplicationDetailFetcher;
    this.remoteProgramRunRecordsFetcher = remoteProgramRunRecordsFetcher;
    this.remoteProgramRunRecordFetcher = remoteProgramRunRecordFetcher;
    this.remoteLogsFetcher = remoteLogsFetcher;
    this.programType = programType;
    this.programName = programName;
    this.remoteMetricsSystemClient = remoteMetricsSystemClient;
    this.supportBundleJob = supportBundleJob;
    this.maxRunsPerProgram = maxRunsPerProgram;
  }

  @Override
  public void collect() throws IOException, NotFoundException {
    for (NamespaceId namespaceId : namespaces) {
      Iterable<ApplicationDetail> appDetails;
      if (requestApplication == null) {
        appDetails = remoteApplicationDetailFetcher.list(namespaceId.getNamespace());
      } else {
        try {
          appDetails = Collections.singletonList(
            remoteApplicationDetailFetcher.get(new ApplicationId(namespaceId.getNamespace(), requestApplication)));
        } catch (NotFoundException e) {
          LOG.debug("Failed to find application {} ", requestApplication, e);
          continue;
        }
      }

      for (ApplicationDetail appDetail : appDetails) {
        String application = appDetail.getName();
        ApplicationId applicationId = new ApplicationId(namespaceId.getNamespace(), application);

        File appFolderPath = new File(basePath, appDetail.getName());
        DirUtils.mkdirs(appFolderPath);
        try (FileWriter file = new FileWriter(new File(appFolderPath, appDetail.getName() + ".json"))) {
          GSON.toJson(appDetail, file);
        }
        ProgramId programId = new ProgramId(namespaceId.getNamespace(), appDetail.getName(), programType, programName);
        Iterable<RunRecord> runRecordList;
        if (runId != null) {
          ProgramRunId programRunId =
            new ProgramRunId(namespaceId.getNamespace(), appDetail.getName(), programType, programName, runId);
          RunRecordDetail runRecordDetail = remoteProgramRunRecordFetcher.getRunRecordMeta(programRunId);
          runRecordList = Collections.singletonList(runRecordDetail);
        } else {
          runRecordList = getRunRecords(programId);
        }

        SupportBundleRuntimeInfoTask supportBundleRuntimeInfoTask =
          new SupportBundleRuntimeInfoTask(appFolderPath, namespaceId, applicationId, programType, programId,
                                           remoteMetricsSystemClient, runRecordList, appDetail);
        SupportBundlePipelineRunLogTask supportBundlePipelineRunLogTask =
          new SupportBundlePipelineRunLogTask(appFolderPath, programId, remoteLogsFetcher, runRecordList);

        String runtimeInfoClassName = supportBundleRuntimeInfoTask.getClass().getName();
        String runtimeInfoTaskName =
          uuid.concat(": ").concat(runtimeInfoClassName).concat(": ").concat(appDetail.getName());
        supportBundleJob.executeTask(supportBundleRuntimeInfoTask, basePath.getPath(), runtimeInfoTaskName,
                                     runtimeInfoTaskName);

        String runtimeLogClassName = supportBundlePipelineRunLogTask.getClass().getName();
        String runtimeLogTaskName =
          uuid.concat(": ").concat(runtimeLogClassName).concat(": ").concat(appDetail.getName());
        supportBundleJob.executeTask(supportBundlePipelineRunLogTask, basePath.getPath(), runtimeLogTaskName,
                                     runtimeLogClassName);
      }
    }
  }

  private Iterable<RunRecord> getRunRecords(ProgramId programId) throws NotFoundException, IOException {
    Iterable<RunRecord> allRunRecordList =
      remoteProgramRunRecordsFetcher.getProgramRuns(programId, 0, Long.MAX_VALUE, 100);
    return () -> StreamSupport.stream(allRunRecordList.spliterator(), false)
      .filter(run -> run.getStatus().isEndState())
      .sorted(Comparator.comparing(RunRecord::getStartTs).reversed())
      .limit(maxRunsPerProgram)
      .iterator();
  }
}
