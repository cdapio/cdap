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
import io.cdap.cdap.logging.gateway.handlers.RemoteProgramLogsFetcher;
import io.cdap.cdap.logging.gateway.handlers.RemoteProgramRunRecordsFetcher;
import io.cdap.cdap.metadata.RemoteApplicationDetailFetcher;
import io.cdap.cdap.metrics.process.RemoteMetricsSystemClient;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.support.job.SupportBundleJob;
import io.cdap.cdap.support.status.SupportBundleTaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
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
  private final RemoteProgramLogsFetcher remoteProgramLogsFetcher;
  private final RemoteMetricsSystemClient remoteMetricsSystemClient;
  private final List<NamespaceId> namespaces;
  private final String uuid;
  private final String application;
  private final ProgramType programType;
  private final String programName;
  private final SupportBundleJob supportBundleJob;
  private final int maxRunsPerProgram;

  @Inject
  public SupportBundlePipelineInfoTask(String uuid, List<NamespaceId> namespaces, String application, File basePath,
                                       RemoteApplicationDetailFetcher remoteApplicationDetailFetcher,
                                       RemoteProgramRunRecordsFetcher remoteProgramRunRecordsFetcher,
                                       RemoteProgramLogsFetcher remoteProgramLogsFetcher, ProgramType programType,
                                       String programName, RemoteMetricsSystemClient remoteMetricsSystemClient,
                                       SupportBundleJob supportBundleJob, int maxRunsPerProgram) {
    this.uuid = uuid;
    this.basePath = basePath;
    this.namespaces = namespaces;
    this.application = application;
    this.remoteApplicationDetailFetcher = remoteApplicationDetailFetcher;
    this.remoteProgramRunRecordsFetcher = remoteProgramRunRecordsFetcher;
    this.remoteProgramLogsFetcher = remoteProgramLogsFetcher;
    this.programType = programType;
    this.programName = programName;
    this.remoteMetricsSystemClient = remoteMetricsSystemClient;
    this.supportBundleJob = supportBundleJob;
    this.maxRunsPerProgram = maxRunsPerProgram;
  }

  @Override
  public void collect() throws IOException, NotFoundException {
    for (NamespaceId namespaceId : namespaces) {
      try {
        Stream<ApplicationDetail> appDetails;
        if (application == null) {
          appDetails = remoteApplicationDetailFetcher.list(namespaceId.getNamespace()).stream();
        } else {
          appDetails =
            Stream.of(remoteApplicationDetailFetcher.get(new ApplicationId(namespaceId.getNamespace(), application)));
        }

        for (ApplicationDetail appDetail : (Iterable<ApplicationDetail>) () -> appDetails.iterator()) {
          String application = appDetail.getName();
          ApplicationId applicationId = new ApplicationId(namespaceId.getNamespace(), application);

          File appFolderPath = new File(basePath, appDetail.getName());
          DirUtils.mkdirs(appFolderPath);
          try (FileWriter file = new FileWriter(new File(appFolderPath, appDetail.getName() + ".json"))) {
            GSON.toJson(appDetail, file);
          }
          ProgramId programId =
            new ProgramId(namespaceId.getNamespace(), appDetail.getName(), programType, programName);
          Iterable<RunRecord> runRecordList = getRunRecords(programId);

          SupportBundleRuntimeInfoTask supportBundleRuntimeInfoTask =
            new SupportBundleRuntimeInfoTask(appFolderPath, namespaceId, applicationId, programType, programId,
                                             remoteMetricsSystemClient, runRecordList, appDetail);
          SupportBundlePipelineRunLogTask supportBundlePipelineRunLogTask =
            new SupportBundlePipelineRunLogTask(appFolderPath, programId, remoteProgramLogsFetcher, runRecordList);

          String runtimeInfoClassName = supportBundleRuntimeInfoTask.getClass().getName();
          String runtimeInfoTaskName =
            uuid.concat(": ").concat(runtimeInfoClassName).concat(": ").concat(appDetail.getName());
          processSubTask(runtimeInfoTaskName, runtimeInfoClassName, supportBundleRuntimeInfoTask);

          String runtimeLogClassName = supportBundlePipelineRunLogTask.getClass().getName();
          String runtimeLogTaskName =
            uuid.concat(": ").concat(runtimeLogClassName).concat(": ").concat(appDetail.getName());
          processSubTask(runtimeLogTaskName, runtimeLogClassName, supportBundlePipelineRunLogTask);
        }
      } catch (NotFoundException e) {
        LOG.debug("Failed to find application {} ", application, e);
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

  private void processSubTask(String taskName, String taskType, SupportBundleTask supportBundleTask) {
    SupportBundleTaskStatus runtimeInfoTaskStatus =
      supportBundleJob.initializeTask(taskName, taskType, basePath.getPath());
    supportBundleJob.executeTask(runtimeInfoTaskStatus, supportBundleTask, basePath.getPath(), taskType, taskName);
  }
}
