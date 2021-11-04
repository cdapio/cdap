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
import com.google.inject.assistedinject.Assisted;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.logging.gateway.handlers.RemoteProgramLogsFetcher;
import io.cdap.cdap.logging.gateway.handlers.RemoteProgramRunRecordsFetcher;
import io.cdap.cdap.metadata.RemoteApplicationDetailFetcher;
import io.cdap.cdap.metrics.process.RemoteMetricsSystemClient;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.support.job.SupportBundleJob;
import io.cdap.cdap.support.status.SupportBundleTaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Collects pipeline details.
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
  private final String programType;
  private final String programName;
  private final SupportBundleJob supportBundleJob;
  private final int maxRunsPerProgram;

  @Inject
  public SupportBundlePipelineInfoTask(@Assisted String uuid, @Assisted List<String> namespaceList,
                                       @Assisted String appId, @Assisted String basePath,
                                       RemoteApplicationDetailFetcher remoteApplicationDetailFetcher,
                                       RemoteProgramRunRecordsFetcher remoteProgramRunRecordsFetcher,
                                       RemoteProgramLogsFetcher remoteProgramLogsFetcher, @Assisted String programType,
                                       @Assisted String programName,
                                       RemoteMetricsSystemClient remoteMetricsSystemClient,
                                       @Assisted SupportBundleJob supportBundleJob, @Assisted int maxRunsPerProgram) {
    this.uuid = uuid;
    this.basePath = basePath;
    this.namespaceList = namespaceList;
    this.appId = appId;
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
    for (String namespaceId : namespaceList) {
      List<ApplicationRecord> apps = new ArrayList<>();
      if (appId == null) {
        apps.addAll(remoteApplicationDetailFetcher.list(namespaceId).stream()
                      .map(applicationDetail -> new ApplicationRecord(applicationDetail)).collect(Collectors.toList()));
      } else {
        apps.add(new ApplicationRecord(remoteApplicationDetailFetcher.get(new ApplicationId(namespaceId, appId))));
      }

      for (ApplicationRecord app : apps) {
        String appId = app.getName();
        ApplicationId applicationId = new ApplicationId(namespaceId, appId);
        File appFolderPath = new File(basePath, app.getName());
        DirUtils.mkdirs(appFolderPath);
        try (FileWriter file = new FileWriter(new File(appFolderPath, appId + ".json"))) {
          ApplicationDetail applicationDetail = remoteApplicationDetailFetcher.get(applicationId);
          gson.toJson(applicationDetail, file);

          List<RunRecord> runRecordList = sortRunRecords(namespaceId, programType, programName);
          SupportBundleRuntimeInfoTask supportBundleRuntimeInfoTask =
            new SupportBundleRuntimeInfoTask(appFolderPath.getPath(), namespaceId, appId, programType, programName,
                                             remoteMetricsSystemClient, runRecordList, applicationDetail);
          SupportBundlePipelineRunLogTask supportBundlePipelineRunLogTask =
            new SupportBundlePipelineRunLogTask(appFolderPath.getPath(), namespaceId, appId, programType, programName,
                                                remoteProgramLogsFetcher, runRecordList);
          String runtimeInfoClassName = supportBundleRuntimeInfoTask.getClass().getName();
          String runtimeInfoTaskName =
            uuid.concat(": ").concat(runtimeInfoClassName).concat(": ").concat(app.getName());
          SupportBundleTaskStatus runtimeInfoTaskStatus =
            supportBundleJob.initializeTask(runtimeInfoTaskName, runtimeInfoClassName);
          supportBundleJob.executeTask(runtimeInfoTaskStatus, supportBundleRuntimeInfoTask, basePath,
                                       runtimeInfoClassName, runtimeInfoTaskName, 0);
          String runtimeLogClassName = supportBundlePipelineRunLogTask.getClass().getName();
          String runtimeLogTaskName = uuid.concat(": ").concat(runtimeLogClassName).concat(": ").concat(app.getName());
          SupportBundleTaskStatus runtimeLogTaskStatus =
            supportBundleJob.initializeTask(runtimeLogTaskName, runtimeLogClassName);
          supportBundleJob.executeTask(runtimeLogTaskStatus, supportBundlePipelineRunLogTask, basePath,
                                       runtimeLogClassName, runtimeLogTaskName, 0);
        } catch (IOException e) {
          LOG.error("Failed to write pipeline info file with namespace {} ", namespaceId, e);
          throw new IOException("Failed to write pipeline info file ", e);
        }
      }
    }
  }

  private List<RunRecord> sortRunRecords(String namespaceId, String programType,
                                         String programName) throws NotFoundException, IOException {
    List<RunRecord> runRecordList = new ArrayList<>();
    ProgramId programId = new ProgramId(namespaceId, appId, ProgramType.valueOfCategoryName(programType), programName);
    List<RunRecord> allRunRecordList = remoteProgramRunRecordsFetcher.getProgramRuns(programId, 0, Long.MAX_VALUE, 100);

    List<RunRecord> sortedRunRecordList = allRunRecordList.stream().filter(run -> run.getStatus().isEndState())
      .sorted(Comparator.comparing(RunRecord::getStartTs).reversed()).collect(Collectors.toList());
    // Gets the last N runs info
    for (RunRecord runRecord : sortedRunRecordList) {
      if (runRecordList.size() >= maxRunsPerProgram) {
        break;
      }
      runRecordList.add(runRecord);
    }
    return runRecordList;
  }
}
