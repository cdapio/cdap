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

import com.google.inject.Inject;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.logging.gateway.handlers.RemoteLogsFetcher;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.support.lib.SupportBundleFileNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Collects pipeline run info.
 */
public class SupportBundlePipelineRunLogTask implements SupportBundleTask {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundlePipelineRunLogTask.class);
  private final File appFolderPath;
  private final RemoteLogsFetcher remoteLogsFetcher;
  private final ProgramId programName;
  private final Iterable<RunRecord> runRecordList;

  @Inject
  public SupportBundlePipelineRunLogTask(File appFolderPath, ProgramId programName,
                                         RemoteLogsFetcher remoteLogsFetcher,
                                         Iterable<RunRecord> runRecordList) {
    this.appFolderPath = appFolderPath;
    this.remoteLogsFetcher = remoteLogsFetcher;
    this.programName = programName;
    this.runRecordList = runRecordList;
  }

  @Override
  public void collect() throws IOException, NotFoundException {
    for (RunRecord runRecord : runRecordList) {
      String runId = runRecord.getPid();
      File file = new File(appFolderPath, runId + SupportBundleFileNames.LOG_SUFFIX_NAME);
      long currentTimeMillis = System.currentTimeMillis();
      long fromMillis = 0;
      remoteLogsFetcher.writeProgramRunLogs(programName, runId, fromMillis, currentTimeMillis / 1000, file);
    }
  }
}
