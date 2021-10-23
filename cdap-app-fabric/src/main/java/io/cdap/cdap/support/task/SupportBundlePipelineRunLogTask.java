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
import io.cdap.cdap.common.conf.Constants.AppFabric;
import io.cdap.cdap.logging.gateway.handlers.RemoteProgramLogsFetcher;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ProgramId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOError;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Collects pipeline run info
 */
public class SupportBundlePipelineRunLogTask implements SupportBundleTask {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundlePipelineRunLogTask.class);
  private final String appFolderPath;
  private final RemoteProgramLogsFetcher remoteProgramLogsFetcher;
  private final String namespaceId;
  private final String appId;
  private final String workflowName;
  private final List<RunRecord> runRecordList;

  @Inject
  public SupportBundlePipelineRunLogTask(String appFolderPath, String namespaceId, String appId,
                                         String workflowName,
                                         RemoteProgramLogsFetcher remoteProgramLogsFetcher,
                                         List<RunRecord> runRecordList) {
    this.appFolderPath = appFolderPath;
    this.remoteProgramLogsFetcher = remoteProgramLogsFetcher;
    this.namespaceId = namespaceId;
    this.appId = appId;
    this.workflowName = workflowName;
    this.runRecordList = runRecordList;
  }

  public void initializeCollection() throws Exception {
    for (RunRecord runRecord : runRecordList) {
      String runId = runRecord.getPid();
      try (FileWriter file = new FileWriter(new File(appFolderPath, runId + "-log.txt"))) {
        long currentTimeMillis = System.currentTimeMillis();
        long fromMillis = currentTimeMillis - TimeUnit.DAYS.toMillis(1);
        ProgramId programId =
            new ProgramId(
                namespaceId, appId, ProgramType.valueOfCategoryName("workflows"), workflowName);
        String runLog =
            remoteProgramLogsFetcher.getProgramRunLogs(
                programId, runId, fromMillis / 1000, currentTimeMillis / 1000);
        file.write(runLog);
      } catch (IOException e) {
        LOG.warn("Can not write file with run {} ", runId, e);
        throw new IOException("Can not write file ", e);
      } catch (NotFoundException e) {
        LOG.warn("Can not find run log with run {} ", runId, e);
        throw new NotFoundException("Can not find run log ", e.getMessage());
      }
    }
  }
}
