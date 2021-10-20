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
import io.cdap.cdap.logging.gateway.handlers.RemoteProgramRunRecordsFetcher;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ProgramId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Collects pipeline run info
 */
public class SupportBundleRuntimeInfoTask implements SupportBundleTask {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleRuntimeInfoTask.class);
  private static final Gson gson = new GsonBuilder().create();
  private final String namespaceId;
  private final String appId;
  private final RemoteProgramRunRecordsFetcher remoteProgramRunRecordsFetcher;
  private final String workflow;
  private final int numOfRunNeeded;
  private final int maxRunsPerNamespace;
  private final String appPath;
  private Map<RunRecord, JsonObject> runMetricsMap;

  @Inject
  public SupportBundleRuntimeInfoTask(String appPath,
                                      String namespaceId,
                                      String appId,
                                      RemoteProgramRunRecordsFetcher remoteProgramRunRecordsFetcher,
                                      String workflow,
                                      int numOfRunNeeded,
                                      int maxRunsPerNamespace) {
    this.namespaceId = namespaceId;
    this.appId = appId;
    this.remoteProgramRunRecordsFetcher = remoteProgramRunRecordsFetcher;
    this.workflow = workflow;
    this.numOfRunNeeded = numOfRunNeeded;
    this.appPath = appPath;
    this.maxRunsPerNamespace = maxRunsPerNamespace;
  }

  public void setRunMetricsMap(Map<RunRecord, JsonObject> runMetricsMap) {
    this.runMetricsMap = runMetricsMap;
  }

  public void initializeCollection() {
    for (RunRecord runRecord : runMetricsMap.keySet()) {
      String runId = runRecord.getPid();
      try {
        FileWriter file = new FileWriter(new File(appPath, runId + ".json"));
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("status", runRecord.getStatus().toString());
        jsonObject.addProperty("start", runRecord.getStartTs());
        jsonObject.addProperty("end", runRecord.getStopTs());
        jsonObject.addProperty("profileName", runRecord.getProfileId().getProfile());
        jsonObject.addProperty("runtimeArgs", runRecord.getProperties().get("runtimeArgs"));
        jsonObject.add("metrics", runMetricsMap.get(runRecord));
        file.write(gson.toJson(jsonObject));
      } catch (Exception e) {
        LOG.warn("Can not write to run info file ", e);
      }
    }
  }

  public List<RunRecord> getRunRecords() {
    return sortRunRecords();
  }

  private List<RunRecord> sortRunRecords() {
    List<RunRecord> runRecordList = new ArrayList<>();
    try {
      ProgramId programId =
          new ProgramId(namespaceId, appId, ProgramType.valueOfCategoryName("workflows"), workflow);
      List<RunRecord> allRunRecordList =
          remoteProgramRunRecordsFetcher.getProgramRuns(programId, 0, Long.MAX_VALUE, 100);

      List<RunRecord> sortedRunRecordList =
          allRunRecordList.stream()
              .filter(run -> run.getStatus().isEndState())
              .sorted(
                  Collections.reverseOrder(
                      (a, b) -> {
                        if (a.getStartTs() <= b.getStartTs()) {
                          return 1;
                        }
                        return -1;
                      }))
              .collect(Collectors.toList());
      // Gets the last N runs info
      for (RunRecord runRecord : sortedRunRecordList) {
        if (runRecordList.size() >= numOfRunNeeded || runRecordList.size() >= maxRunsPerNamespace) {
          break;
        }
        runRecordList.add(runRecord);
      }
    } catch (Exception e) {
      LOG.warn("Failed to get program runs ", e);
    }
    return runRecordList;
  }
}
