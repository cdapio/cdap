/*
 *
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.graphql.store.programrecord.datafetchers;

import com.google.inject.Inject;
import graphql.execution.DataFetcherResult;
import graphql.schema.AsyncDataFetcher;
import graphql.schema.DataFetcher;
import io.cdap.cdap.client.ScheduleClient;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.program.RemoteProgramClient;
import io.cdap.cdap.graphql.cdap.schema.GraphQLFields;
import io.cdap.cdap.graphql.store.programrecord.schema.ProgramRecordFields;
import io.cdap.cdap.proto.ProgramRecord;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.ScheduledRuntime;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.WorkflowId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fetchers to get schedules
 */
public class ScheduleDataFetcher {

  private final ScheduleClient scheduleClient;
  private final RemoteProgramClient remoteProgramClient;

  @Inject
  public ScheduleDataFetcher(RemoteProgramClient remoteProgramClient) {
    this.scheduleClient = new ScheduleClient(ClientConfig.getDefault());
    this.remoteProgramClient = remoteProgramClient;
  }

  /**
   * Fetcher to get the start times
   *
   * @return the data fetcher
   */
  public DataFetcher getSchedulesDataFetcher() {
    return AsyncDataFetcher.async(
      dataFetchingEnvironment -> {
        ProgramRecord programRecord = dataFetchingEnvironment.getSource();
        String programRecordName = programRecord.getName();

        Map<String, Object> localContext = dataFetchingEnvironment.getLocalContext();
        String namespace = (String) localContext.get(GraphQLFields.NAMESPACE);
        String applicationName = (String) localContext.get(GraphQLFields.NAME);

        WorkflowId workflowId = new WorkflowId(namespace, applicationName, programRecordName);
        List<ScheduleDetail> scheduleDetails = scheduleClient.listSchedules(workflowId);

        Map<String, Object> newLocalContext = new ConcurrentHashMap<>(localContext);
        newLocalContext.put(ProgramRecordFields.WORKFLOW_ID, workflowId);

        return DataFetcherResult.newResult()
          .data(scheduleDetails)
          .localContext(newLocalContext)
          .build();
      }
    );
  }

  /**
   * Fetcher to get the runs
   *
   * @return the data fetcher
   */
  public DataFetcher getRunsDataFetcher() {
    return AsyncDataFetcher.async(
      dataFetchingEnvironment -> {
        ProgramRecord programRecord = dataFetchingEnvironment.getSource();
        ProgramType programType = programRecord.getType();
        String programName = programRecord.getName();

        Map<String, Object> localContext = dataFetchingEnvironment.getLocalContext();
        String namespace = (String) localContext.get(GraphQLFields.NAMESPACE);
        String applicationName = (String) localContext.get(GraphQLFields.NAME);

        ProgramId programId = new ProgramId(namespace, applicationName, programType, programName);

        return remoteProgramClient.getAllProgramRuns(programId, 0, Integer.MAX_VALUE, Integer.MAX_VALUE);
      }
    );
  }

  /**
   * Fetcher to get the times of the next runs
   *
   * @return the data fetcher
   */
  public DataFetcher getTimeDataFetcher() {
    return AsyncDataFetcher.async(
      dataFetchingEnvironment -> {
        Map<String, Object> localContext = dataFetchingEnvironment.getLocalContext();
        WorkflowId workflowId = (WorkflowId) localContext.get(ProgramRecordFields.WORKFLOW_ID);

        List<ScheduledRuntime> scheduledRuntimes = scheduleClient.nextRuntimes(workflowId);
        List<Long> times = new ArrayList<>();

        for (ScheduledRuntime scheduledRuntime : scheduledRuntimes) {
          times.add(scheduledRuntime.getTime());
        }

        return times;
      }
    );
  }
}

