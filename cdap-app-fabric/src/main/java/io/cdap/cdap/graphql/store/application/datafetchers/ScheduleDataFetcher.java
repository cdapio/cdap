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

package io.cdap.cdap.graphql.store.application.datafetchers;

import graphql.schema.AsyncDataFetcher;
import graphql.schema.DataFetcher;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.client.ScheduleClient;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.graphql.cdap.schema.GraphQLFields;
import io.cdap.cdap.proto.ProgramRecord;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.WorkflowId;

import java.util.Map;

public class ScheduleDataFetcher {

  private static final ScheduleDataFetcher INSTANCE = new ScheduleDataFetcher();

  private final ScheduleClient scheduleClient;
  private final ProgramClient programClient;

  private ScheduleDataFetcher() {
    this.scheduleClient = new ScheduleClient(ClientConfig.getDefault());
    this.programClient = new ProgramClient(ClientConfig.getDefault());
  }

  public static ScheduleDataFetcher getInstance() {
    return INSTANCE;
  }

  /**
   * Fetcher to get the next runtimes
   *
   * @return the data fetcher
   */
  public DataFetcher getNextRuntimesDataFetcher() {
    return AsyncDataFetcher.async(
      dataFetchingEnvironment -> {
        ProgramRecord programRecord = dataFetchingEnvironment.getSource();
        String programRecordName = programRecord.getName();

        Map<String, Object> localContext = dataFetchingEnvironment.getLocalContext();
        String namespace = (String) localContext.get(GraphQLFields.NAMESPACE);
        String applicationName = (String) localContext.get(GraphQLFields.NAME);

        WorkflowId workflowId = new WorkflowId(namespace, applicationName, programRecordName);

        return scheduleClient.nextRuntimes(workflowId);
      }
    );
  }

  public DataFetcher getsome() {
    return AsyncDataFetcher.async(
      dataFetchingEnvironment -> {
        ProgramRecord programRecord = dataFetchingEnvironment.getSource();
        ProgramType programType = programRecord.getType();
        String programName = programRecord.getName();

        Map<String, Object> localContext = dataFetchingEnvironment.getLocalContext();
        String namespace = (String) localContext.get(GraphQLFields.NAMESPACE);
        String applicationName = (String) localContext.get(GraphQLFields.NAME);

        ProgramId programId = new ProgramId(namespace, applicationName, programType, programName);

        return programClient.getAllProgramRuns(programId, 0, Integer.MAX_VALUE, Integer.MAX_VALUE);
      }
    );
  }

}
