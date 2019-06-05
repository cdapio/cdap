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

import com.google.inject.Inject;
import graphql.execution.DataFetcherResult;
import graphql.schema.AsyncDataFetcher;
import graphql.schema.DataFetcher;
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.client.ScheduleClient;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.graphql.cdap.schema.GraphQLFields;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ProgramRecord;
import io.cdap.cdap.proto.ScheduledRuntime;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.WorkflowId;

import java.util.List;
import java.util.Map;

/**
 * Fetchers to get applications
 */
public class ApplicationRecordDataFetcher {

  private static final ApplicationRecordDataFetcher INSTANCE = new ApplicationRecordDataFetcher();

  private final ApplicationClient applicationClient;
  private final ScheduleClient scheduleClient;

  @Inject
  private ApplicationRecordDataFetcher() {
    // TODO the client config should, somehow, get passed
    this.applicationClient = new ApplicationClient(ClientConfig.getDefault());
    this.scheduleClient = new ScheduleClient(ClientConfig.getDefault());
  }

  public static ApplicationRecordDataFetcher getInstance() {
    return INSTANCE;
  }

  /**
   * Fetcher to get applications
   *
   * @return the data fetcher
   */
  public DataFetcher getApplicationsDataFetcher() {
    return AsyncDataFetcher.async(
      dataFetchingEnvironment -> {
        String namespace = dataFetchingEnvironment.getArgument(GraphQLFields.NAMESPACE);

        return applicationClient.list(new NamespaceId(namespace));
      }
    );
  }

  /**
   * Fetcher to get an application
   *
   * @return the data fetcher
   */
  public DataFetcher getApplicationDataFetcher() {
    return AsyncDataFetcher.async(
      dataFetchingEnvironment -> {
        Map<String, Object> localContext = dataFetchingEnvironment.getLocalContext();
        localContext.putAll(dataFetchingEnvironment.getArguments());

        String namespace = dataFetchingEnvironment.getArgument(GraphQLFields.NAMESPACE);
        String applicationName = dataFetchingEnvironment.getArgument(GraphQLFields.NAME);

        ApplicationId appId = new ApplicationId(namespace, applicationName);
        ApplicationDetail applicationDetail = applicationClient.get(appId);

        return DataFetcherResult.newResult().data(applicationDetail).build();
      }
    );
  }

  // TODO add in its own class?
  public DataFetcher getSomeDataFetcher() {
    return AsyncDataFetcher.async(
      dataFetchingEnvironment -> {
        Map<String, Object> localContext = dataFetchingEnvironment.getLocalContext();

        String namespace = (String) localContext.get(GraphQLFields.NAMESPACE);
        String application = (String) localContext.get(GraphQLFields.NAME);
        ProgramRecord programRecord = dataFetchingEnvironment.getSource();
        String w = programRecord.getName();

        WorkflowId workflowId = new WorkflowId(namespace, application, w);
        List<ScheduledRuntime> x = scheduleClient.nextRuntimes(workflowId);
        // TODO probably return list?
        return x.get(0).getTime();
      }
    );
  }
}
