/*
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
package io.cdap.cdap.master.upgrade;

import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.client.NamespaceClient;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.client.ScheduleClient;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.ProgramStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.proto.id.WorkflowId;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Suspends all schedules and stops all programs.
 * The first parameter to this job should be the hostname, and the second should be the port the
 * router service is running on. Eg, if the router is running at URI http://your-hostname:11015, then pass in
 * your-hostname as the first parameter and 11015 as the second.
 */
public class UpgradeJobMain {

  private static final int DEFAULT_READ_TIMEOUT_MILLIS = 90 * 1000;
  private static final String SCHEDULED = "SCHEDULED";

  public static void main(String[] args) {
    if (args.length != 2) {
      throw new RuntimeException(
        String.format("Invalid number of arguments to UpgradeJobMain. Needed 2, found %d", args.length));
    }
    ConnectionConfig connectionConfig = ConnectionConfig.builder()
      .setHostname(args[0])
      .setPort(Integer.parseInt(args[1]))
      .setSSLEnabled(false)
      .build();
    ClientConfig clientConfig =
      ClientConfig.builder()
        .setDefaultReadTimeout(DEFAULT_READ_TIMEOUT_MILLIS)
        .setConnectionConfig(connectionConfig)
        .build();
    RetryStrategy retryStrategy =
      RetryStrategies.timeLimit(30, TimeUnit.SECONDS,
                                RetryStrategies.exponentialDelay(10, 500, TimeUnit.MILLISECONDS));

    try {
      Retries.callWithRetries(
        () -> {
          suspendSchedulesAndStopPipelines(clientConfig);
          return null;
        }, retryStrategy, e -> e instanceof IOException || e instanceof NotFoundException);
    } catch (Exception e) {
      throw new RuntimeException("Failed to prepare instance for upgrade.", e);
    }
  }

  private static void suspendSchedulesAndStopPipelines(ClientConfig clientConfig) throws Exception {
    ApplicationClient applicationClient = new ApplicationClient(clientConfig);
    ScheduleClient scheduleClient = new ScheduleClient(clientConfig);
    ProgramClient programClient = new ProgramClient(clientConfig);
    NamespaceClient namespaceClient = new NamespaceClient(clientConfig);

    List<NamespaceId> namespaceIdList =
      namespaceClient.list().stream().map(NamespaceMeta::getNamespaceId).collect(Collectors.toList());
    namespaceIdList.add(NamespaceId.SYSTEM);

    for (NamespaceId namespaceId : namespaceIdList) {
      for (ApplicationRecord record : applicationClient.list(namespaceId)) {
        ApplicationId applicationId =
          new ApplicationId(namespaceId.getNamespace(), record.getName(), record.getAppVersion());
        List<WorkflowId> workflowIds =
          applicationClient.get(applicationId).getPrograms().stream()
            .filter(programRecord -> programRecord.getType().equals(ProgramType.WORKFLOW))
            .map(programRecord -> new WorkflowId(applicationId, programRecord.getName()))
            .collect(Collectors.toList());
        for (WorkflowId workflowId : workflowIds) {
          List<ScheduleId> scheduleIds =
            scheduleClient.listSchedules(workflowId).stream()
              .map(scheduleDetail ->
                     new ScheduleId(namespaceId.getNamespace(), record.getName(),
                                    record.getAppVersion(), scheduleDetail.getName()))
              .collect(Collectors.toList());
          for (ScheduleId scheduleId : scheduleIds) {
            if (scheduleClient.getStatus(scheduleId).equals(SCHEDULED)) {
              scheduleClient.suspend(scheduleId);
            }
          }
          // Need to stop workflows first or else the program will fail to stop below
          if (programClient.getStatus(workflowId).equals(ProgramStatus.RUNNING.toString())) {
            programClient.stop(workflowId);
          }
        }
      }

      // All schedules are stopped, now stop all programs
      programClient.stopAll(namespaceId);
    }
  }
}
