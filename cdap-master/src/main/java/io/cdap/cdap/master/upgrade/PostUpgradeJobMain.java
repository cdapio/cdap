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
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Restarts all schedules and programs stopped between startTimeMillis and now.
 * The first parameter to this job should be the hostname, the second should be the port the
 * router service is running on, the third should be startTimeMillis, and the fourth should be "true" if we want to
 * restart system apps.
 * Eg, if the router is running at URI http://your-hostname:11015, all schedules and programs stopped between
 * 1565913178ms and now, and system apps are to be restarted, then pass in
 * your-hostname as the first parameter, 11015 as the second, 1565913178 as the third, and true as the fourth.
 */
public class PostUpgradeJobMain {

  private static final int DEFAULT_READ_TIMEOUT_MILLIS = 90 * 1000;

  public static void main(String[] args) {
    if (args.length < 3 || args.length > 4) {
      throw new RuntimeException(
        String.format("Invalid number of arguments to %s. Needed 3, found %d",
                      PostUpgradeJobMain.class.getSimpleName(), args.length));
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
    final boolean restartSystemApps = args.length == 4 && "true".equals(args[3]);

    try {
      Retries.callWithRetries(
        () -> {
          restartPipelinesAndSchedules(clientConfig, Long.parseLong(args[2]), restartSystemApps);
          return null;
        }, retryStrategy, e -> e instanceof IOException || e instanceof NotFoundException);
    } catch (Exception e) {
      throw new RuntimeException("Failed to restart pipelines and schedules.", e);
    }
  }

  private static void restartPipelinesAndSchedules(
    ClientConfig clientConfig, long startTimeMillis, boolean restartSystemApps) throws Exception {
    long endTimeMillis = System.currentTimeMillis();
    ApplicationClient applicationClient = new ApplicationClient(clientConfig);
    ScheduleClient scheduleClient = new ScheduleClient(clientConfig);
    ProgramClient programClient = new ProgramClient(clientConfig);
    NamespaceClient namespaceClient = new NamespaceClient(clientConfig);

    List<NamespaceId> namespaceIdList =
      namespaceClient.list().stream().map(NamespaceMeta::getNamespaceId).collect(Collectors.toList());
    if (restartSystemApps) {
      namespaceIdList.add(NamespaceId.SYSTEM);
    }

    for (NamespaceId namespaceId : namespaceIdList) {
      for (ApplicationRecord record : applicationClient.list(namespaceId)) {
        ApplicationId applicationId =
          new ApplicationId(namespaceId.getNamespace(), record.getName(), record.getAppVersion());
        programClient.restart(applicationId,
                              TimeUnit.MILLISECONDS.toSeconds(startTimeMillis),
                              TimeUnit.MILLISECONDS.toSeconds(endTimeMillis));
      }

      // Re-enable schedules in a namespace AFTER programs have been restarted.
      scheduleClient.reEnableSuspendedSchedules(namespaceId, startTimeMillis, endTimeMillis);
    }
  }
}
