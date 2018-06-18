/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.provision;

import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.api.retry.RetryableException;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.StoreRequest;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunClusterStatus;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.TopicId;
import co.cask.cdap.runtime.spi.provisioner.Cluster;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * Sends notifications about program run provisioner operations.
 */
public class ProvisionerNotifier {
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  private final TopicId topic;
  private final RetryStrategy retryStrategy;
  private final MessagingService messagingService;

  @Inject
  ProvisionerNotifier(CConfiguration cConf, MessagingService messagingService) {
    this.topic = NamespaceId.SYSTEM.topic(cConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC));
    this.retryStrategy = RetryStrategies.fromConfiguration(cConf, "system.program.state.");
    this.messagingService = messagingService;
  }

  public void provisioning(ProgramRunId programRunId, ProgramOptions programOptions,
                           ProgramDescriptor programDescriptor, String userId) {
    publish(ImmutableMap.<String, String>builder()
              .put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId))
              .put(ProgramOptionConstants.PROGRAM_DESCRIPTOR, GSON.toJson(programDescriptor))
              .put(ProgramOptionConstants.USER_ID, userId)
              .put(ProgramOptionConstants.CLUSTER_STATUS, ProgramRunClusterStatus.PROVISIONING.name())
              .put(ProgramOptionConstants.DEBUG_ENABLED, String.valueOf(programOptions.isDebug()))
              .put(ProgramOptionConstants.USER_OVERRIDES, GSON.toJson(programOptions.getUserArguments().asMap()))
              .put(ProgramOptionConstants.SYSTEM_OVERRIDES, GSON.toJson(programOptions.getArguments().asMap()))
              .put(ProgramOptionConstants.ARTIFACT_ID, GSON.toJson(programDescriptor.getArtifactId().toApiArtifactId()))
              .build());
  }

  public void provisioned(ProgramRunId programRunId, ProgramOptions programOptions, ProgramDescriptor programDescriptor,
                          String userId, Cluster cluster, @Nullable SecureKeyInfo clusterKeyInfo) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder()
      .put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId))
      .put(ProgramOptionConstants.PROGRAM_DESCRIPTOR, GSON.toJson(programDescriptor))
      .put(ProgramOptionConstants.USER_ID, userId)
      .put(ProgramOptionConstants.CLUSTER_STATUS, ProgramRunClusterStatus.PROVISIONED.name())
      .put(ProgramOptionConstants.CLUSTER, GSON.toJson(cluster))
      .put(ProgramOptionConstants.DEBUG_ENABLED, String.valueOf(programOptions.isDebug()))
      .put(ProgramOptionConstants.USER_OVERRIDES, GSON.toJson(programOptions.getUserArguments().asMap()))
      .put(ProgramOptionConstants.SYSTEM_OVERRIDES, GSON.toJson(programOptions.getArguments().asMap()));

    if (clusterKeyInfo != null) {
      builder.put(ProgramOptionConstants.CLUSTER_KEY_INFO, GSON.toJson(clusterKeyInfo));
    }

    publish(builder.build());
  }

  public void deprovisioning(ProgramRunId programRunId) {
    publish(ImmutableMap.<String, String>builder()
              .put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId))
              .put(ProgramOptionConstants.CLUSTER_STATUS, ProgramRunClusterStatus.DEPROVISIONING.name())
              .build());
  }

  public void deprovisioned(ProgramRunId programRunId) {
    publish(ImmutableMap.<String, String>builder()
              .put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId))
              .put(ProgramOptionConstants.CLUSTER_STATUS, ProgramRunClusterStatus.DEPROVISIONED.name()).build());
  }


  private void publish(Map<String, String> properties) {
    final StoreRequest storeRequest = StoreRequestBuilder.of(topic)
      .addPayloads(GSON.toJson(new Notification(Notification.Type.PROGRAM_STATUS, properties)))
      .build();
    Retries.supplyWithRetries(
      () -> {
        try {
          messagingService.publish(storeRequest);
        } catch (TopicNotFoundException e) {
          throw new RetryableException(e);
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
        return null;
      },
      retryStrategy);
  }
}
