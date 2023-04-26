/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.program;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.StoreRequest;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.io.IOException;
import java.util.Map;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class MessagingProgramStatePublisherTest {
  private static final Gson GSON = new Gson();
  private static final int NUM_PARTITIONS = 10;
  private static final String RUN_ID = "testRun";

  @Rule
  public MockitoRule rule = MockitoJUnit.rule();

  @Mock
  private MessagingService messagingService;

  @Captor
  private ArgumentCaptor<StoreRequest> storeRequestCaptor;

  @Test
  public void testOneTopic() throws TopicNotFoundException, IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.AppFabric.PROGRAM_STATUS_EVENT_NUM_PARTITIONS, 1);
    MessagingProgramStatePublisher publisher = new MessagingProgramStatePublisher(cConf, messagingService);

    publisher.publish(Notification.Type.PROGRAM_STATUS, ImmutableMap.of());
    Mockito.verify(messagingService).publish(storeRequestCaptor.capture());
    StoreRequest storeRequest = storeRequestCaptor.getValue();
    Assert.assertEquals("programstatusevent", storeRequest.getTopicId().getTopic());
  }

  @Test
  public void testMultipleTopics() throws TopicNotFoundException, IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.AppFabric.PROGRAM_STATUS_EVENT_NUM_PARTITIONS, NUM_PARTITIONS);
    MessagingProgramStatePublisher publisher = new MessagingProgramStatePublisher(cConf, messagingService);

    ProgramRunId runId = new ProgramRunId("namespace", "application",
                                          ProgramType.SPARK, "program", RUN_ID);
    Map<String, String> properties = ImmutableMap.of(
      ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(runId));
    publisher.publish(Notification.Type.PROGRAM_STATUS, properties);
    ApplicationId applicationId = runId.getParent().getParent();
    int topicNum = Math.abs(applicationId.hashCode()) % NUM_PARTITIONS;
    Mockito.verify(messagingService).publish(storeRequestCaptor.capture());
    StoreRequest storeRequest = storeRequestCaptor.getValue();
    Assert.assertEquals("programstatusevent" + topicNum, storeRequest.getTopicId().getTopic());
  }

  @Test
  public void testMultipleTopics_noRun() throws TopicNotFoundException, IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.AppFabric.PROGRAM_STATUS_EVENT_NUM_PARTITIONS, 10);
    MessagingProgramStatePublisher publisher = new MessagingProgramStatePublisher(cConf, messagingService);

    publisher.publish(Notification.Type.PROGRAM_STATUS, ImmutableMap.of());
    Mockito.verify(messagingService).publish(storeRequestCaptor.capture());
    StoreRequest storeRequest = storeRequestCaptor.getValue();
    Assert.assertEquals("programstatusevent0", storeRequest.getTopicId().getTopic());
  }
}
