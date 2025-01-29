/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.schedule.trigger;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProgramStatusTriggerTest {

  private static final Gson GSON = new Gson();
  private ProgramStatusTrigger trigger;
  private ProgramRunId programRunId;
  private static final RetryStrategy retryStrategy = createNotificationRetryStrategy();
  @Mock
  private AppMetadataStore mockMetadataStore;

  @Before
  public void setUp() throws Exception {
    trigger = new ProgramStatusTriggerBuilder(ProgramType.WORKFLOW.name(), "program",
        ProgramStatus.COMPLETED, ProgramStatus.FAILED).build("default", "application", "123");
    programRunId = new ProgramRunId("default", "application",
        io.cdap.cdap.proto.ProgramType.WORKFLOW, "program", "testRun");

    mockMetadataStore = mock(AppMetadataStore.class);
  }

  @Test
  public void testIsSatisfiedTrue() throws Exception {
    Map<String, String> properties = ImmutableMap.of(ProgramOptionConstants.PROGRAM_RUN_ID,
        GSON.toJson(programRunId),
        ProgramOptionConstants.PROGRAM_STATUS, ProgramStatus.COMPLETED.name());
    Notification notification = new Notification(Notification.Type.PROGRAM_STATUS, properties);
    List<Notification> notificationList = ImmutableList.of(notification);
    BasicWorkflowToken validWorkflowToken = new BasicWorkflowToken(1);
    validWorkflowToken.setCurrentNode("node");
    validWorkflowToken.put("resolved.plugin.properties.map", "");

    Mockito.when(mockMetadataStore.getWorkflowToken(Mockito.any(), Mockito.any()))
        .thenReturn(validWorkflowToken);
    boolean result = trigger.isSatisfied(null,
        new NotificationContext(notificationList, mockMetadataStore, retryStrategy));

    Assert.assertTrue(result);
    // Workflow token should be fetched only once.
    verify(mockMetadataStore, times(1)).getWorkflowToken(Mockito.any(), Mockito.any());
  }

  @Test
  public void testIsSatisfiedTrueNonWorkflowProgram() throws Exception {
    ProgramStatusTrigger sparkTrigger = new ProgramStatusTriggerBuilder(ProgramType.SPARK.name(),
        "spark-program",
        ProgramStatus.COMPLETED, ProgramStatus.FAILED).build("default", "application", "123");
    ProgramRunId sparkProgramRunId = new ProgramRunId("default", "application",
        io.cdap.cdap.proto.ProgramType.SPARK, "spark-program", "spark-program");

    Map<String, String> properties = ImmutableMap.of(ProgramOptionConstants.PROGRAM_RUN_ID,
        GSON.toJson(sparkProgramRunId),
        ProgramOptionConstants.PROGRAM_STATUS, ProgramStatus.COMPLETED.name());
    Notification notification = new Notification(Notification.Type.PROGRAM_STATUS, properties);
    List<Notification> notificationList = ImmutableList.of(notification);

    boolean result = sparkTrigger.isSatisfied(null,
        new NotificationContext(notificationList, mockMetadataStore, retryStrategy));

    Assert.assertTrue(result);
    // Workflow token should be fetched only once.
    verify(mockMetadataStore, times(0)).getWorkflowToken(Mockito.any(), Mockito.any());
  }

  @Test
  public void testIsSatisfiedFalseNullWorkflowToken() throws Exception {
    Map<String, String> properties = ImmutableMap.of(ProgramOptionConstants.PROGRAM_RUN_ID,
        GSON.toJson(programRunId),
        ProgramOptionConstants.PROGRAM_STATUS, ProgramStatus.COMPLETED.name());
    Notification notification = new Notification(Notification.Type.PROGRAM_STATUS, properties);
    List<Notification> notificationList = ImmutableList.of(notification);

    Mockito.when(mockMetadataStore.getWorkflowToken(Mockito.any(), Mockito.any()))
        .thenReturn(null);
    boolean result = trigger.isSatisfied(null,
        new NotificationContext(notificationList, mockMetadataStore, retryStrategy));

    Assert.assertFalse(result);
    // Workflow token should be fetched 4 times, one for original call and 3 retries.
    verify(mockMetadataStore, times(4)).getWorkflowToken(Mockito.any(), Mockito.any());
  }

  @Test
  public void testIsSatisfiedFalseEmptyWorkflowToken() throws Exception {
    Map<String, String> properties = ImmutableMap.of(ProgramOptionConstants.PROGRAM_RUN_ID,
        GSON.toJson(programRunId),
        ProgramOptionConstants.PROGRAM_STATUS, ProgramStatus.COMPLETED.name());
    Notification notification = new Notification(Notification.Type.PROGRAM_STATUS, properties);
    List<Notification> notificationList = ImmutableList.of(notification);
    BasicWorkflowToken emptyWorkflowToken = new BasicWorkflowToken(0);

    Mockito.when(mockMetadataStore.getWorkflowToken(Mockito.any(), Mockito.any()))
        .thenReturn(emptyWorkflowToken);
    boolean result = trigger.isSatisfied(null,
        new NotificationContext(notificationList, mockMetadataStore, retryStrategy));

    Assert.assertFalse(result);
    // Workflow token should be fetched 4 times, one for original call and 3 retries.
    verify(mockMetadataStore, times(4)).getWorkflowToken(Mockito.any(), Mockito.any());
  }

  @Test
  public void testIsSatisfiedFalseNonMatchingStatus() throws Exception {
    Map<String, String> properties = ImmutableMap.of(ProgramOptionConstants.PROGRAM_RUN_ID,
        GSON.toJson(programRunId),
        ProgramOptionConstants.PROGRAM_STATUS, ProgramStatus.KILLED.name());
    Notification notification = new Notification(Notification.Type.PROGRAM_STATUS, properties);
    List<Notification> notificationList = ImmutableList.of(notification);

    boolean result = trigger.isSatisfied(null,
        new NotificationContext(notificationList, mockMetadataStore, retryStrategy));

    Assert.assertFalse(result);
    // Workflow token should not be fetched since pre-condition check fails.
    verify(mockMetadataStore, times(0)).getWorkflowToken(Mockito.any(), Mockito.any());
  }

  private static RetryStrategy createNotificationRetryStrategy() {
    return RetryStrategies.limit(3, RetryStrategies.fixDelay(1, TimeUnit.MILLISECONDS));
  }
}
