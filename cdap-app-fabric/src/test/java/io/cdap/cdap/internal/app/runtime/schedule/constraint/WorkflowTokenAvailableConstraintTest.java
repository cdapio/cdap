/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.schedule.constraint;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.queue.Job;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.ProgramStatusTrigger;
import io.cdap.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProtoConstraint;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.WorkflowId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link WorkflowTokenAvailableConstraint}.
 */
public class WorkflowTokenAvailableConstraintTest {

  private static final Gson GSON = new Gson();
  private static final long MAX_WAIT_MS = 60_000L;
  private static final String MAPPING_KEY = "triggering.properties.mapping";
  private static final String REQUIRED_KEY = "resolved.plugin.properties.map";

  private static final ApplicationId UPSTREAM_APP = NamespaceId.DEFAULT.app("upstreamPipeline");
  private static final WorkflowId UPSTREAM_WORKFLOW = UPSTREAM_APP.workflow("DataPipelineWorkflow");
  private static final ProgramRunId UPSTREAM_RUN = UPSTREAM_WORKFLOW.run("run-1");
  private static final ApplicationId DOWNSTREAM_APP =
      NamespaceId.DEFAULT.app("downstreamPipeline");
  private static final ProgramId DOWNSTREAM_WORKFLOW =
      DOWNSTREAM_APP.workflow("DataPipelineWorkflow");

  private static final String PLUGIN_MAPPING_JSON =
      "{\"arguments\":[],\"pluginProperties\":[{\"stageName\":\"stage1\","
          + "\"source\":\"prop1\",\"target\":\"targetProp\"}]}";
  private static final String ARGUMENT_ONLY_MAPPING_JSON =
      "{\"arguments\":[{\"source\":\"arg1\",\"target\":\"targetArg\"}],"
          + "\"pluginProperties\":[]}";

  private Store store;
  private WorkflowTokenAvailableConstraint constraint;

  @Before
  public void setUp() {
    store = Mockito.mock(Store.class);
    constraint = new WorkflowTokenAvailableConstraint(MAX_WAIT_MS, MAPPING_KEY, REQUIRED_KEY);
  }

  @Test
  public void testScheduleWithoutMappingIsNeverDelayed() {
    ProgramSchedule schedule = scheduleWithProperties(Collections.<String, String>emptyMap());
    Job job = jobAt(1000L, ImmutableList.of(statusNotification(UPSTREAM_RUN)));

    ConstraintResult result = constraint.check(schedule, new ConstraintContext(job, 1000L, store));
    Assert.assertEquals(ConstraintResult.SatisfiedState.SATISFIED, result.getSatisfiedState());
    Mockito.verifyNoInteractions(store);
  }

  @Test
  public void testMalformedMappingIsNeverDelayed() {
    ProgramSchedule schedule =
        scheduleWithProperties(ImmutableMap.of(MAPPING_KEY, "{not-valid-json"));
    Job job = jobAt(1000L, ImmutableList.of(statusNotification(UPSTREAM_RUN)));

    ConstraintResult result = constraint.check(schedule, new ConstraintContext(job, 1000L, store));
    Assert.assertEquals(ConstraintResult.SatisfiedState.SATISFIED, result.getSatisfiedState());
    Mockito.verifyNoInteractions(store);
  }

  @Test
  public void testArgumentOnlyMappingIsNeverDelayed() {
    ProgramSchedule schedule =
        scheduleWithProperties(ImmutableMap.of(MAPPING_KEY, ARGUMENT_ONLY_MAPPING_JSON));
    Job job = jobAt(1000L, ImmutableList.of(statusNotification(UPSTREAM_RUN)));

    ConstraintResult result = constraint.check(schedule, new ConstraintContext(job, 1000L, store));
    Assert.assertEquals(ConstraintResult.SatisfiedState.SATISFIED, result.getSatisfiedState());
    Mockito.verifyNoInteractions(store);
  }

  @Test
  public void testSatisfiedWhenRequiredKeyIsPresent() {
    Mockito.when(store.getWorkflowToken(UPSTREAM_WORKFLOW, UPSTREAM_RUN.getRun()))
        .thenReturn(tokenWithRequiredKey());
    ProgramSchedule schedule =
        scheduleWithProperties(ImmutableMap.of(MAPPING_KEY, PLUGIN_MAPPING_JSON));
    Job job = jobAt(1000L, ImmutableList.of(statusNotification(UPSTREAM_RUN)));

    ConstraintResult result = constraint.check(schedule, new ConstraintContext(job, 1500L, store));
    Assert.assertEquals(ConstraintResult.SatisfiedState.SATISFIED, result.getSatisfiedState());
  }

  @Test
  public void testDeferredWhenTokenIsNull() {
    Mockito.when(store.getWorkflowToken(UPSTREAM_WORKFLOW, UPSTREAM_RUN.getRun()))
        .thenReturn(null);
    ProgramSchedule schedule =
        scheduleWithProperties(ImmutableMap.of(MAPPING_KEY, PLUGIN_MAPPING_JSON));
    Job job = jobAt(1000L, ImmutableList.of(statusNotification(UPSTREAM_RUN)));

    ConstraintResult result = constraint.check(schedule, new ConstraintContext(job, 5000L, store));
    Assert.assertEquals(ConstraintResult.SatisfiedState.NOT_SATISFIED, result.getSatisfiedState());
  }

  @Test
  public void testDeferredWhenTokenIsEmpty() {
    // AppMetadataStore.getWorkflowToken returns an empty BasicWorkflowToken(0) when no row exists.
    Mockito.when(store.getWorkflowToken(UPSTREAM_WORKFLOW, UPSTREAM_RUN.getRun()))
        .thenReturn(new BasicWorkflowToken(0));
    ProgramSchedule schedule =
        scheduleWithProperties(ImmutableMap.of(MAPPING_KEY, PLUGIN_MAPPING_JSON));
    Job job = jobAt(1000L, ImmutableList.of(statusNotification(UPSTREAM_RUN)));

    ConstraintResult result = constraint.check(schedule, new ConstraintContext(job, 5000L, store));
    Assert.assertEquals(ConstraintResult.SatisfiedState.NOT_SATISFIED, result.getSatisfiedState());
  }

  @Test
  public void testDeferredWhenIntermediateTokenLacksRequiredKey() {
    // Intermediate workflow token saves during a run contain node keys, not the final required key.
    BasicWorkflowToken intermediateToken = new BasicWorkflowToken(1);
    intermediateToken.setCurrentNode("stage1");
    intermediateToken.put("some.other.key", "val");
    Mockito.when(store.getWorkflowToken(UPSTREAM_WORKFLOW, UPSTREAM_RUN.getRun()))
        .thenReturn(intermediateToken);

    ProgramSchedule schedule =
        scheduleWithProperties(ImmutableMap.of(MAPPING_KEY, PLUGIN_MAPPING_JSON));
    Job job = jobAt(1000L, ImmutableList.of(statusNotification(UPSTREAM_RUN)));

    ConstraintResult result = constraint.check(schedule, new ConstraintContext(job, 5000L, store));
    Assert.assertEquals(ConstraintResult.SatisfiedState.NOT_SATISFIED, result.getSatisfiedState());
  }

  @Test
  public void testProceedsAfterDeadline() {
    Mockito.when(store.getWorkflowToken(UPSTREAM_WORKFLOW, UPSTREAM_RUN.getRun()))
        .thenReturn(new BasicWorkflowToken(0));
    ProgramSchedule schedule =
        scheduleWithProperties(ImmutableMap.of(MAPPING_KEY, PLUGIN_MAPPING_JSON));
    long creationTime = 1000L;
    Job job = jobAt(creationTime, ImmutableList.of(statusNotification(UPSTREAM_RUN)));

    ConstraintResult beforeDeadline =
        constraint.check(
            schedule, new ConstraintContext(job, creationTime + MAX_WAIT_MS - 1, store));
    Assert.assertEquals(
        ConstraintResult.SatisfiedState.NOT_SATISFIED, beforeDeadline.getSatisfiedState());

    ConstraintResult atDeadline =
        constraint.check(schedule, new ConstraintContext(job, creationTime + MAX_WAIT_MS, store));
    Assert.assertEquals(
        ConstraintResult.SatisfiedState.SATISFIED, atDeadline.getSatisfiedState());
  }

  @Test
  public void testNonWorkflowProgramIsNeverDelayed() {
    ProgramRunId sparkRun =
        new ProgramRunId(
            NamespaceId.DEFAULT.getNamespace(), "upstreamPipeline", ProgramType.SPARK, "s1", "r1");
    ProgramSchedule schedule =
        scheduleWithProperties(ImmutableMap.of(MAPPING_KEY, PLUGIN_MAPPING_JSON));
    Job job = jobAt(1000L, ImmutableList.of(statusNotification(sparkRun)));

    ConstraintResult result = constraint.check(schedule, new ConstraintContext(job, 1000L, store));
    Assert.assertEquals(ConstraintResult.SatisfiedState.SATISFIED, result.getSatisfiedState());
    Mockito.verifyNoInteractions(store);
  }

  @Test
  public void testNotificationWithoutRunIdIsIgnored() {
    Notification notificationWithoutRunId =
        new Notification(Notification.Type.PROGRAM_STATUS, Collections.<String, String>emptyMap());
    ProgramSchedule schedule =
        scheduleWithProperties(ImmutableMap.of(MAPPING_KEY, PLUGIN_MAPPING_JSON));
    Job job = jobAt(1000L, ImmutableList.of(notificationWithoutRunId));

    ConstraintResult result = constraint.check(schedule, new ConstraintContext(job, 1000L, store));
    Assert.assertEquals(ConstraintResult.SatisfiedState.SATISFIED, result.getSatisfiedState());
    Mockito.verifyNoInteractions(store);
  }

  @Test
  public void testNoNotificationsIsSatisfied() {
    ProgramSchedule schedule =
        scheduleWithProperties(ImmutableMap.of(MAPPING_KEY, PLUGIN_MAPPING_JSON));
    Job job = jobAt(1000L, Collections.<Notification>emptyList());

    ConstraintResult result = constraint.check(schedule, new ConstraintContext(job, 1000L, store));
    Assert.assertEquals(ConstraintResult.SatisfiedState.SATISFIED, result.getSatisfiedState());
    Mockito.verifyNoInteractions(store);
  }

  @Test
  public void testNeverReturnsNeverSatisfied() {
    Mockito.when(store.getWorkflowToken(UPSTREAM_WORKFLOW, UPSTREAM_RUN.getRun()))
        .thenReturn(new BasicWorkflowToken(0));
    ProgramSchedule schedule =
        scheduleWithProperties(ImmutableMap.of(MAPPING_KEY, PLUGIN_MAPPING_JSON));
    long creationTime = 1000L;
    Job job = jobAt(creationTime, ImmutableList.of(statusNotification(UPSTREAM_RUN)));

    for (long offset : new long[] {0L, 1L, MAX_WAIT_MS / 2, MAX_WAIT_MS, MAX_WAIT_MS * 10}) {
      ConstraintResult result =
          constraint.check(schedule, new ConstraintContext(job, creationTime + offset, store));
      Assert.assertNotEquals(
          ConstraintResult.SatisfiedState.NEVER_SATISFIED, result.getSatisfiedState());
    }
  }

  private static WorkflowToken tokenWithRequiredKey() {
    BasicWorkflowToken token = new BasicWorkflowToken(1);
    token.setCurrentNode("DataPipelineWorkflow");
    token.put(REQUIRED_KEY, "{\"stage1\":{\"prop1\":\"val1\"}}");
    return token;
  }

  private static ProgramSchedule scheduleWithProperties(Map<String, String> properties) {
    return new ProgramSchedule(
        "sched1",
        "desc",
        DOWNSTREAM_WORKFLOW,
        properties,
        new ProgramStatusTrigger(UPSTREAM_WORKFLOW, ProgramStatus.COMPLETED),
        Collections.<ProtoConstraint>emptyList());
  }

  private static Notification statusNotification(ProgramRunId runId) {
    return new Notification(
        Notification.Type.PROGRAM_STATUS,
        ImmutableMap.of(
            ProgramOptionConstants.PROGRAM_RUN_ID,
            GSON.toJson(runId),
            ProgramOptionConstants.PROGRAM_STATUS,
            ProgramStatus.COMPLETED.name()));
  }

  private static Job jobAt(long creationTime, List<Notification> notifications) {
    Job job = Mockito.mock(Job.class);
    Mockito.when(job.getCreationTime()).thenReturn(creationTime);
    Mockito.when(job.getNotifications()).thenReturn(notifications);
    return job;
  }
}
