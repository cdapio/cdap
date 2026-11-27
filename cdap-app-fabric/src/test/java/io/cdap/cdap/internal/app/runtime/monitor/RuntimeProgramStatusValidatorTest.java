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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.internal.app.program.MessagingProgramStateWriter;
import io.cdap.cdap.internal.app.program.ProgramStatePublisher;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeProgramStatusValidator.Action;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeProgramStatusValidator.RequestValidator;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunClusterStatus;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.netty.handler.codec.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

/**
 * Unit tests for {@link RuntimeProgramStatusValidator}.
 */
public class RuntimeProgramStatusValidatorTest {

  private static final Gson GSON = new Gson();
  private static final String FAILURE_METRIC =
      Constants.Metrics.RuntimeToken.VALIDATION_FAILURE_PREFIX + "message_status";

  private ProgramRunId wfRun;
  private ProgramRunId otherWfRun;
  private ProgramRunId otherVersionWfRun;
  private ProgramRunId sparkRun;
  private ProgramRunId innerSparkRun;
  private ProgramRunId innerMrRun;
  private ProgramRunId innerWorkerRun;
  private ProgramRunId otherNsSparkRun;
  private ProgramRunId otherAppSparkRun;
  private ProgramRunId otherVersionSparkRun;

  private AtomicLong clockMillis;
  private Map<ProgramRunId, Optional<ProgramRunId>> topLevelRuns;
  private Set<ProgramRunId> failingLookups;
  private Map<ProgramRunId, Integer> lookupCounts;
  private Map<String, Long> counters;
  private Logger mockLogger;
  private Consumer<ProgramRunId> lookupHook;

  @Before
  public void setUp() {
    wfRun = new NamespaceId("ns1").app("app1", "1.0").workflow("wf1").run(RunIds.generate());
    otherWfRun = new NamespaceId("ns1").app("app1", "1.0").workflow("wf1").run(RunIds.generate());
    otherVersionWfRun = new NamespaceId("ns1").app("app1", "2.0").workflow("wf1")
        .run(wfRun.getRun());
    sparkRun = new NamespaceId("ns1").app("app1", "1.0").spark("sp1").run(RunIds.generate());
    innerSparkRun = new NamespaceId("ns1").app("app1", "1.0").spark("innerSp")
        .run(RunIds.generate());
    innerMrRun = new NamespaceId("ns1").app("app1", "1.0").mr("innerMr")
        .run(RunIds.generate());
    innerWorkerRun = new NamespaceId("ns1").app("app1", "1.0").worker("innerWk")
        .run(RunIds.generate());
    otherNsSparkRun = new NamespaceId("ns2").app("app1", "1.0").spark("innerSp")
        .run(RunIds.generate());
    otherAppSparkRun = new NamespaceId("ns1").app("otherApp", "1.0").spark("innerSp")
        .run(RunIds.generate());
    otherVersionSparkRun = new NamespaceId("ns1").app("app1", "2.0").spark("innerSp")
        .run(RunIds.generate());

    clockMillis = new AtomicLong(1_000_000L);
    topLevelRuns = new ConcurrentHashMap<>();
    failingLookups = Collections.newSetFromMap(new ConcurrentHashMap<>());
    lookupCounts = new ConcurrentHashMap<>();
    counters = new ConcurrentHashMap<>();
    lookupHook = null;
  }

  private RuntimeProgramStatusValidator createValidator(boolean enforcing) {
    return createValidator(enforcing,
        RuntimeProgramStatusValidator.DEFAULT_MAX_ADMITTED_INNER_RUNS,
        RuntimeProgramStatusValidator.DEFAULT_MAX_UNKNOWN_RUNS,
        RuntimeProgramStatusValidator.DEFAULT_MAX_REPORT_ONLY_NOTES);
  }

  private RuntimeProgramStatusValidator createValidator(boolean enforcing,
      int maxAdmitted, int maxUnknown, int maxNotes) {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(Constants.RuntimeMonitor.RUNTIME_TOKEN_VALIDATION_ENABLED, enforcing);

    MetricsCollectionService metricsService = new NoOpMetricsCollectionService() {
      @Override
      public MetricsContext getContext(Map<String, String> tags) {
        MetricsContext delegate = super.getContext(tags);
        return new MetricsContext() {
          @Override
          public MetricsContext childContext(String tagName, String tagValue) {
            return delegate.childContext(tagName, tagValue);
          }

          @Override
          public MetricsContext childContext(Map<String, String> childTags) {
            return delegate.childContext(childTags);
          }

          @Override
          public Map<String, String> getTags() {
            return delegate.getTags();
          }

          @Override
          public void increment(String metricName, long value) {
            counters.merge(metricName, value, Long::sum);
          }

          @Override
          public void gauge(String metricName, long value) {
            delegate.gauge(metricName, value);
          }

          @Override
          public void event(String metricName, long value) {
            delegate.event(metricName, value);
          }
        };
      }
    };

    mockLogger = Mockito.mock(Logger.class);
    RuntimeValidationReporter reporter = new RuntimeValidationReporter(
        cConf, metricsService, mockLogger, clockMillis::get);

    RuntimeRequestValidator requestValidator = new RuntimeRequestValidator() {
      @Override
      public ValidatedRuntimeRequest validate(ProgramRunId programRunId, HttpRequest request) {
        return ValidatedRuntimeRequest.legacy(new ProgramRunInfo(Long.MAX_VALUE));
      }

      @Override
      public Optional<ProgramRunId> getTopLevelRun(ProgramRunId programRunId) {
        lookupCounts.merge(programRunId, 1, Integer::sum);
        if (lookupHook != null) {
          lookupHook.accept(programRunId);
        }
        if (failingLookups.contains(programRunId)) {
          throw new ServiceUnavailableException(Constants.Service.RUNTIME, "lookup failed");
        }
        return topLevelRuns.getOrDefault(programRunId, Optional.empty());
      }
    };

    return new RuntimeProgramStatusValidator(requestValidator, reporter, clockMillis::get,
        maxAdmitted, maxUnknown, maxNotes);
  }

  private static byte[] encodeNotification(Notification.Type type, Map<String, String> props) {
    return GSON.toJson(new Notification(type, props)).getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] encodeStatusViaWriter(ProgramRunId runId, ProgramRunStatus status) {
    List<byte[]> captured = new ArrayList<>();
    ProgramStatePublisher publisher = (type, props) -> captured.add(
        GSON.toJson(new Notification(type, props)).getBytes(StandardCharsets.UTF_8));
    MessagingProgramStateWriter writer = new MessagingProgramStateWriter(publisher);
    switch (status) {
      case RUNNING:
        writer.running(runId, null);
        break;
      case COMPLETED:
        writer.completed(runId);
        break;
      case FAILED:
        writer.error(runId, new RuntimeException("boom"));
        break;
      case KILLED:
        writer.killed(runId);
        break;
      case STOPPING:
        writer.stop(runId, 30);
        break;
      case SUSPENDED:
        writer.suspend(runId);
        break;
      case RESUMING:
        writer.resume(runId);
        break;
      default:
        throw new IllegalArgumentException("Unsupported status: " + status);
    }
    Assert.assertEquals(1, captured.size());
    return captured.get(0);
  }

  private static byte[] encodeInnerStartingViaWriter(ProgramRunId innerRun,
      Map<String, String> systemArgs) {
    List<byte[]> captured = new ArrayList<>();
    ProgramStatePublisher publisher = (type, props) -> captured.add(
        GSON.toJson(new Notification(type, props)).getBytes(StandardCharsets.UTF_8));
    MessagingProgramStateWriter writer = new MessagingProgramStateWriter(publisher);
    ProgramOptions options = new SimpleProgramOptions(
        innerRun.getParent(), new BasicArguments(systemArgs), new BasicArguments(), false);
    ArtifactId artifactId = innerRun.getNamespaceId().artifact("app-art", "1.0.0");
    ProgramDescriptor descriptor = new ProgramDescriptor(innerRun.getParent(), null, artifactId);
    writer.start(innerRun, options, null, descriptor);
    Assert.assertEquals(1, captured.size());
    return captured.get(0);
  }

  private static byte[] encodeHeartbeat(ProgramRunId runId, String heartBeatTime) {
    Map<String, String> props = new HashMap<>();
    props.put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(runId));
    if (heartBeatTime != null) {
      props.put(ProgramOptionConstants.HEART_BEAT_TIME, heartBeatTime);
    }
    return encodeNotification(Notification.Type.PROGRAM_HEART_BEAT, props);
  }

  private static Map<String, String> validWorkflowOverrides(ProgramRunId workflowRun,
      String nodeId) {
    Map<String, String> args = new HashMap<>();
    args.put(ProgramOptionConstants.WORKFLOW_NAME, workflowRun.getProgram());
    args.put(ProgramOptionConstants.WORKFLOW_RUN_ID, workflowRun.getRun());
    args.put(SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName());
    if (nodeId != null) {
      args.put(ProgramOptionConstants.WORKFLOW_NODE_ID, nodeId);
    }
    return args;
  }

  private static String buildNestedErrorJson(int depth) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < depth; i++) {
      sb.append("{\"className\":\"e\",\"stackTraces\":[],\"cause\":");
    }
    sb.append("null");
    for (int i = 0; i < depth; i++) {
      sb.append('}');
    }
    return sb.toString();
  }

  @Test
  public void testCommonAndOwnStatusAndHeartbeatRules() {
    for (boolean enforcing : Arrays.asList(true, false)) {
      counters.clear();
      lookupCounts.clear();
      RuntimeProgramStatusValidator validator = createValidator(enforcing);
      final Action expectedDrop = enforcing ? Action.SKIP : Action.FORWARD;

      RequestValidator rv = validator.newRequestValidator(wfRun, "programstatusevent0");
      long expectedDrops = 0L;

      // 1. Accepted statuses of W: RUNNING, COMPLETED, FAILED, KILLED
      for (ProgramRunStatus s : Arrays.asList(ProgramRunStatus.RUNNING,
          ProgramRunStatus.COMPLETED, ProgramRunStatus.FAILED, ProgramRunStatus.KILLED)) {
        Assert.assertEquals("Expected FORWARD for own status " + s, Action.FORWARD,
            rv.apply(encodeStatusViaWriter(wfRun, s)));
      }

      // 2. Rejected statuses of W: STOPPING, SUSPENDED, RESUMING, STARTING, PENDING, REJECTED
      for (ProgramRunStatus s : Arrays.asList(ProgramRunStatus.STOPPING,
          ProgramRunStatus.SUSPENDED, ProgramRunStatus.RESUMING)) {
        Assert.assertEquals("Expected drop for own status " + s, expectedDrop,
            rv.apply(encodeStatusViaWriter(wfRun, s)));
        expectedDrops++;
      }
      for (ProgramRunStatus s : Arrays.asList(ProgramRunStatus.STARTING,
          ProgramRunStatus.PENDING, ProgramRunStatus.REJECTED)) {
        byte[] raw = encodeNotification(Notification.Type.PROGRAM_STATUS, ImmutableMap.of(
            ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(wfRun),
            ProgramOptionConstants.PROGRAM_STATUS, s.name()));
        Assert.assertEquals("Expected drop for own status " + s, expectedDrop, rv.apply(raw));
        expectedDrops++;
      }

      // 3. Wrong version for W's own run -> dropped
      Assert.assertEquals(expectedDrop,
          rv.apply(encodeStatusViaWriter(otherVersionWfRun, ProgramRunStatus.RUNNING)));
      expectedDrops++;

      // 4. clusterStatus next to programStatus (and clusterStatus with null value) -> dropped
      Map<String, String> withClusterStatus = new HashMap<>();
      withClusterStatus.put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(wfRun));
      withClusterStatus.put(ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.RUNNING.name());
      withClusterStatus.put(ProgramOptionConstants.CLUSTER_STATUS,
          ProgramRunClusterStatus.PROVISIONED.name());
      Assert.assertEquals(expectedDrop,
          rv.apply(encodeNotification(Notification.Type.PROGRAM_STATUS, withClusterStatus)));
      expectedDrops++;

      JsonObject withNullClusterStatus = new JsonObject();
      withNullClusterStatus.addProperty("notificationType", "PROGRAM_STATUS");
      JsonObject propsObj = new JsonObject();
      propsObj.addProperty(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(wfRun));
      propsObj.addProperty(ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.RUNNING.name());
      propsObj.add(ProgramOptionConstants.CLUSTER_STATUS, JsonNull.INSTANCE);
      withNullClusterStatus.add("properties", propsObj);
      Assert.assertEquals(expectedDrop,
          rv.apply(withNullClusterStatus.toString().getBytes(StandardCharsets.UTF_8)));
      expectedDrops++;

      // 5. Heartbeat for W -> accepted; malformed or with programStatus or for any other run ->
      //    dropped without any lookup
      Assert.assertEquals(Action.FORWARD, rv.apply(encodeHeartbeat(wfRun, "1700000000000")));

      // Heartbeat for known inner run AND unknown run -> dropped with ZERO lookups
      topLevelRuns.put(innerSparkRun, Optional.of(wfRun));
      Assert.assertEquals(expectedDrop, rv.apply(encodeHeartbeat(innerSparkRun, "1700000000000")));
      expectedDrops++;
      Assert.assertEquals(expectedDrop, rv.apply(encodeHeartbeat(innerMrRun, "1700000000000")));
      expectedDrops++;
      Assert.assertEquals(expectedDrop,
          rv.apply(encodeHeartbeat(otherVersionWfRun, "1700000000000")));
      expectedDrops++;

      // Heartbeat with programStatus key -> dropped
      Assert.assertEquals(expectedDrop, rv.apply(encodeNotification(
          Notification.Type.PROGRAM_HEART_BEAT, ImmutableMap.of(
              ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(wfRun),
              ProgramOptionConstants.HEART_BEAT_TIME, "1700000000000",
              ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.RUNNING.name()))));
      expectedDrops++;

      // Heartbeat with non-numeric or missing heartBeatTime -> dropped
      Assert.assertEquals(expectedDrop, rv.apply(encodeHeartbeat(wfRun, "not-a-number")));
      expectedDrops++;
      Assert.assertEquals(expectedDrop, rv.apply(encodeHeartbeat(wfRun, null)));
      expectedDrops++;

      // 6. Malformed run id (random v4 UUID, uppercase UUID), wrong notification type, null
      //    properties, null payload, invalid JSON -> dropped
      ProgramRunId v4Run = new NamespaceId("ns1").app("app1", "1.0").workflow("wf1")
          .run(UUID.randomUUID().toString());
      ProgramRunId upperRun = new NamespaceId("ns1").app("app1", "1.0").workflow("wf1")
          .run(wfRun.getRun().toUpperCase());
      Assert.assertEquals(expectedDrop,
          rv.apply(encodeStatusViaWriter(v4Run, ProgramRunStatus.RUNNING)));
      expectedDrops++;
      Assert.assertEquals(expectedDrop,
          rv.apply(encodeStatusViaWriter(upperRun, ProgramRunStatus.RUNNING)));
      expectedDrops++;
      Assert.assertEquals(expectedDrop, rv.apply(encodeNotification(
          Notification.Type.TIME, ImmutableMap.of(
              ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(wfRun)))));
      expectedDrops++;
      Assert.assertEquals(expectedDrop,
          rv.apply("{\"notificationType\":\"PROGRAM_STATUS\"}".getBytes(StandardCharsets.UTF_8)));
      expectedDrops++;
      // Missing and empty programRunId -> dropped
      Assert.assertEquals(expectedDrop, rv.apply(encodeNotification(
          Notification.Type.PROGRAM_STATUS,
          ImmutableMap.of(ProgramOptionConstants.PROGRAM_STATUS,
              ProgramRunStatus.RUNNING.name()))));
      expectedDrops++;
      Assert.assertEquals(expectedDrop, rv.apply(encodeNotification(
          Notification.Type.PROGRAM_STATUS, ImmutableMap.of(
              ProgramOptionConstants.PROGRAM_RUN_ID, "",
              ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.RUNNING.name()))));
      expectedDrops++;
      // Missing and unrecognized programStatus -> dropped
      Assert.assertEquals(expectedDrop, rv.apply(encodeNotification(
          Notification.Type.PROGRAM_STATUS,
          ImmutableMap.of(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(wfRun)))));
      expectedDrops++;
      Assert.assertEquals(expectedDrop, rv.apply(encodeNotification(
          Notification.Type.PROGRAM_STATUS, ImmutableMap.of(
              ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(wfRun),
              ProgramOptionConstants.PROGRAM_STATUS, "UNKNOWN_STATUS"))));
      expectedDrops++;
      Assert.assertEquals(expectedDrop, rv.apply("null".getBytes(StandardCharsets.UTF_8)));
      expectedDrops++;
      Assert.assertEquals(expectedDrop, rv.apply("not-json".getBytes(StandardCharsets.UTF_8)));
      expectedDrops++;
      Assert.assertEquals(expectedDrop, rv.apply(null));
      expectedDrops++;

      // Zero lookups across all of the above (including heartbeats for inner runs)
      Assert.assertTrue("Expected zero lookups, saw: " + lookupCounts, lookupCounts.isEmpty());

      rv.finish();
      Assert.assertEquals(Long.valueOf(expectedDrops), counters.get(FAILURE_METRIC));
    }
  }

  @Test
  public void testInnerStartingValidation() {
    for (boolean enforcing : Arrays.asList(true, false)) {
      counters.clear();
      lookupCounts.clear();
      topLevelRuns.clear();
      RuntimeProgramStatusValidator validator = createValidator(enforcing);
      final Action expectedDrop = enforcing ? Action.SKIP : Action.FORWARD;

      // 1. Valid Spark and MapReduce inner STARTING -> accepted and remembered in both modes
      RequestValidator rv1 = validator.newRequestValidator(wfRun, "programstatusevent");
      Assert.assertEquals(Action.FORWARD, rv1.apply(
          encodeInnerStartingViaWriter(innerSparkRun, validWorkflowOverrides(wfRun, "n1"))));
      Assert.assertEquals(Action.FORWARD, rv1.apply(
          encodeInnerStartingViaWriter(innerMrRun, validWorkflowOverrides(wfRun, "n2"))));
      rv1.finish();
      Assert.assertFalse(counters.containsKey(FAILURE_METRIC));
      Assert.assertEquals(2L, validator.getAdmittedInnerRunsSize());

      // 1b. Retried STARTING for the same workflow when already remembered and recorded for wfRun
      topLevelRuns.put(innerSparkRun, Optional.of(wfRun));
      RequestValidator rvRetry = validator.newRequestValidator(wfRun, "programstatusevent");
      Assert.assertEquals(Action.FORWARD, rvRetry.apply(
          encodeInnerStartingViaWriter(innerSparkRun, validWorkflowOverrides(wfRun, "n1"))));
      rvRetry.finish();
      Assert.assertFalse(counters.containsKey(FAILURE_METRIC));
      topLevelRuns.remove(innerSparkRun);

      // 2. Starting for a run remembered for another workflow -> dropped (no lookup needed)
      lookupCounts.clear();
      RequestValidator rvOtherWf = validator.newRequestValidator(otherWfRun, "programstatusevent");
      Assert.assertEquals(expectedDrop, rvOtherWf.apply(
          encodeInnerStartingViaWriter(innerSparkRun, validWorkflowOverrides(otherWfRun, "n1"))));
      rvOtherWf.finish();
      Assert.assertEquals(Long.valueOf(1L), counters.get(FAILURE_METRIC));
      Assert.assertTrue(lookupCounts.isEmpty());

      // 3. Invalid STARTING variations
      counters.clear();
      lookupCounts.clear();
      RequestValidator rvBad = validator.newRequestValidator(wfRun, "programstatusevent");

      // Missing or mismatched workflowName (with valid profile so only workflowName fails)
      Map<String, String> noWfName = validWorkflowOverrides(wfRun, "n1");
      noWfName.remove(ProgramOptionConstants.WORKFLOW_NAME);
      long expectedDrops = 0L;
      Assert.assertEquals(expectedDrop, rvBad.apply(
          encodeInnerStartingViaWriter(innerSparkRun, noWfName)));
      expectedDrops++;

      Map<String, String> wrongWfName = validWorkflowOverrides(wfRun, "n1");
      wrongWfName.put(ProgramOptionConstants.WORKFLOW_NAME, "otherWf");
      Assert.assertEquals(expectedDrop, rvBad.apply(
          encodeInnerStartingViaWriter(innerSparkRun, wrongWfName)));
      expectedDrops++;

      // Another workflow's run id
      Assert.assertEquals(expectedDrop, rvBad.apply(
          encodeInnerStartingViaWriter(innerSparkRun, validWorkflowOverrides(otherWfRun, "n1"))));
      expectedDrops++;

      // Missing / empty workflowNodeId
      Assert.assertEquals(expectedDrop, rvBad.apply(
          encodeInnerStartingViaWriter(innerSparkRun, validWorkflowOverrides(wfRun, null))));
      expectedDrops++;
      Assert.assertEquals(expectedDrop, rvBad.apply(
          encodeInnerStartingViaWriter(innerSparkRun, validWorkflowOverrides(wfRun, ""))));
      expectedDrops++;

      // Wrong application version, wrong app, wrong namespace, non-Spark/MR program type
      Assert.assertEquals(expectedDrop, rvBad.apply(
          encodeInnerStartingViaWriter(otherVersionSparkRun, validWorkflowOverrides(wfRun, "n1"))));
      expectedDrops++;
      Assert.assertEquals(expectedDrop, rvBad.apply(
          encodeInnerStartingViaWriter(otherAppSparkRun, validWorkflowOverrides(wfRun, "n1"))));
      expectedDrops++;
      Assert.assertEquals(expectedDrop, rvBad.apply(
          encodeInnerStartingViaWriter(otherNsSparkRun, validWorkflowOverrides(wfRun, "n1"))));
      expectedDrops++;
      Assert.assertEquals(expectedDrop, rvBad.apply(
          encodeInnerStartingViaWriter(innerWorkerRun, validWorkflowOverrides(wfRun, "n1"))));
      expectedDrops++;

      // Null value inside systemOverrides (with valid artifactId so ProgramOptions.fromNotification
      // throws NPE -> drop)
      Map<String, String> badOverridesProps = new HashMap<>();
      badOverridesProps.put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(innerSparkRun));
      badOverridesProps.put(ProgramOptionConstants.PROGRAM_STATUS,
          ProgramRunStatus.STARTING.name());
      badOverridesProps.put(ProgramOptionConstants.PROGRAM_ARTIFACT_ID,
          GSON.toJson(innerSparkRun.getNamespaceId().artifact("app-art", "1.0.0")));
      badOverridesProps.put(ProgramOptionConstants.SYSTEM_OVERRIDES,
          "{\"workflowName\":\"wf1\",\"workflowRunId\":\"" + wfRun.getRun()
              + "\",\"workflowNodeId\":\"n1\",\"system.profile.name\":\""
              + ProfileId.NATIVE.getScopedName() + "\",\"bad\":null}");
      Assert.assertEquals(expectedDrop, rvBad.apply(
          encodeNotification(Notification.Type.PROGRAM_STATUS, badOverridesProps)));
      expectedDrops++;

      // None of the precondition failures above should trigger any lookup
      Assert.assertTrue("Expected zero lookups for precondition failures", lookupCounts.isEmpty());

      // Record belongs to another workflow -> dropped
      ProgramRunId ownedByOtherWf = new NamespaceId("ns1").app("app1", "1.0").spark("spOther")
          .run(RunIds.generate());
      topLevelRuns.put(ownedByOtherWf, Optional.of(otherWfRun));
      Assert.assertEquals(expectedDrop, rvBad.apply(
          encodeInnerStartingViaWriter(ownedByOtherWf, validWorkflowOverrides(wfRun, "n1"))));
      expectedDrops++;

      // Standalone run that is not launched yet (getTopLevelRun returns the standalone run itself)
      ProgramRunId standalonePending = new NamespaceId("ns1").app("app1", "1.0").spark("spStand")
          .run(RunIds.generate());
      topLevelRuns.put(standalonePending, Optional.of(standalonePending));
      Assert.assertEquals(expectedDrop, rvBad.apply(
          encodeInnerStartingViaWriter(standalonePending, validWorkflowOverrides(wfRun, "n1"))));
      expectedDrops++;

      rvBad.finish();
      Assert.assertEquals(Long.valueOf(expectedDrops), counters.get(FAILURE_METRIC));

      // 4. Non-workflow token W sending inner STARTING -> dropped with zero lookups
      counters.clear();
      lookupCounts.clear();
      RequestValidator rvSpark = validator.newRequestValidator(sparkRun, "programstatusevent");
      Assert.assertEquals(expectedDrop, rvSpark.apply(
          encodeInnerStartingViaWriter(innerSparkRun, validWorkflowOverrides(sparkRun, "n1"))));
      rvSpark.finish();
      Assert.assertEquals(Long.valueOf(1L), counters.get(FAILURE_METRIC));
      Assert.assertTrue(lookupCounts.isEmpty());
    }
  }

  @Test
  public void testInnerOtherStatusPreconditionsAuthorityAndDedup() {
    for (boolean enforcing : Arrays.asList(true, false)) {
      counters.clear();
      lookupCounts.clear();
      topLevelRuns.clear();
      RuntimeProgramStatusValidator validator = createValidator(enforcing);
      final Action expectedDrop = enforcing ? Action.SKIP : Action.FORWARD;

      // 1. Preconditions checked BEFORE lookup, wait, or note:
      //    non-workflow W, non-Spark/MR inner program, forbidden statuses (STOPPING/SUSPENDED/RESUMING)
      RequestValidator rvSparkToken = validator.newRequestValidator(sparkRun, "programstatusevent");
      Assert.assertEquals(expectedDrop,
          rvSparkToken.apply(encodeStatusViaWriter(innerSparkRun, ProgramRunStatus.RUNNING)));
      rvSparkToken.finish();

      RequestValidator rvWf = validator.newRequestValidator(wfRun, "programstatusevent");
      Assert.assertEquals(expectedDrop,
          rvWf.apply(encodeStatusViaWriter(innerWorkerRun, ProgramRunStatus.RUNNING)));
      for (ProgramRunStatus forbidden : Arrays.asList(ProgramRunStatus.STOPPING,
          ProgramRunStatus.SUSPENDED, ProgramRunStatus.RESUMING)) {
        Assert.assertEquals(expectedDrop,
            rvWf.apply(encodeStatusViaWriter(innerSparkRun, forbidden)));
      }
      rvWf.finish();
      Assert.assertTrue("Precondition failures must not call getTopLevelRun",
          lookupCounts.isEmpty());
      Assert.assertEquals(0, validator.getReportOnlyNotesSize());
      Assert.assertEquals(0L, validator.getUnknownFirstSeenSize());
      Assert.assertEquals(Long.valueOf(5L), counters.get(FAILURE_METRIC));

      // 2. Inner run known from same request (STARTING then RUNNING) -> 1 lookup, 0 notes, 0 drops
      counters.clear();
      lookupCounts.clear();
      RequestValidator rvSame = validator.newRequestValidator(wfRun, "programstatusevent");
      Assert.assertEquals(Action.FORWARD, rvSame.apply(
          encodeInnerStartingViaWriter(innerSparkRun, validWorkflowOverrides(wfRun, "n1"))));
      Assert.assertEquals(Action.FORWARD,
          rvSame.apply(encodeStatusViaWriter(innerSparkRun, ProgramRunStatus.RUNNING)));
      Assert.assertEquals(Action.FORWARD,
          rvSame.apply(encodeStatusViaWriter(innerSparkRun, ProgramRunStatus.COMPLETED)));
      rvSame.finish();
      Assert.assertEquals(Integer.valueOf(1), lookupCounts.get(innerSparkRun));
      Assert.assertEquals(0, validator.getReportOnlyNotesSize());
      Assert.assertFalse(counters.containsKey(FAILURE_METRIC));

      // 3. Inner run known from server memory in a second request -> accepted, 0 notes
      RequestValidator rvMem = validator.newRequestValidator(wfRun, "programstatusevent");
      Assert.assertEquals(Action.FORWARD,
          rvMem.apply(encodeStatusViaWriter(innerSparkRun, ProgramRunStatus.RUNNING)));
      rvMem.finish();
      Assert.assertEquals(0, validator.getReportOnlyNotesSize());
      Assert.assertFalse(counters.containsKey(FAILURE_METRIC));

      // 4. App-Fabric record naming another workflow wins over server memory!
      topLevelRuns.put(innerSparkRun, Optional.of(otherWfRun));
      lookupCounts.clear();
      RequestValidator rvAuthority = validator.newRequestValidator(wfRun, "programstatusevent");
      Assert.assertEquals(expectedDrop,
          rvAuthority.apply(encodeStatusViaWriter(innerSparkRun, ProgramRunStatus.RUNNING)));
      Assert.assertEquals(expectedDrop,
          rvAuthority.apply(encodeStatusViaWriter(innerSparkRun, ProgramRunStatus.COMPLETED)));
      rvAuthority.finish();
      Assert.assertEquals(Integer.valueOf(1), lookupCounts.get(innerSparkRun));
      Assert.assertEquals(Long.valueOf(2L), counters.get(FAILURE_METRIC));

      // 5. Inner run known from lookup (store/fetch) with all 4 allowed statuses -> 1 lookup
      topLevelRuns.put(innerMrRun, Optional.of(wfRun));
      lookupCounts.clear();
      counters.clear();
      RequestValidator rvKnown = validator.newRequestValidator(wfRun, "programstatusevent");
      Assert.assertEquals(Action.FORWARD,
          rvKnown.apply(encodeStatusViaWriter(innerMrRun, ProgramRunStatus.RUNNING)));
      Assert.assertEquals(Action.FORWARD,
          rvKnown.apply(encodeStatusViaWriter(innerMrRun, ProgramRunStatus.COMPLETED)));
      Assert.assertEquals(Action.FORWARD,
          rvKnown.apply(encodeStatusViaWriter(innerMrRun, ProgramRunStatus.FAILED)));
      Assert.assertEquals(Action.FORWARD,
          rvKnown.apply(encodeStatusViaWriter(innerMrRun, ProgramRunStatus.KILLED)));
      rvKnown.finish();
      Assert.assertEquals(Integer.valueOf(1), lookupCounts.get(innerMrRun));
      Assert.assertFalse(counters.containsKey(FAILURE_METRIC));
    }
  }

  @Test
  public void testEnforcingUnknownRunWaitThenDropAfter60s() {
    RuntimeProgramStatusValidator validator = createValidator(true);

    // At t0: unknown inner run -> WAIT, not counted
    RequestValidator rv1 = validator.newRequestValidator(wfRun, "programstatusevent0");
    Assert.assertEquals(Action.WAIT,
        rv1.apply(encodeStatusViaWriter(innerSparkRun, ProgramRunStatus.RUNNING)));
    rv1.finish();
    Assert.assertFalse(counters.containsKey(FAILURE_METRIC));

    // At t0 + 59_999ms: still WAIT
    clockMillis.addAndGet(RuntimeProgramStatusValidator.UNKNOWN_WAIT_MILLIS - 1L);
    RequestValidator rv2 = validator.newRequestValidator(wfRun, "programstatusevent0");
    Assert.assertEquals(Action.WAIT,
        rv2.apply(encodeStatusViaWriter(innerSparkRun, ProgramRunStatus.RUNNING)));
    rv2.finish();
    Assert.assertFalse(counters.containsKey(FAILURE_METRIC));

    // At t0 + 60_000ms: dropped and counted, later valid event in same request still forwarded
    clockMillis.addAndGet(1L);
    RequestValidator rv3 = validator.newRequestValidator(wfRun, "programstatusevent0");
    Assert.assertEquals(Action.SKIP,
        rv3.apply(encodeStatusViaWriter(innerSparkRun, ProgramRunStatus.RUNNING)));
    Assert.assertEquals(Action.FORWARD,
        rv3.apply(encodeStatusViaWriter(wfRun, ProgramRunStatus.RUNNING)));
    rv3.finish();
    Assert.assertEquals(Long.valueOf(1L), counters.get(FAILURE_METRIC));

    // Once the run becomes known, retry succeeds
    ProgramRunId laterKnown = new NamespaceId("ns1").app("app1", "1.0").spark("spRetry")
        .run(RunIds.generate());
    RequestValidator rvWait = validator.newRequestValidator(wfRun, "programstatusevent0");
    Assert.assertEquals(Action.WAIT,
        rvWait.apply(encodeStatusViaWriter(laterKnown, ProgramRunStatus.RUNNING)));
    rvWait.finish();

    topLevelRuns.put(laterKnown, Optional.of(wfRun));
    RequestValidator rvRetry = validator.newRequestValidator(wfRun, "programstatusevent0");
    Assert.assertEquals(Action.FORWARD,
        rvRetry.apply(encodeStatusViaWriter(laterKnown, ProgramRunStatus.RUNNING)));
    rvRetry.finish();
    Assert.assertEquals(Long.valueOf(1L), counters.get(FAILURE_METRIC));
  }

  @Test
  public void testReportOnlyNotesRecheckCapFailureAndExpiry() {
    RuntimeProgramStatusValidator validator = createValidator(false);
    // Keep W's record present unless explicitly deleted below
    topLevelRuns.put(wfRun, Optional.of(wfRun));

    // 1. Two events for the same unknown run -> published, not counted, leave ONE note
    RequestValidator rv1 = validator.newRequestValidator(wfRun, "programstatusevent0");
    Assert.assertEquals(Action.FORWARD,
        rv1.apply(encodeStatusViaWriter(innerSparkRun, ProgramRunStatus.RUNNING)));
    Assert.assertEquals(Action.FORWARD,
        rv1.apply(encodeStatusViaWriter(innerSparkRun, ProgramRunStatus.COMPLETED)));
    rv1.finish();
    Assert.assertFalse(counters.containsKey(FAILURE_METRIC));
    Assert.assertEquals(1, validator.getReportOnlyNotesSize());

    // 2. Even > 10 minutes later, the note is STILL present and counted once by a later request
    clockMillis.addAndGet(TimeUnit.MINUTES.toMillis(15));
    RequestValidator trigger = validator.newRequestValidator(otherWfRun, "programstatusevent1");
    Assert.assertEquals(Action.FORWARD,
        trigger.apply(encodeStatusViaWriter(otherWfRun, ProgramRunStatus.RUNNING)));
    trigger.finish();
    Assert.assertEquals(Long.valueOf(1L), counters.get(FAILURE_METRIC));
    Assert.assertEquals(0, validator.getReportOnlyNotesSize());

    // 3. Note whose run becomes W's -> NOT counted, W NOT looked up
    counters.clear();
    lookupCounts.clear();
    ProgramRunId becomesW = new NamespaceId("ns1").app("app1", "1.0").spark("spBecomesW")
        .run(RunIds.generate());
    RequestValidator rv2 = validator.newRequestValidator(wfRun, "programstatusevent");
    Assert.assertEquals(Action.FORWARD,
        rv2.apply(encodeStatusViaWriter(becomesW, ProgramRunStatus.RUNNING)));
    rv2.finish();
    topLevelRuns.put(becomesW, Optional.of(wfRun));
    clockMillis.addAndGet(RuntimeProgramStatusValidator.UNKNOWN_WAIT_MILLIS);
    lookupCounts.clear();
    validator.newRequestValidator(otherWfRun, "programstatusevent").finish();
    Assert.assertFalse(counters.containsKey(FAILURE_METRIC));
    Assert.assertEquals(0, validator.getReportOnlyNotesSize());
    Assert.assertFalse("W should not be looked up when run itself has a record",
        lookupCounts.containsKey(wfRun));

    // 4. Note whose run belongs to another workflow by re-check -> counted once, W NOT looked up
    ProgramRunId belongsOther = new NamespaceId("ns1").app("app1", "1.0").spark("spOther")
        .run(RunIds.generate());
    RequestValidator rv3 = validator.newRequestValidator(wfRun, "programstatusevent");
    Assert.assertEquals(Action.FORWARD,
        rv3.apply(encodeStatusViaWriter(belongsOther, ProgramRunStatus.RUNNING)));
    rv3.finish();
    topLevelRuns.put(belongsOther, Optional.of(otherWfRun));
    clockMillis.addAndGet(RuntimeProgramStatusValidator.UNKNOWN_WAIT_MILLIS);
    lookupCounts.clear();
    validator.newRequestValidator(otherWfRun, "programstatusevent").finish();
    Assert.assertEquals(Long.valueOf(1L), counters.get(FAILURE_METRIC));
    Assert.assertEquals(0, validator.getReportOnlyNotesSize());
    Assert.assertFalse(lookupCounts.containsKey(wfRun));

    // 5. Application deleted (neither run nor W has a record) -> not counted, note removed
    counters.clear();
    topLevelRuns.remove(wfRun);
    ProgramRunId deletedRun = new NamespaceId("ns1").app("app1", "1.0").spark("spDel")
        .run(RunIds.generate());
    RequestValidator rv4 = validator.newRequestValidator(wfRun, "programstatusevent");
    Assert.assertEquals(Action.FORWARD,
        rv4.apply(encodeStatusViaWriter(deletedRun, ProgramRunStatus.RUNNING)));
    rv4.finish();
    clockMillis.addAndGet(RuntimeProgramStatusValidator.UNKNOWN_WAIT_MILLIS);
    validator.newRequestValidator(otherWfRun, "programstatusevent").finish();
    Assert.assertFalse(counters.containsKey(FAILURE_METRIC));
    Assert.assertEquals(0, validator.getReportOnlyNotesSize());

    // 6. Five notes due -> one request re-checks 4, leaving 1 for the next request
    topLevelRuns.put(wfRun, Optional.of(wfRun));
    RequestValidator rvFive = validator.newRequestValidator(wfRun, "programstatusevent");
    List<ProgramRunId> fiveRuns = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      ProgramRunId r = new NamespaceId("ns1").app("app1", "1.0").spark("spFive" + i)
          .run(RunIds.generate());
      fiveRuns.add(r);
      Assert.assertEquals(Action.FORWARD,
          rvFive.apply(encodeStatusViaWriter(r, ProgramRunStatus.RUNNING)));
    }
    rvFive.finish();
    Assert.assertEquals(5, validator.getReportOnlyNotesSize());

    clockMillis.addAndGet(RuntimeProgramStatusValidator.UNKNOWN_WAIT_MILLIS);
    // Fail lookup on first run -> keeps note, ends re-checks, request succeeds, 0 counted
    failingLookups.add(fiveRuns.get(0));
    validator.newRequestValidator(otherWfRun, "programstatusevent").finish();
    Assert.assertFalse(counters.containsKey(FAILURE_METRIC));
    Assert.assertEquals(5, validator.getReportOnlyNotesSize());

    // Also test lookup failure on W (after innerRun lookup returned empty) -> keeps note, stops
    failingLookups.clear();
    failingLookups.add(wfRun);
    validator.newRequestValidator(otherWfRun, "programstatusevent").finish();
    Assert.assertFalse(counters.containsKey(FAILURE_METRIC));
    Assert.assertEquals(5, validator.getReportOnlyNotesSize());

    // Clear failure: next request re-checks exactly 4 of 5
    failingLookups.clear();
    validator.newRequestValidator(otherWfRun, "programstatusevent").finish();
    Assert.assertEquals(Long.valueOf(4L), counters.get(FAILURE_METRIC));
    Assert.assertEquals(1, validator.getReportOnlyNotesSize());

    // Next request re-checks the 5th
    validator.newRequestValidator(otherWfRun, "programstatusevent").finish();
    Assert.assertEquals(Long.valueOf(5L), counters.get(FAILURE_METRIC));
    Assert.assertEquals(0, validator.getReportOnlyNotesSize());
  }

  @Test
  public void testMemoryBoundsAndFullNotesCountedImmediately() {
    // 1. Report-only notes full -> extra unknown run counted immediately
    RuntimeProgramStatusValidator reportOnly = createValidator(false, 2, 2, 2);
    ProgramRunId r1 = new NamespaceId("ns1").app("app1", "1.0").spark("s1").run(RunIds.generate());
    ProgramRunId r2 = new NamespaceId("ns1").app("app1", "1.0").spark("s2").run(RunIds.generate());
    ProgramRunId r3 = new NamespaceId("ns1").app("app1", "1.0").spark("s3").run(RunIds.generate());
    RequestValidator rv = reportOnly.newRequestValidator(wfRun, "programstatusevent");
    Assert.assertEquals(Action.FORWARD,
        rv.apply(encodeStatusViaWriter(r1, ProgramRunStatus.RUNNING)));
    Assert.assertEquals(Action.FORWARD,
        rv.apply(encodeStatusViaWriter(r2, ProgramRunStatus.RUNNING)));
    // Duplicate of r1 fits without counting
    Assert.assertEquals(Action.FORWARD,
        rv.apply(encodeStatusViaWriter(r1, ProgramRunStatus.COMPLETED)));
    // r3 overflows capacity (2) -> counted immediately while still forwarded
    Assert.assertEquals(Action.FORWARD,
        rv.apply(encodeStatusViaWriter(r3, ProgramRunStatus.RUNNING)));
    rv.finish();
    Assert.assertEquals(2, reportOnly.getReportOnlyNotesSize());
    Assert.assertEquals(Long.valueOf(1L), counters.get(FAILURE_METRIC));

    // 2. admittedInnerRuns and unknownFirstSeen are bounded and expire after 10 minutes
    counters.clear();
    RuntimeProgramStatusValidator enforcing = createValidator(true, 2, 2, 2);
    RequestValidator rvStart = enforcing.newRequestValidator(wfRun, "programstatusevent");
    for (int i = 0; i < 5; i++) {
      ProgramRunId inner = new NamespaceId("ns1").app("app1", "1.0").spark("admit" + i)
          .run(RunIds.generate());
      Assert.assertEquals(Action.FORWARD, rvStart.apply(
          encodeInnerStartingViaWriter(inner, validWorkflowOverrides(wfRun, "n" + i))));
    }
    rvStart.finish();
    Assert.assertEquals(2L, enforcing.getAdmittedInnerRunsSize());

    for (int i = 0; i < 5; i++) {
      ProgramRunId unk = new NamespaceId("ns1").app("app1", "1.0").spark("unk" + i)
          .run(RunIds.generate());
      RequestValidator rvUnk = enforcing.newRequestValidator(wfRun, "programstatusevent");
      Assert.assertEquals(Action.WAIT,
          rvUnk.apply(encodeStatusViaWriter(unk, ProgramRunStatus.RUNNING)));
      rvUnk.finish();
    }
    Assert.assertEquals(2L, enforcing.getUnknownFirstSeenSize());

    // Just before 10-minute expiry both caches retain their 2 entries
    clockMillis.addAndGet(RuntimeProgramStatusValidator.MEMORY_EXPIRY_MILLIS - 1L);
    Assert.assertEquals(2L, enforcing.getAdmittedInnerRunsSize());
    Assert.assertEquals(2L, enforcing.getUnknownFirstSeenSize());

    // Advance past 10-minute expiry: both caches expire completely
    clockMillis.addAndGet(2L);
    Assert.assertEquals(0L, enforcing.getAdmittedInnerRunsSize());
    Assert.assertEquals(0L, enforcing.getUnknownFirstSeenSize());
  }

  @Test
  public void testTransientLookupFailureDuringApplyAndDedup() {
    for (boolean enforcing : Arrays.asList(true, false)) {
      counters.clear();
      lookupCounts.clear();
      failingLookups.clear();
      RuntimeProgramStatusValidator validator = createValidator(enforcing);
      failingLookups.add(innerSparkRun);

      // Failing lookup on inner STARTING and inner RUNNING within one request:
      // - enforcing: returns WAIT, 0 drops, run is NOT added to admittedInnerRuns
      // - report-only: returns FORWARD for both events, 0 drops, 0 notes, at most 1 lookup per run
      RequestValidator rv = validator.newRequestValidator(wfRun, "programstatusevent0");
      Action expected = enforcing ? Action.WAIT : Action.FORWARD;
      Assert.assertEquals(expected, rv.apply(
          encodeInnerStartingViaWriter(innerSparkRun, validWorkflowOverrides(wfRun, "n1"))));
      Assert.assertEquals(expected,
          rv.apply(encodeStatusViaWriter(innerSparkRun, ProgramRunStatus.RUNNING)));
      rv.finish();

      Assert.assertEquals(Integer.valueOf(1), lookupCounts.get(innerSparkRun));
      Assert.assertFalse(counters.containsKey(FAILURE_METRIC));
      Assert.assertEquals(0L, validator.getAdmittedInnerRunsSize());
      Assert.assertEquals(0L, validator.getUnknownFirstSeenSize());
      Assert.assertEquals(0, validator.getReportOnlyNotesSize());
    }
  }

  @Test
  public void testReportOnlyBoundaryEarliestTimestampInFlightAndAdmittedRecheck() {
    // 1. Earliest timestamp preservation, < 60s non-due boundary, and attribution to (wfRun, topic)
    RuntimeProgramStatusValidator validator = createValidator(false, 10, 10, 3);
    topLevelRuns.put(wfRun, Optional.of(wfRun));

    RequestValidator rv1 = validator.newRequestValidator(wfRun, "programstatusevent0");
    Assert.assertEquals(Action.FORWARD,
        rv1.apply(encodeStatusViaWriter(innerSparkRun, ProgramRunStatus.RUNNING)));
    rv1.finish();
    Assert.assertEquals(1, validator.getReportOnlyNotesSize());

    // Duplicate event at t0 + 30s must NOT advance firstSeenMillis; still not due
    clockMillis.addAndGet(TimeUnit.SECONDS.toMillis(30));
    RequestValidator rvDup = validator.newRequestValidator(wfRun, "programstatusevent0");
    Assert.assertEquals(Action.FORWARD,
        rvDup.apply(encodeStatusViaWriter(innerSparkRun, ProgramRunStatus.COMPLETED)));
    rvDup.finish();
    Assert.assertEquals(1, validator.getReportOnlyNotesSize());
    Assert.assertFalse(counters.containsKey(FAILURE_METRIC));

    // At t0 + 59_999ms: still not due
    clockMillis.addAndGet(TimeUnit.SECONDS.toMillis(30) - 1L);
    validator.newRequestValidator(otherWfRun, "programstatusevent1").finish();
    Assert.assertEquals(1, validator.getReportOnlyNotesSize());
    Assert.assertFalse(counters.containsKey(FAILURE_METRIC));

    // At t0 + 60_000ms: due! Triggered by otherWfRun on programstatusevent1, attributed to
    // (wfRun, programstatusevent0)
    clockMillis.addAndGet(1L);
    validator.newRequestValidator(otherWfRun, "programstatusevent1").finish();
    Assert.assertEquals(0, validator.getReportOnlyNotesSize());
    Assert.assertEquals(Long.valueOf(1L), counters.get(FAILURE_METRIC));
    Mockito.verify(mockLogger, Mockito.times(1)).debug(Mockito.anyString(),
        Mockito.eq("message_status"), Mockito.eq(wfRun), Mockito.eq("programstatusevent0"),
        Mockito.eq(1L), Mockito.eq(false));

    // 2. In-flight recheck capacity, earliest-timestamp restore on failure, and admittedInnerRuns
    //    clearing during recheck without looking up wfRun
    counters.clear();
    lookupCounts.clear();
    final ProgramRunId r1 = new NamespaceId("ns1").app("app1", "1.0").spark("inflight1")
        .run(RunIds.generate());
    final ProgramRunId r2 = new NamespaceId("ns1").app("app1", "1.0").spark("inflight2")
        .run(RunIds.generate());
    final ProgramRunId rOverflow = new NamespaceId("ns1").app("app1", "1.0").spark("inflightOver")
        .run(RunIds.generate());

    RequestValidator rvSeed1 = validator.newRequestValidator(wfRun, "programstatusevent0");
    Assert.assertEquals(Action.FORWARD,
        rvSeed1.apply(encodeStatusViaWriter(r1, ProgramRunStatus.RUNNING)));
    rvSeed1.finish();
    // Seed r2 10s later so r2 is not yet due when r1 reaches 60s
    clockMillis.addAndGet(TimeUnit.SECONDS.toMillis(10));
    RequestValidator rvSeed2 = validator.newRequestValidator(wfRun, "programstatusevent0");
    Assert.assertEquals(Action.FORWARD,
        rvSeed2.apply(encodeStatusViaWriter(r2, ProgramRunStatus.RUNNING)));
    rvSeed2.finish();
    Assert.assertEquals(2, validator.getReportOnlyNotesSize());

    // Advance to t1 + 60s (50s after r2). While r1 is claimed in-flight (size=1, inFlight=1),
    // simulate a concurrent duplicate event for r1 (inserting newer timestamp -> size=2,
    // inFlight=1 == maxNotes 3) followed by rOverflow (must overflow and count immediately).
    // Then fail the outer r1 lookup so finishClaimedNote restores r1's earlier t1 timestamp.
    clockMillis.addAndGet(TimeUnit.SECONDS.toMillis(50));
    lookupHook = runId -> {
      if (runId.equals(r1)) {
        lookupHook = null;
        RequestValidator concurrent = validator.newRequestValidator(wfRun, "programstatusevent0");
        concurrent.apply(encodeStatusViaWriter(r1, ProgramRunStatus.COMPLETED));
        concurrent.apply(encodeStatusViaWriter(rOverflow, ProgramRunStatus.RUNNING));
        concurrent.finish();
        failingLookups.add(r1);
      }
    };
    validator.newRequestValidator(otherWfRun, "programstatusevent1").finish();
    Assert.assertEquals(Long.valueOf(1L), counters.get(FAILURE_METRIC));
    Assert.assertEquals(2, validator.getReportOnlyNotesSize());

    // Advance only 1ms: r1 is still due only because finishClaimedNote preserved t1 over t1+60s.
    // While r1 is being rechecked again, admit STARTING for r1 under wfRun so recheckSingleNote
    // clears r1 via admittedInnerRuns with 0 drops and skips wfRun lookup.
    failingLookups.clear();
    counters.clear();
    lookupCounts.clear();
    clockMillis.addAndGet(1L);
    lookupHook = runId -> {
      if (runId.equals(r1)) {
        lookupHook = null;
        RequestValidator rvAdmit = validator.newRequestValidator(wfRun, "programstatusevent0");
        Assert.assertEquals(Action.FORWARD, rvAdmit.apply(
            encodeInnerStartingViaWriter(r1, validWorkflowOverrides(wfRun, "n1"))));
        rvAdmit.finish();
      }
    };
    validator.newRequestValidator(otherWfRun, "programstatusevent1").finish();
    Assert.assertEquals(1, validator.getReportOnlyNotesSize());
    Assert.assertFalse(counters.containsKey(FAILURE_METRIC));
    Assert.assertFalse("wfRun must not be looked up when r1 is remembered in admittedInnerRuns",
        lookupCounts.containsKey(wfRun));
  }

  @Test
  public void testOutOfOrderStartingClearsPendingMemoriesAndFinishIsIdempotent() {
    // Out-of-order STARTING clears only the matching (innerRun, workflowRun) entry in
    // unknownFirstSeen (enforcing) and reportOnlyNotes (report-only), keeping other workflows'
    // entries isolated.
    RuntimeProgramStatusValidator enforcing = createValidator(true);
    RequestValidator rvWaitOther = enforcing.newRequestValidator(otherWfRun, "programstatusevent0");
    Assert.assertEquals(Action.WAIT,
        rvWaitOther.apply(encodeStatusViaWriter(innerSparkRun, ProgramRunStatus.RUNNING)));
    rvWaitOther.finish();
    RequestValidator rvWait = enforcing.newRequestValidator(wfRun, "programstatusevent0");
    Assert.assertEquals(Action.WAIT,
        rvWait.apply(encodeStatusViaWriter(innerSparkRun, ProgramRunStatus.RUNNING)));
    rvWait.finish();
    Assert.assertEquals(2L, enforcing.getUnknownFirstSeenSize());

    RequestValidator rvStartEnf = enforcing.newRequestValidator(wfRun, "programstatusevent0");
    Assert.assertEquals(Action.FORWARD, rvStartEnf.apply(
        encodeInnerStartingViaWriter(innerSparkRun, validWorkflowOverrides(wfRun, "n1"))));
    rvStartEnf.finish();
    Assert.assertEquals(1L, enforcing.getUnknownFirstSeenSize());

    RuntimeProgramStatusValidator reportOnly = createValidator(false);
    topLevelRuns.put(wfRun, Optional.of(wfRun));
    topLevelRuns.put(otherWfRun, Optional.of(otherWfRun));
    RequestValidator rvNoteOther = reportOnly.newRequestValidator(otherWfRun,
        "programstatusevent0");
    Assert.assertEquals(Action.FORWARD,
        rvNoteOther.apply(encodeStatusViaWriter(innerMrRun, ProgramRunStatus.RUNNING)));
    rvNoteOther.finish();
    RequestValidator rvNote = reportOnly.newRequestValidator(wfRun, "programstatusevent0");
    Assert.assertEquals(Action.FORWARD,
        rvNote.apply(encodeStatusViaWriter(innerMrRun, ProgramRunStatus.RUNNING)));
    rvNote.finish();
    Assert.assertEquals(2, reportOnly.getReportOnlyNotesSize());

    RequestValidator rvStartRo = reportOnly.newRequestValidator(wfRun, "programstatusevent0");
    Assert.assertEquals(Action.FORWARD, rvStartRo.apply(
        encodeInnerStartingViaWriter(innerMrRun, validWorkflowOverrides(wfRun, "n2"))));
    rvStartRo.finish();
    // wfRun's note is cleared while otherWfRun's note remains until re-checked
    Assert.assertEquals(1, reportOnly.getReportOnlyNotesSize());
    clockMillis.addAndGet(RuntimeProgramStatusValidator.UNKNOWN_WAIT_MILLIS);
    reportOnly.newRequestValidator(wfRun, "programstatusevent0").finish();
    Assert.assertEquals(0, reportOnly.getReportOnlyNotesSize());
    Assert.assertEquals(Long.valueOf(1L), counters.get(FAILURE_METRIC));

    // finish() idempotency: calling finish() twice reports drops once and re-checks at most 4 notes
    counters.clear();
    RequestValidator rvSeed = reportOnly.newRequestValidator(wfRun, "programstatusevent0");
    for (int i = 0; i < 5; i++) {
      ProgramRunId unk = new NamespaceId("ns1").app("app1", "1.0").spark("idem" + i)
          .run(RunIds.generate());
      rvSeed.apply(encodeStatusViaWriter(unk, ProgramRunStatus.RUNNING));
    }
    rvSeed.finish();
    clockMillis.addAndGet(RuntimeProgramStatusValidator.UNKNOWN_WAIT_MILLIS);

    RequestValidator rvIdem = reportOnly.newRequestValidator(wfRun, "programstatusevent0");
    rvIdem.apply(encodeStatusViaWriter(wfRun, ProgramRunStatus.STOPPING));
    rvIdem.finish();
    rvIdem.finish();
    // 1 immediate drop (STOPPING) + 4 re-checked notes (not 2 + 5)
    Assert.assertEquals(Long.valueOf(5L), counters.get(FAILURE_METRIC));
    Assert.assertEquals(1, reportOnly.getReportOnlyNotesSize());
  }

  @Test
  public void testMalformedStatusFieldsAndInnerStartingArtifactsAndProfilesDropped() {
    Gson throwableGson = new GsonBuilder()
        .registerTypeAdapter(BasicThrowable.class, new BasicThrowableCodec())
        .create();
    // Pre-verify on a bounded 256 KiB stack thread that depth-15000 BasicThrowable JSON
    // overflows BasicThrowableCodec directly regardless of the host JVM's -Xss setting.
    String stackOverflowErrorJson = buildNestedErrorJson(15_000);

    for (boolean enforcing : Arrays.asList(true, false)) {
      counters.clear();
      lookupCounts.clear();
      RuntimeProgramStatusValidator validator = createValidator(enforcing);
      final Action expectedDrop = enforcing ? Action.SKIP : Action.FORWARD;
      RequestValidator rv = validator.newRequestValidator(wfRun, "programstatusevent0");
      long expectedDrops = 0L;

      // 1. RUNNING with missing or non-numeric logical.start.time (both own and inner runs)
      for (ProgramRunId targetRun : Arrays.asList(wfRun, innerSparkRun)) {
        String ctx = "enforcing=" + enforcing + ", run=" + targetRun;
        Assert.assertEquals("Missing logical.start.time (" + ctx + ")", expectedDrop,
            rv.apply(encodeNotification(Notification.Type.PROGRAM_STATUS, ImmutableMap.of(
                ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(targetRun),
                ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.RUNNING.name()))));
        expectedDrops++;
        Assert.assertEquals("Non-numeric logical.start.time (" + ctx + ")", expectedDrop,
            rv.apply(encodeNotification(Notification.Type.PROGRAM_STATUS, ImmutableMap.of(
                ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(targetRun),
                ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.RUNNING.name(),
                ProgramOptionConstants.LOGICAL_START_TIME, "not-a-number"))));
        expectedDrops++;
      }

      // 2. COMPLETED/FAILED/KILLED with missing/non-numeric endTime or malformed/deep error
      for (ProgramRunStatus endStatus : Arrays.asList(
          ProgramRunStatus.COMPLETED, ProgramRunStatus.FAILED, ProgramRunStatus.KILLED)) {
        for (ProgramRunId targetRun : Arrays.asList(wfRun, innerSparkRun)) {
          String ctx = "enforcing=" + enforcing + ", status=" + endStatus + ", run=" + targetRun;
          Assert.assertEquals("Missing endTime (" + ctx + ")", expectedDrop,
              rv.apply(encodeNotification(Notification.Type.PROGRAM_STATUS, ImmutableMap.of(
                  ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(targetRun),
                  ProgramOptionConstants.PROGRAM_STATUS, endStatus.name()))));
          expectedDrops++;

          Assert.assertEquals("Non-numeric endTime (" + ctx + ")", expectedDrop,
              rv.apply(encodeNotification(Notification.Type.PROGRAM_STATUS, ImmutableMap.of(
                  ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(targetRun),
                  ProgramOptionConstants.PROGRAM_STATUS, endStatus.name(),
                  ProgramOptionConstants.END_TIME, "abc"))));
          expectedDrops++;

          Assert.assertEquals("Malformed error JSON (" + ctx + ")", expectedDrop,
              rv.apply(encodeNotification(Notification.Type.PROGRAM_STATUS, ImmutableMap.of(
                  ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(targetRun),
                  ProgramOptionConstants.PROGRAM_STATUS, endStatus.name(),
                  ProgramOptionConstants.END_TIME, "2000",
                  ProgramOptionConstants.PROGRAM_ERROR, "{not-valid-json"))));
          expectedDrops++;

          Assert.assertEquals("Empty object error JSON (" + ctx + ")", expectedDrop,
              rv.apply(encodeNotification(Notification.Type.PROGRAM_STATUS, ImmutableMap.of(
                  ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(targetRun),
                  ProgramOptionConstants.PROGRAM_STATUS, endStatus.name(),
                  ProgramOptionConstants.END_TIME, "2000",
                  ProgramOptionConstants.PROGRAM_ERROR, "{}"))));
          expectedDrops++;
        }
      }

      // Explicit JSON "message":null and non-object PROGRAM_ERROR payloads -> dropped
      for (String badErrorJson : Arrays.asList(
          "{\"className\":\"java.lang.NullPointerException\",\"message\":null,\"stackTraces\":[]}",
          "123",
          "[]")) {
        Assert.assertEquals("Invalid error payload " + badErrorJson + " (enforcing="
                + enforcing + ")", expectedDrop,
            rv.apply(encodeNotification(Notification.Type.PROGRAM_STATUS, ImmutableMap.of(
                ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(wfRun),
                ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.FAILED.name(),
                ProgramOptionConstants.END_TIME, "2000",
                ProgramOptionConstants.PROGRAM_ERROR, badErrorJson))));
        expectedDrops++;
      }

      // Real FAILED notification with null-message Throwable (and chained cause) -> FORWARD
      List<byte[]> nullMsgCaptured = new ArrayList<>();
      MessagingProgramStateWriter nullMsgWriter = new MessagingProgramStateWriter(
          (type, props) -> nullMsgCaptured.add(
              GSON.toJson(new Notification(type, props)).getBytes(StandardCharsets.UTF_8)));
      nullMsgWriter.error(wfRun, new NullPointerException());
      nullMsgWriter.error(wfRun, new IllegalStateException(new NullPointerException()));
      for (byte[] validNullMsgErr : nullMsgCaptured) {
        Assert.assertEquals(Action.FORWARD, rv.apply(validNullMsgErr));
      }

      // Cause-chain depth boundary: MAX_ERROR_CAUSE_DEPTH passes; +1 and StackOverflowError drop
      Assert.assertEquals(Action.FORWARD, rv.apply(encodeNotification(
          Notification.Type.PROGRAM_STATUS, ImmutableMap.of(
              ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(wfRun),
              ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.FAILED.name(),
              ProgramOptionConstants.END_TIME, "2000",
              ProgramOptionConstants.PROGRAM_ERROR,
              buildNestedErrorJson(RuntimeProgramStatusValidator.MAX_ERROR_CAUSE_DEPTH)))));

      Assert.assertEquals(expectedDrop, rv.apply(encodeNotification(
          Notification.Type.PROGRAM_STATUS, ImmutableMap.of(
              ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(wfRun),
              ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.FAILED.name(),
              ProgramOptionConstants.END_TIME, "2000",
              ProgramOptionConstants.PROGRAM_ERROR,
              buildNestedErrorJson(RuntimeProgramStatusValidator.MAX_ERROR_CAUSE_DEPTH + 1)))));
      expectedDrops++;

      byte[] soePayload = encodeNotification(Notification.Type.PROGRAM_STATUS, ImmutableMap.of(
          ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(wfRun),
          ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.FAILED.name(),
          ProgramOptionConstants.END_TIME, "2000",
          ProgramOptionConstants.PROGRAM_ERROR, stackOverflowErrorJson));
      Throwable[] threadFailure = new Throwable[1];
      Thread soeThread = new Thread(null, () -> {
        try {
          try {
            throwableGson.fromJson(stackOverflowErrorJson, BasicThrowable.class);
            Assert.fail("Expected StackOverflowError for depth-15000 BasicThrowable JSON");
          } catch (StackOverflowError expected) {
            // expected
          }
          Assert.assertEquals(expectedDrop, rv.apply(soePayload));
        } catch (Throwable t) {
          threadFailure[0] = t;
        }
      }, "basic-throwable-soe-test", 256 * 1024);
      soeThread.start();
      try {
        soeThread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError("Interrupted waiting for stack-overflow test thread", e);
      }
      if (threadFailure[0] != null) {
        throw new AssertionError("BasicThrowable stack-overflow check failed", threadFailure[0]);
      }
      expectedDrops++;

      // 3. Inner STARTING with missing, JSON-null, missing-entity, empty/invalid-version, or
      //    foreign-ns programArtifactId -> dropped before any remote lookup
      Map<String, String> validStartProps = GSON.fromJson(
          new String(encodeInnerStartingViaWriter(
              innerSparkRun, validWorkflowOverrides(wfRun, "n1")), StandardCharsets.UTF_8),
          Notification.class).getProperties();

      Map<String, String> missingArtProps = new HashMap<>(validStartProps);
      missingArtProps.remove(ProgramOptionConstants.PROGRAM_ARTIFACT_ID);
      Assert.assertEquals(expectedDrop, rv.apply(
          encodeNotification(Notification.Type.PROGRAM_STATUS, missingArtProps)));
      expectedDrops++;

      Map<String, String> nullArtProps = new HashMap<>(validStartProps);
      nullArtProps.put(ProgramOptionConstants.PROGRAM_ARTIFACT_ID, "null");
      Assert.assertEquals(expectedDrop, rv.apply(
          encodeNotification(Notification.Type.PROGRAM_STATUS, nullArtProps)));
      expectedDrops++;

      Map<String, String> foreignArtProps = new HashMap<>(validStartProps);
      foreignArtProps.put(ProgramOptionConstants.PROGRAM_ARTIFACT_ID,
          GSON.toJson(new NamespaceId("ns2").artifact("app-art", "1.0.0")));
      Assert.assertEquals(expectedDrop, rv.apply(
          encodeNotification(Notification.Type.PROGRAM_STATUS, foreignArtProps)));
      expectedDrops++;

      for (String malformedArtifactJson : Arrays.asList(
          "{\"namespace\":\"ns1\",\"artifact\":\"app-art\",\"version\":\"1.0.0\"}",
          "{\"namespace\":\"ns1\",\"artifact\":\"app-art\",\"version\":\"\",\"entity\":\"ARTIFACT\"}",
          "{\"namespace\":\"ns1\",\"artifact\":\"app-art\",\"version\":\"not-semver\","
              + "\"entity\":\"ARTIFACT\"}")) {
        Map<String, String> badArtProps = new HashMap<>(validStartProps);
        badArtProps.put(ProgramOptionConstants.PROGRAM_ARTIFACT_ID, malformedArtifactJson);
        Assert.assertEquals("Expected drop for artifact JSON " + malformedArtifactJson,
            expectedDrop, rv.apply(
                encodeNotification(Notification.Type.PROGRAM_STATUS, badArtProps)));
        expectedDrops++;
      }

      // 4. Inner STARTING with missing or malformed system.profile.name -> dropped before lookup
      ProgramRunId noProfileInner = new NamespaceId("ns1").app("app1", "1.0").spark("noProf")
          .run(RunIds.generate());
      Map<String, String> noProfileOverrides = validWorkflowOverrides(wfRun, "n2");
      noProfileOverrides.remove(SystemArguments.PROFILE_NAME);
      Assert.assertEquals(expectedDrop, rv.apply(
          encodeInnerStartingViaWriter(noProfileInner, noProfileOverrides)));
      expectedDrops++;

      ProgramRunId badProfileInner = new NamespaceId("ns1").app("app1", "1.0").spark("badProf")
          .run(RunIds.generate());
      Map<String, String> badProfileOverrides = validWorkflowOverrides(wfRun, "n3");
      badProfileOverrides.put(SystemArguments.PROFILE_NAME, "bogus:x");
      Assert.assertEquals(expectedDrop, rv.apply(
          encodeInnerStartingViaWriter(badProfileInner, badProfileOverrides)));
      expectedDrops++;

      // None of the malformed payloads above should trigger a remote run lookup
      Assert.assertTrue("Expected zero remote lookups for malformed status payloads",
          lookupCounts.isEmpty());

      // Positive controls: system-scoped artifact and USER-scoped profile both pass
      Map<String, String> systemArtProps = new HashMap<>(validStartProps);
      systemArtProps.put(ProgramOptionConstants.PROGRAM_ARTIFACT_ID,
          GSON.toJson(NamespaceId.SYSTEM.artifact("cdap-data-pipeline", "6.12.0")));
      Assert.assertEquals(Action.FORWARD, rv.apply(
          encodeNotification(Notification.Type.PROGRAM_STATUS, systemArtProps)));

      ProgramRunId userProfileInner = new NamespaceId("ns1").app("app1", "1.0").spark("userProf")
          .run(RunIds.generate());
      Map<String, String> userProfileOverrides = validWorkflowOverrides(wfRun, "n4");
      userProfileOverrides.put(SystemArguments.PROFILE_NAME,
          new NamespaceId("ns1").profile("custom-dp").getScopedName());
      Assert.assertEquals(Action.FORWARD, rv.apply(
          encodeInnerStartingViaWriter(userProfileInner, userProfileOverrides)));

      rv.finish();
      Assert.assertEquals(Long.valueOf(expectedDrops), counters.get(FAILURE_METRIC));
    }
  }

  @Test
  public void testAdmittedInnerRunOwnerCheckedAcrossSiblingWorkflowRuns() {
    // Enforcing: wfRun admits innerSparkRun (no App-Fabric record yet). Sibling otherWfRun
    // (same namespace/app/version) sending RUNNING/COMPLETED/FAILED/KILLED for innerSparkRun
    // must WAIT before UNKNOWN_WAIT_MILLIS and SKIP + count after UNKNOWN_WAIT_MILLIS, while
    // wfRun still gets FORWARD.
    RuntimeProgramStatusValidator enforcing = createValidator(true);
    RequestValidator rvAdmitEnf = enforcing.newRequestValidator(wfRun, "programstatusevent0");
    Assert.assertEquals(Action.FORWARD, rvAdmitEnf.apply(
        encodeInnerStartingViaWriter(innerSparkRun, validWorkflowOverrides(wfRun, "n1"))));
    rvAdmitEnf.finish();

    for (ProgramRunStatus status : Arrays.asList(ProgramRunStatus.RUNNING,
        ProgramRunStatus.COMPLETED, ProgramRunStatus.FAILED, ProgramRunStatus.KILLED)) {
      RequestValidator rvOtherBefore = enforcing.newRequestValidator(
          otherWfRun, "programstatusevent0");
      Assert.assertEquals("Sibling workflow must WAIT before timeout for " + status,
          Action.WAIT, rvOtherBefore.apply(encodeStatusViaWriter(innerSparkRun, status)));
      rvOtherBefore.finish();
    }
    Assert.assertFalse(counters.containsKey(FAILURE_METRIC));

    // Owner wfRun still gets FORWARD for all 4 statuses
    RequestValidator rvOwnerEnf = enforcing.newRequestValidator(wfRun, "programstatusevent0");
    for (ProgramRunStatus status : Arrays.asList(ProgramRunStatus.RUNNING,
        ProgramRunStatus.COMPLETED, ProgramRunStatus.FAILED, ProgramRunStatus.KILLED)) {
      Assert.assertEquals(Action.FORWARD,
          rvOwnerEnf.apply(encodeStatusViaWriter(innerSparkRun, status)));
    }
    rvOwnerEnf.finish();
    Assert.assertFalse(counters.containsKey(FAILURE_METRIC));

    // After UNKNOWN_WAIT_MILLIS, sibling otherWfRun is SKIPped and counted for all 4 statuses
    clockMillis.addAndGet(RuntimeProgramStatusValidator.UNKNOWN_WAIT_MILLIS);
    RequestValidator rvOtherAfter = enforcing.newRequestValidator(
        otherWfRun, "programstatusevent0");
    for (ProgramRunStatus status : Arrays.asList(ProgramRunStatus.RUNNING,
        ProgramRunStatus.COMPLETED, ProgramRunStatus.FAILED, ProgramRunStatus.KILLED)) {
      Assert.assertEquals("Sibling workflow must SKIP after timeout for " + status,
          Action.SKIP, rvOtherAfter.apply(encodeStatusViaWriter(innerSparkRun, status)));
    }
    rvOtherAfter.finish();
    Assert.assertEquals(Long.valueOf(4L), counters.get(FAILURE_METRIC));

    // Report-only: wfRun admits innerMrRun (no App-Fabric record yet). Sibling otherWfRun
    // leaves a deferred note that is counted once after UNKNOWN_WAIT_MILLIS, while wfRun leaves
    // 0 notes and 0 drops.
    counters.clear();
    RuntimeProgramStatusValidator reportOnly = createValidator(false);
    topLevelRuns.put(otherWfRun, Optional.of(otherWfRun));
    RequestValidator rvAdmitRo = reportOnly.newRequestValidator(wfRun, "programstatusevent0");
    Assert.assertEquals(Action.FORWARD, rvAdmitRo.apply(
        encodeInnerStartingViaWriter(innerMrRun, validWorkflowOverrides(wfRun, "n2"))));
    rvAdmitRo.finish();

    RequestValidator rvOwnerRo = reportOnly.newRequestValidator(wfRun, "programstatusevent0");
    for (ProgramRunStatus status : Arrays.asList(ProgramRunStatus.RUNNING,
        ProgramRunStatus.COMPLETED, ProgramRunStatus.FAILED, ProgramRunStatus.KILLED)) {
      Assert.assertEquals(Action.FORWARD,
          rvOwnerRo.apply(encodeStatusViaWriter(innerMrRun, status)));
    }
    rvOwnerRo.finish();
    Assert.assertEquals(0, reportOnly.getReportOnlyNotesSize());
    Assert.assertFalse(counters.containsKey(FAILURE_METRIC));

    RequestValidator rvOtherRo = reportOnly.newRequestValidator(otherWfRun, "programstatusevent0");
    for (ProgramRunStatus status : Arrays.asList(ProgramRunStatus.RUNNING,
        ProgramRunStatus.COMPLETED, ProgramRunStatus.FAILED, ProgramRunStatus.KILLED)) {
      Assert.assertEquals(Action.FORWARD,
          rvOtherRo.apply(encodeStatusViaWriter(innerMrRun, status)));
    }
    rvOtherRo.finish();
    Assert.assertEquals(1, reportOnly.getReportOnlyNotesSize());
    Assert.assertFalse(counters.containsKey(FAILURE_METRIC));

    clockMillis.addAndGet(RuntimeProgramStatusValidator.UNKNOWN_WAIT_MILLIS);
    reportOnly.newRequestValidator(wfRun, "programstatusevent0").finish();
    Assert.assertEquals(0, reportOnly.getReportOnlyNotesSize());
    Assert.assertEquals(Long.valueOf(1L), counters.get(FAILURE_METRIC));
  }

  @Test
  public void testDefaultGetTopLevelRunAllowsLambdaValidators() {
    ProgramId programId = new NamespaceId("ns1").app("app1", "1.0").spark("sp1");
    ProgramRunId runId = programId.run(RunIds.generate());
    RuntimeRequestValidator lambda =
        (id, req) -> ValidatedRuntimeRequest.legacy(new ProgramRunInfo(Long.MAX_VALUE));
    Assert.assertEquals(Optional.empty(), lambda.getTopLevelRun(runId));
  }
}
