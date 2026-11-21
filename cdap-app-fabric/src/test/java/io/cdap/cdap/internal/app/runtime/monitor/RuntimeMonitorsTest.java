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
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.DatasetTypeId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.proto.id.SecureKeyId;
import io.cdap.cdap.proto.id.ServiceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.proto.id.WorkflowId;
import java.util.Map;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link RuntimeMonitors}.
 */
public class RuntimeMonitorsTest {

  private static final Gson GSON = new Gson();

  @Test
  public void testTrimSystemArgsKeepsRuntimeScopes() {
    String scopesJson = "{\"connections\":[\"c1\"],\"oauth\":[],\"secrets\":[\"s1\"],"
        + "\"hasExternalDynamicSecret\":false}";
    Map<String, String> input = ImmutableMap.of(
        SystemArguments.PROFILE_NAME, "system:dataproc",
        ProgramOptionConstants.PRINCIPAL, "alice",
        ProgramOptionConstants.RUNTIME_SCOPES, scopesJson,
        "other.system.arg", "dropped");

    Map<String, String> trimmed = RuntimeMonitors.trimSystemArgs(input);

    Assert.assertEquals(ImmutableMap.of(
        SystemArguments.PROFILE_NAME, "system:dataproc",
        ProgramOptionConstants.PRINCIPAL, "alice",
        ProgramOptionConstants.RUNTIME_SCOPES, scopesJson), trimmed);
  }

  @Test
  public void testTrimSystemArgsOmitsAbsentRuntimeScopes() {
    Map<String, String> input = ImmutableMap.of(
        SystemArguments.PROFILE_NAME, "system:dataproc",
        "other.system.arg", "dropped");

    Map<String, String> trimmed = RuntimeMonitors.trimSystemArgs(input);

    Assert.assertEquals(ImmutableMap.of(SystemArguments.PROFILE_NAME, "system:dataproc"), trimmed);
    Assert.assertFalse(trimmed.containsKey(ProgramOptionConstants.RUNTIME_SCOPES));
  }

  @Test
  public void testIsWellFormedValidEntities() {
    NamespaceId ns = new NamespaceId("ns1");
    ApplicationId app = ns.app("app1", "1.0");
    ApplicationId snapshotApp = ns.app("app1", ApplicationId.DEFAULT_VERSION);
    ProgramId program = app.spark("spark1");
    WorkflowId workflow = app.workflow("wf1");
    ServiceId service = app.service("svc1");
    ProgramReference programRef = program.getProgramReference();
    ProgramRunId run = program.run(RunIds.generate());
    DatasetId dataset = ns.dataset("ds1");
    DatasetModuleId datasetModule = ns.datasetModule("dm1");
    DatasetTypeId datasetType = ns.datasetType("dt1");
    ArtifactId artifact = ns.artifact("art1", "1.0.0");
    ScheduleId schedule = app.schedule("sched1");
    TopicId topic = ns.topic("topic1");
    ProfileId profile = ns.profile("prof1");
    SecureKeyId secureKey = ns.secureKey("key1");

    Assert.assertTrue(RuntimeMonitors.isWellFormed(ns));
    Assert.assertTrue(RuntimeMonitors.isWellFormed(app));
    Assert.assertTrue(RuntimeMonitors.isWellFormed(snapshotApp));
    Assert.assertTrue(RuntimeMonitors.isWellFormed(program));
    Assert.assertTrue(RuntimeMonitors.isWellFormed(workflow));
    Assert.assertTrue(RuntimeMonitors.isWellFormed(service));
    Assert.assertTrue(RuntimeMonitors.isWellFormed(programRef));
    Assert.assertTrue(RuntimeMonitors.isWellFormed(run));
    Assert.assertTrue(RuntimeMonitors.isWellFormed(dataset));
    Assert.assertTrue(RuntimeMonitors.isWellFormed(datasetModule));
    Assert.assertTrue(RuntimeMonitors.isWellFormed(datasetType));
    Assert.assertTrue(RuntimeMonitors.isWellFormed(artifact));
    Assert.assertTrue(RuntimeMonitors.isWellFormed(schedule));
    Assert.assertTrue(RuntimeMonitors.isWellFormed(topic));
    Assert.assertTrue(RuntimeMonitors.isWellFormed(profile));
    Assert.assertTrue(RuntimeMonitors.isWellFormed(secureKey));
  }

  @Test
  public void testIsWellFormedNullAndMalformedGsonIds() {
    Assert.assertFalse(RuntimeMonitors.isWellFormed(null));

    // Missing required fields via Gson
    Assert.assertFalse(RuntimeMonitors.isWellFormed(GSON.fromJson("{}", ProgramRunId.class)));
    Assert.assertFalse(RuntimeMonitors.isWellFormed(
        GSON.fromJson("{\"namespace\":\"default\",\"application\":\"app\"}", ProgramId.class)));

    // Wrong entity type
    ProgramRunId validRun = NamespaceId.DEFAULT.app("app", "1.0").spark("s").run(RunIds.generate());
    String wrongEntityJson = GSON.toJson(validRun).replace("\"PROGRAM_RUN\"", "\"DATASET\"");
    Assert.assertFalse(RuntimeMonitors.isWellFormed(
        GSON.fromJson(wrongEntityJson, ProgramRunId.class)));

    // Invalid namespace character
    String invalidNsJson = GSON.toJson(validRun).replace("\"default\"", "\"bad/ns\"");
    Assert.assertFalse(RuntimeMonitors.isWellFormed(
        GSON.fromJson(invalidNsJson, ProgramRunId.class)));

    // ProgramId with CUSTOM_ACTION does not survive rebuild
    ProgramId customAction = new ProgramId(NamespaceId.DEFAULT.app("app", "1.0"),
        ProgramType.CUSTOM_ACTION, "act");
    Assert.assertFalse(RuntimeMonitors.isWellFormed(customAction));
  }

  @Test
  public void testIsWellFormedRejectsInvalidVersionAndProgramNames() {
    String validRun = RunIds.generate().getId();

    for (String badVersion : new String[]{"", ".", "..", "bad ver", "1.0?q=1", "a/b"}) {
      ApplicationId badApp = new ApplicationId("default", "app", badVersion);
      Assert.assertFalse("Expected invalid version: " + badVersion,
          RuntimeMonitors.isWellFormed(badApp));
      Assert.assertFalse("Expected invalid version on ProgramId: " + badVersion,
          RuntimeMonitors.isWellFormed(badApp.spark("s")));
      Assert.assertFalse("Expected invalid version on ProgramRunId: " + badVersion,
          RuntimeMonitors.isWellFormed(badApp.spark("s").run(validRun)));
    }

    ApplicationId validApp = NamespaceId.DEFAULT.app("app", "1.0");
    for (String badProg : new String[]{"", "bad prog", "../other", "p?x=1", "p#frag", "a/b"}) {
      ProgramId badProgId = new ProgramId(validApp, ProgramType.SPARK, badProg);
      Assert.assertFalse("Expected invalid program on ProgramId: " + badProg,
          RuntimeMonitors.isWellFormed(badProgId));
      Assert.assertFalse("Expected invalid program on ProgramReference: " + badProg,
          RuntimeMonitors.isWellFormed(badProgId.getProgramReference()));
      Assert.assertFalse("Expected invalid program on ProgramRunId: " + badProg,
          RuntimeMonitors.isWellFormed(badProgId.run(validRun)));
    }
  }

  @Test
  public void testIsWellFormedRunUuidChecks() {
    ProgramId spark = NamespaceId.DEFAULT.app("app", "1.0").spark("s");
    String validUuid = RunIds.generate().getId();

    // Upper-case UUID is not canonical
    Assert.assertFalse(RuntimeMonitors.isWellFormed(spark.run(validUuid.toUpperCase())));

    // Random (version 4) UUID is not time-based
    Assert.assertFalse(RuntimeMonitors.isWellFormed(spark.run(UUID.randomUUID().toString())));

    // Non-UUID string
    Assert.assertFalse(RuntimeMonitors.isWellFormed(spark.run("not-a-uuid")));

    // Version 1 UUID with timestamp before Unix epoch (1970-01-01)
    // Timestamp = 0x0000000000000001 (100ns after 1582-10-15), version 1, RFC 4122 variant 2
    UUID pre1970 = new UUID(0x0000000100001000L, 0x8000000000000000L);
    Assert.assertEquals(1, pre1970.version());
    Assert.assertEquals(2, pre1970.variant());
    Assert.assertFalse(RuntimeMonitors.isWellFormed(spark.run(pre1970.toString())));

    // Version 1 UUID 100ns before Unix epoch (tests sub-millisecond pre-1970 truncation edge case)
    long epochOffset = 0x01b21dd213814000L;
    UUID subMsPre1970 = buildVersion1Uuid(epochOffset - 1L);
    Assert.assertEquals(1, subMsPre1970.version());
    Assert.assertEquals(2, subMsPre1970.variant());
    Assert.assertFalse(RuntimeMonitors.isWellFormed(spark.run(subMsPre1970.toString())));

    // Exact Unix epoch boundary (1970-01-01T00:00:00Z) is well-formed
    UUID exactEpoch = buildVersion1Uuid(epochOffset);
    Assert.assertEquals(1, exactEpoch.version());
    Assert.assertEquals(2, exactEpoch.variant());
    Assert.assertTrue(RuntimeMonitors.isWellFormed(spark.run(exactEpoch.toString())));
  }

  private static UUID buildVersion1Uuid(long timestamp100Ns) {
    long msb = ((timestamp100Ns & 0xFFFFFFFFL) << 32)
        | (((timestamp100Ns >>> 32) & 0xFFFFL) << 16)
        | (1L << 12)
        | ((timestamp100Ns >>> 48) & 0x0FFFL);
    return new UUID(msb, 0x8000000000000000L);
  }
}
