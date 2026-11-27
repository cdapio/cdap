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

package io.cdap.cdap.internal.app.runtime.schedule.trigger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.api.schedule.TriggerInfo;
import io.cdap.cdap.api.schedule.TriggeringScheduleInfo;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.schedule.DefaultTriggeringScheduleInfo;
import io.cdap.cdap.internal.app.runtime.schedule.TriggeringScheduleInfoAdapter;
import io.cdap.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link TriggerMappedArguments}.
 */
public class TriggerMappedArgumentsTest {

  private static final Gson SCHEDULE_GSON =
      TriggeringScheduleInfoAdapter.addTypeAdapters(new GsonBuilder()).create();

  private static final Gson GSON = new Gson();

  @Test
  public void testEmptyCases() {
    Assert.assertFalse(TriggerMappedArguments.extract(null, true).isUnreadable());
    Assert.assertTrue(TriggerMappedArguments.extract(null, true).getArguments().isEmpty());

    Assert.assertFalse(TriggerMappedArguments.extract(Collections.emptyMap(), false).isUnreadable());
    Assert.assertTrue(
        TriggerMappedArguments.extract(Collections.emptyMap(), false).getArguments().isEmpty());

    Map<String, String> emptyInfo = Collections.singletonMap(
        ProgramOptionConstants.TRIGGERING_SCHEDULE_INFO, "");
    Assert.assertFalse(TriggerMappedArguments.extract(emptyInfo, true).isUnreadable());
    Assert.assertTrue(TriggerMappedArguments.extract(emptyInfo, true).getArguments().isEmpty());

    // Schedule without triggering.properties.mapping
    TriggeringScheduleInfo noMappingSchedule = new DefaultTriggeringScheduleInfo(
        "sched", "desc",
        Collections.singletonList(programStatusTrigger("ns1", "p1", null, Collections.emptyMap())),
        Collections.emptyMap());
    TriggerMappedArguments.Result noMappingResult =
        TriggerMappedArguments.extract(systemArgsWith(noMappingSchedule), true);
    Assert.assertFalse(noMappingResult.isUnreadable());
    Assert.assertTrue(noMappingResult.getArguments().isEmpty());

    // Time-trigger-only schedule with mapping property
    TriggeringScheduleInfo timeOnlySchedule = new DefaultTriggeringScheduleInfo(
        "timeSched", "desc",
        Collections.singletonList(new DefaultTimeTriggerInfo("0 * * * *", 12345L)),
        Collections.singletonMap(TriggerMappedArguments.TRIGGERING_PROPERTIES_MAPPING,
            "{\"arguments\":[{\"source\":\"a\",\"target\":\"b\"}]}"));
    TriggerMappedArguments.Result timeOnlyResult =
        TriggerMappedArguments.extract(systemArgsWith(timeOnlySchedule), true);
    Assert.assertFalse(timeOnlyResult.isUnreadable());
    Assert.assertTrue(timeOnlyResult.getArguments().isEmpty());

    // Schedule JSON with null properties field
    Map<String, String> nullPropsArgs = Collections.singletonMap(
        ProgramOptionConstants.TRIGGERING_SCHEDULE_INFO,
        "{\"name\":\"sched\",\"triggerInfos\":[],\"properties\":null}");
    TriggerMappedArguments.Result nullPropsResult =
        TriggerMappedArguments.extract(nullPropsArgs, true);
    Assert.assertFalse(nullPropsResult.isUnreadable());
    Assert.assertTrue(nullPropsResult.getArguments().isEmpty());
  }

  @Test
  public void testMixedTriggerInfosAndNamespaceFilter() {
    // Mixed time + program-status triggers, plus same pipeline name in two namespaces
    TriggerInfo timeTrigger = new DefaultTimeTriggerInfo("0 * * * *", 12345L);
    TriggerInfo triggerNs1 = programStatusTrigger("ns1", "sharedPipe", null,
        ImmutableMap.of("k", "fromNs1"));
    TriggerInfo triggerNs2 = programStatusTrigger("ns2", "sharedPipe", null,
        ImmutableMap.of("k", "fromNs2"));

    String mappingJson = "{\"arguments\":["
        + "{\"source\":\"k\",\"target\":\"onlyNs1\","
        + " \"pipeline\":{\"namespace\":\"ns1\",\"name\":\"sharedPipe\"}},"
        + "{\"source\":\"k\",\"target\":\"onlyNs2\","
        + " \"pipeline\":{\"namespace\":\"ns2\",\"name\":\"sharedPipe\"}},"
        + "{\"source\":\"k\",\"target\":\"unmatchedNs3\","
        + " \"pipeline\":{\"namespace\":\"ns3\",\"name\":\"sharedPipe\"}},"
        + "{\"source\":\"k\",\"target\":\"skipEmptyPipeline\",\"pipeline\":{}},"
        + "{\"source\":\"k\",\"target\":\"skipMissingName\","
        + " \"pipeline\":{\"namespace\":\"ns1\"}},"
        + "{\"source\":\"k\",\"target\":\"skipMissingNamespace\","
        + " \"pipeline\":{\"name\":\"sharedPipe\"}}"
        + "]}";
    TriggeringScheduleInfo scheduleInfo = new DefaultTriggeringScheduleInfo(
        "mixedSched", "desc",
        ImmutableList.of(timeTrigger, triggerNs1, triggerNs2),
        Collections.singletonMap(TriggerMappedArguments.TRIGGERING_PROPERTIES_MAPPING, mappingJson));

    TriggerMappedArguments.Result result =
        TriggerMappedArguments.extract(systemArgsWith(scheduleInfo), true);
    Assert.assertFalse(result.isUnreadable());
    Assert.assertEquals(
        ImmutableMap.of("onlyNs1", "fromNs1", "onlyNs2", "fromNs2"),
        result.getArguments());
  }

  @Test
  public void testArgumentMappingAndWorkflowTokenPrecedence() {
    BasicWorkflowToken token = new BasicWorkflowToken(1);
    token.setCurrentNode("node1");
    token.put("overrideKey", "fromToken");
    token.put("tokenOnly", "tokenVal");

    Map<String, String> runtimeArgs = ImmutableMap.of(
        "overrideKey", "fromRuntimeArgs",
        "argOnly", "argVal");

    String mappingJson = "{\"arguments\":["
        + "{\"source\":\"overrideKey\",\"target\":\"mappedOverride\"},"
        + "{\"source\":\"tokenOnly\"},"
        + "{\"source\":\"argOnly\",\"target\":\"mappedArg\"},"
        + "{\"source\":\"missingKey\",\"target\":\"ignored\"},"
        + "{\"target\":\"nullSourceIgnored\"}"
        + "]}";

    TriggeringScheduleInfo scheduleInfo = new DefaultTriggeringScheduleInfo(
        "sched", "desc",
        Collections.singletonList(programStatusTrigger("ns1", "upstream", token, runtimeArgs)),
        Collections.singletonMap(TriggerMappedArguments.TRIGGERING_PROPERTIES_MAPPING, mappingJson));

    TriggerMappedArguments.Result result =
        TriggerMappedArguments.extract(systemArgsWith(scheduleInfo), false);
    Assert.assertFalse(result.isUnreadable());
    Assert.assertEquals(
        ImmutableMap.of(
            "mappedOverride", "fromToken",
            "tokenOnly", "tokenVal",
            "mappedArg", "argVal"),
        result.getArguments());
  }

  @Test
  public void testPluginPropertyMappings() {
    Map<String, Map<String, String>> resolvedProps = ImmutableMap.of(
        "StageA", ImmutableMap.of("prop1", "val1", "prop2", "val2"));
    BasicWorkflowToken token = new BasicWorkflowToken(1);
    token.setCurrentNode("node1");
    token.put(TriggerMappedArguments.RESOLVED_PLUGIN_PROPERTIES_MAP, GSON.toJson(resolvedProps));

    String mappingJson = "{\"pluginProperties\":["
        + "{\"stageName\":\"StageA\",\"source\":\"prop1\",\"target\":\"targetProp\"},"
        + "{\"stageName\":\"StageA\",\"source\":\"prop2\"},"
        + "{\"stageName\":\"StageA\",\"source\":\"missingProp\",\"target\":\"skip1\"},"
        + "{\"stageName\":\"MissingStage\",\"source\":\"prop1\",\"target\":\"skip2\"},"
        + "{\"source\":\"prop1\",\"target\":\"skipNullStage\"},"
        + "{\"stageName\":\"StageA\",\"target\":\"skipNullSource\"}"
        + "]}";

    TriggeringScheduleInfo scheduleInfo = new DefaultTriggeringScheduleInfo(
        "sched", "desc",
        Collections.singletonList(
            programStatusTrigger("ns1", "upstream", token, Collections.emptyMap())),
        Collections.singletonMap(TriggerMappedArguments.TRIGGERING_PROPERTIES_MAPPING, mappingJson));

    TriggerMappedArguments.Result result =
        TriggerMappedArguments.extract(systemArgsWith(scheduleInfo), true);
    Assert.assertFalse(result.isUnreadable());
    Assert.assertEquals(
        ImmutableMap.of("targetProp", "val1", "prop2", "val2"),
        result.getArguments());
  }

  @Test
  public void testCompositeFlagOnAndOff() {
    Map<String, Map<String, String>> resolvedProps1 = ImmutableMap.of(
        "S1", ImmutableMap.of("p", "v1"));
    BasicWorkflowToken token1 = new BasicWorkflowToken(1);
    token1.setCurrentNode("n1");
    token1.put(TriggerMappedArguments.RESOLVED_PLUGIN_PROPERTIES_MAP, GSON.toJson(resolvedProps1));

    // Second trigger intentionally lacks resolved.plugin.properties.map; no plugin property
    // mapping targets p2, so composite=true must succeed.
    TriggerInfo trigger1 = programStatusTrigger("ns1", "p1", token1,
        ImmutableMap.of("k1", "val1", "legacy", "legacy1"));
    TriggerInfo trigger2 = programStatusTrigger("ns2", "p2", null,
        ImmutableMap.of("k2", "val2", "legacy", "legacy2"));

    String mappingJson = "{"
        + "\"arguments\":["
        + "  {\"source\":\"k1\",\"target\":\"out1\",\"pipeline\":{\"namespace\":\"ns1\",\"name\":\"p1\"}},"
        + "  {\"source\":\"k2\",\"target\":\"out2\",\"pipeline\":{\"namespace\":\"ns2\",\"name\":\"p2\"}},"
        + "  {\"source\":\"legacy\",\"target\":\"legacyOut\"}"
        + "],"
        + "\"pluginProperties\":["
        + "  {\"stageName\":\"S1\",\"source\":\"p\",\"target\":\"stageOut\","
        + "   \"pipeline\":{\"namespace\":\"ns1\",\"name\":\"p1\"}}"
        + "]}";

    TriggeringScheduleInfo scheduleInfo = new DefaultTriggeringScheduleInfo(
        "compositeSched", "desc",
        ImmutableList.of(trigger1, trigger2),
        Collections.singletonMap(TriggerMappedArguments.TRIGGERING_PROPERTIES_MAPPING, mappingJson));
    Map<String, String> systemArgs = systemArgsWith(scheduleInfo);

    // Composite enabled: applies both triggers, filters by pipeline, pre-6.8 null pipeline matches both
    TriggerMappedArguments.Result compositeOn = TriggerMappedArguments.extract(systemArgs, true);
    Assert.assertFalse(compositeOn.isUnreadable());
    Assert.assertEquals(
        ImmutableMap.of(
            "out1", "val1",
            "out2", "val2",
            "legacyOut", "legacy2",
            "stageOut", "v1"),
        compositeOn.getArguments());

    // Composite disabled: only first trigger applied, pipeline filter ignored
    TriggerMappedArguments.Result compositeOff = TriggerMappedArguments.extract(systemArgs, false);
    Assert.assertFalse(compositeOff.isUnreadable());
    Assert.assertEquals(
        ImmutableMap.of(
            "out1", "val1",
            "legacyOut", "legacy1",
            "stageOut", "v1"),
        compositeOff.getArguments());
  }

  @Test
  public void testCompositeOffIgnoresPipelineFilter() {
    Map<String, Map<String, String>> resolvedProps = ImmutableMap.of(
        "Stage1", ImmutableMap.of("p", "propVal"));
    BasicWorkflowToken token = new BasicWorkflowToken(1);
    token.setCurrentNode("n1");
    token.put(TriggerMappedArguments.RESOLVED_PLUGIN_PROPERTIES_MAP, GSON.toJson(resolvedProps));

    TriggerInfo trigger = programStatusTrigger("ns1", "actualApp", token,
        ImmutableMap.of("k", "val"));
    String mappingJson = "{"
        + "\"arguments\":[{"
        + "  \"source\":\"k\",\"target\":\"out\","
        + "  \"pipeline\":{\"namespace\":\"ns1\",\"name\":\"otherApp\"}"
        + "}],"
        + "\"pluginProperties\":[{"
        + "  \"stageName\":\"Stage1\",\"source\":\"p\",\"target\":\"propOut\","
        + "  \"pipeline\":{\"namespace\":\"ns1\",\"name\":\"otherApp\"}"
        + "}]}";
    TriggeringScheduleInfo scheduleInfo = new DefaultTriggeringScheduleInfo(
        "sched", "desc",
        Collections.singletonList(trigger),
        Collections.singletonMap(TriggerMappedArguments.TRIGGERING_PROPERTIES_MAPPING, mappingJson));
    Map<String, String> systemArgs = systemArgsWith(scheduleInfo);

    TriggerMappedArguments.Result compositeOn = TriggerMappedArguments.extract(systemArgs, true);
    Assert.assertFalse(compositeOn.isUnreadable());
    Assert.assertTrue(compositeOn.getArguments().isEmpty());

    TriggerMappedArguments.Result compositeOff = TriggerMappedArguments.extract(systemArgs, false);
    Assert.assertFalse(compositeOff.isUnreadable());
    Assert.assertEquals(
        ImmutableMap.of("out", "val", "propOut", "propVal"),
        compositeOff.getArguments());
  }

  @Test
  public void testUnreadableInputsNeverThrow() {
    // Malformed triggeringScheduleInfo JSON
    assertUnreadable(Collections.singletonMap(
        ProgramOptionConstants.TRIGGERING_SCHEDULE_INFO, "{not-json"), true);

    // JSON null triggeringScheduleInfo
    assertUnreadable(Collections.singletonMap(
        ProgramOptionConstants.TRIGGERING_SCHEDULE_INFO, "null"), true);

    // Malformed triggering.properties.mapping JSON
    TriggeringScheduleInfo badMappingJson = new DefaultTriggeringScheduleInfo(
        "sched", "desc",
        Collections.singletonList(programStatusTrigger("ns1", "p1", null, Collections.emptyMap())),
        Collections.singletonMap(TriggerMappedArguments.TRIGGERING_PROPERTIES_MAPPING, "{broken"));
    assertUnreadable(systemArgsWith(badMappingJson), true);

    // JSON null triggering.properties.mapping
    TriggeringScheduleInfo nullMappingJson = new DefaultTriggeringScheduleInfo(
        "sched", "desc",
        Collections.singletonList(programStatusTrigger("ns1", "p1", null, Collections.emptyMap())),
        Collections.singletonMap(TriggerMappedArguments.TRIGGERING_PROPERTIES_MAPPING, "null"));
    assertUnreadable(systemArgsWith(nullMappingJson), true);

    // Null entry in arguments list
    TriggeringScheduleInfo nullArgEntry = new DefaultTriggeringScheduleInfo(
        "sched", "desc",
        Collections.singletonList(programStatusTrigger("ns1", "p1", null, Collections.emptyMap())),
        Collections.singletonMap(TriggerMappedArguments.TRIGGERING_PROPERTIES_MAPPING,
            "{\"arguments\":[null]}"));
    assertUnreadable(systemArgsWith(nullArgEntry), true);

    // Null entry in pluginProperties list
    TriggeringScheduleInfo nullPluginEntry = new DefaultTriggeringScheduleInfo(
        "sched", "desc",
        Collections.singletonList(programStatusTrigger("ns1", "p1", null, Collections.emptyMap())),
        Collections.singletonMap(TriggerMappedArguments.TRIGGERING_PROPERTIES_MAPPING,
            "{\"pluginProperties\":[null]}"));
    assertUnreadable(systemArgsWith(nullPluginEntry), true);

    // Missing resolved.plugin.properties.map when a matching plugin property mapping applies
    TriggeringScheduleInfo missingResolvedProps = new DefaultTriggeringScheduleInfo(
        "sched", "desc",
        Collections.singletonList(programStatusTrigger("ns1", "p1", null, Collections.emptyMap())),
        Collections.singletonMap(TriggerMappedArguments.TRIGGERING_PROPERTIES_MAPPING,
            "{\"pluginProperties\":[{\"stageName\":\"S1\",\"source\":\"p\",\"target\":\"t\"}]}"));
    assertUnreadable(systemArgsWith(missingResolvedProps), true);

    // Malformed resolved.plugin.properties.map JSON
    BasicWorkflowToken badToken = new BasicWorkflowToken(1);
    badToken.setCurrentNode("n1");
    badToken.put(TriggerMappedArguments.RESOLVED_PLUGIN_PROPERTIES_MAP, "{not-valid-json");
    TriggeringScheduleInfo malformedResolvedProps = new DefaultTriggeringScheduleInfo(
        "sched", "desc",
        Collections.singletonList(
            programStatusTrigger("ns1", "p1", badToken, Collections.emptyMap())),
        Collections.singletonMap(TriggerMappedArguments.TRIGGERING_PROPERTIES_MAPPING,
            "{\"pluginProperties\":[{\"stageName\":\"S1\",\"source\":\"p\",\"target\":\"t\"}]}"));
    assertUnreadable(systemArgsWith(malformedResolvedProps), true);

    // Malformed ProgramStatusTriggerInfo with null namespace/applicationName + empty pipeline
    String nullPipelineFieldsJson = "{"
        + "\"name\":\"sched\","
        + "\"triggerInfos\":[{\"type\":\"PROGRAM_STATUS\",\"runtimeArguments\":{\"k\":\"v\"}}],"
        + "\"properties\":{\"triggering.properties.mapping\":"
        + "\"{\\\"arguments\\\":[{\\\"source\\\":\\\"k\\\",\\\"pipeline\\\":{}}]}\"}"
        + "}";
    assertUnreadable(Collections.singletonMap(
        ProgramOptionConstants.TRIGGERING_SCHEDULE_INFO, nullPipelineFieldsJson), true);

    // Deeply nested JSON inside triggerInfos must not throw StackOverflowError
    StringBuilder deepJson = new StringBuilder("{\"name\":\"sched\",\"triggerInfos\":[{\"x\":");
    int depth = 10000;
    for (int i = 0; i < depth; i++) {
      deepJson.append('[');
    }
    for (int i = 0; i < depth; i++) {
      deepJson.append(']');
    }
    deepJson.append("}]}");
    assertUnreadable(Collections.singletonMap(
        ProgramOptionConstants.TRIGGERING_SCHEDULE_INFO, deepJson.toString()), true);
  }

  private static void assertUnreadable(Map<String, String> systemArgs,
      boolean compositeTriggerEnabled) {
    TriggerMappedArguments.Result result =
        TriggerMappedArguments.extract(systemArgs, compositeTriggerEnabled);
    Assert.assertTrue(result.isUnreadable());
    Assert.assertTrue(result.getArguments().isEmpty());
  }

  private static Map<String, String> systemArgsWith(TriggeringScheduleInfo scheduleInfo) {
    return Collections.singletonMap(
        ProgramOptionConstants.TRIGGERING_SCHEDULE_INFO,
        SCHEDULE_GSON.toJson(scheduleInfo));
  }

  private static DefaultProgramStatusTriggerInfo programStatusTrigger(String namespace,
      String appName, @Nullable WorkflowToken token, Map<String, String> runtimeArgs) {
    return new DefaultProgramStatusTriggerInfo(
        namespace, appName, ProgramType.WORKFLOW, "DataPipelineWorkflow",
        RunIds.generate(), ProgramStatus.COMPLETED, token, runtimeArgs);
  }
}
