/*
 * Copyright © 2022 Cask Data, Inc.
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

import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.api.schedule.TriggerInfo;
import io.cdap.cdap.api.schedule.TriggeringScheduleInfo;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.internal.app.runtime.schedule.DefaultTriggeringScheduleInfo;
import io.cdap.cdap.proto.ArgumentMapping;
import io.cdap.cdap.proto.PluginPropertyMapping;
import io.cdap.cdap.proto.RunStartMetadata;
import io.cdap.cdap.proto.TriggeringInfo;
import io.cdap.cdap.proto.TriggeringPipelineId;
import io.cdap.cdap.proto.TriggeringPropertyMapping;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.spi.events.StartMetadata;
import io.cdap.cdap.spi.events.StartType;
import io.cdap.cdap.spi.events.trigger.CompositeTriggeringInfo;
import io.cdap.cdap.spi.events.trigger.PartitionTriggeringInfo;
import io.cdap.cdap.spi.events.trigger.ProgramStatusTriggeringInfo;
import io.cdap.cdap.spi.events.trigger.TimeTriggeringInfo;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link TriggeringInfos}
 */
public class TriggeringInfosTest {

  private static TriggerInfo time, program, partition;
  private static TriggeringInfo timeProto, programProto, partitionProto, andProto;
  private static String namespace, appName, programName, version, cron, scheduleName, runId;
  private static String triggerMappingString = "{\"arguments\": [],\"pluginProperties\": [{\"pipelineId\": " +
    "{\"namespace\": \"testNameSpace\",\"pipelineName\": \"pipeline\"},\"stageName\": \"File\",\"source\": " +
    "\"sampleSize\",\"target\": \"sample-size-arg\"}]}";

  @BeforeClass
  public static void before() {
    namespace = "testNameSpace";
    appName = "testAppName";
    programName = "testProgram";
    version = "1.0.0";
    cron = "* * * * 1 1";
    scheduleName = "testSchedule";
    runId = "678d2401-1dd4-11b2-8ff7-000000dae054";
    time = new DefaultTimeTriggerInfo(cron, 1000000);
    program = new DefaultProgramStatusTriggerInfo(namespace, appName, ProgramType.WORKFLOW, programName,
                                                  RunIds.generate(), ProgramStatus.COMPLETED,
                                                  null, Collections.emptyMap());
    partition = new DefaultPartitionTriggerInfo(namespace, "testDataset", 10, 20);

    TriggeringPropertyMapping mapping = new TriggeringPropertyMapping(
      Arrays.asList(new ArgumentMapping[]{}),
      Arrays.asList(new PluginPropertyMapping[]{
        new PluginPropertyMapping("File", "sampleSize", "sample-size-arg",
                                  new TriggeringPipelineId(namespace, "pipeline"))
      })
    );

    timeProto = new TriggeringInfo
      .TimeTriggeringInfo(createScheduleId(), Collections.emptyMap(), cron);
    programProto = new TriggeringInfo.ProgramStatusTriggeringInfo(createScheduleId(),
                                   Collections.emptyMap(),
                                   new ProgramRunId(namespace, appName,
                                                    io.cdap.cdap.proto.ProgramType.WORKFLOW, programName,
                                                    runId));
    partitionProto = new TriggeringInfo
      .PartitionTriggeringInfo(createScheduleId(), Collections.emptyMap(),
                               "testDataset", namespace, 10, 20);
    TriggeringInfo timeTriggeringInfo = new TriggeringInfo
      .TimeTriggeringInfo(createScheduleId(), Collections.emptyMap(), cron);
    TriggeringInfo partTriggeringInfo = new TriggeringInfo
      .PartitionTriggeringInfo(createScheduleId(), Collections.emptyMap(),
                               "testDataset", namespace, 10, 20);
    andProto = new TriggeringInfo
      .AndTriggeringInfo(Arrays.asList(new TriggeringInfo[]{partTriggeringInfo, timeTriggeringInfo}),
                         createScheduleId(), Collections.emptyMap(), mapping);
  }

  @Test
  public void testProgramStatus() {
    TriggeringScheduleInfo info = new DefaultTriggeringScheduleInfo("name", "desc",
                                                                    Arrays.asList(new TriggerInfo[]{program}),
                                                                    Collections.emptyMap());

    TriggeringInfo triggeringInfo = TriggeringInfos
      .fromTriggeringScheduleInfo(info, Trigger.Type.PROGRAM_STATUS, createScheduleId());

    verifyProgramStatusTriggeringInfo((TriggeringInfo.ProgramStatusTriggeringInfo) triggeringInfo,
                                      (TriggeringInfo.ProgramStatusTriggeringInfo) programProto);
  }

  @Test
  public void testTime() {
    TriggeringScheduleInfo info = new DefaultTriggeringScheduleInfo("name", "desc",
                                                                    Arrays.asList(new TriggerInfo[]{time}),
                                                                    Collections.emptyMap());
    TriggeringInfo triggeringInfo = TriggeringInfos
      .fromTriggeringScheduleInfo(info, Trigger.Type.TIME, createScheduleId());
    Assert.assertEquals(triggeringInfo, timeProto);
  }

  @Test
  public void testPartition() {
    TriggeringScheduleInfo info = new DefaultTriggeringScheduleInfo("name", "desc",
                                                                    Arrays.asList(new TriggerInfo[]{partition}),
                                                                    Collections.emptyMap());
    TriggeringInfo triggeringInfo = TriggeringInfos
      .fromTriggeringScheduleInfo(info, Trigger.Type.PARTITION, createScheduleId());
    Assert.assertEquals(triggeringInfo, partitionProto);
  }

  @Test
  public void testAnd() {
    Map<String, String> props = new HashMap<>();
    props.put(TriggeringInfos.TRIGGERING_PROPERTIES_MAPPING, triggerMappingString);
    TriggeringScheduleInfo info = new DefaultTriggeringScheduleInfo("name", "desc",
                                                                    Arrays.asList(new TriggerInfo[]{partition, time}),
                                                                    props);
    TriggeringInfo triggeringInfo = TriggeringInfos
      .fromTriggeringScheduleInfo(info, Trigger.Type.AND, createScheduleId());
    Assert.assertEquals(triggeringInfo, andProto);
  }

  @Test
  public void testFromProtoToSpi() {
    // Program status
    StartMetadata programFromProto =
      TriggeringInfos.fromProtoToSpi(new RunStartMetadata(RunStartMetadata.Type.PROGRAM_STATUS, programProto));
    ProgramStatusTriggeringInfo programSpi =
      new ProgramStatusTriggeringInfo(new io.cdap.cdap.spi.events.trigger.ScheduleId(appName, scheduleName),
                                      runId, programName, appName, namespace, Collections.emptyMap());
    StartMetadata programStartMetadata = new StartMetadata(StartType.PROGRAM_STATUS, programSpi);
    Assert.assertEquals(programFromProto, programStartMetadata);

    // Time
    StartMetadata timeFromProto =
      TriggeringInfos.fromProtoToSpi(new RunStartMetadata(RunStartMetadata.Type.TIME, timeProto));
    TimeTriggeringInfo timeSpi =
      new TimeTriggeringInfo(new io.cdap.cdap.spi.events.trigger.ScheduleId(appName, scheduleName),
                                      cron, Collections.emptyMap());
    StartMetadata timeStartMetadata = new StartMetadata(StartType.TIME, timeSpi);
    Assert.assertEquals(timeFromProto, timeStartMetadata);

    // Partition
    StartMetadata partitionFromProto =
      TriggeringInfos.fromProtoToSpi(new RunStartMetadata(RunStartMetadata.Type.PARTITION, partitionProto));
    PartitionTriggeringInfo partitionSpi =
      new PartitionTriggeringInfo(namespace, "testDataset", 10, 20);
    StartMetadata partitionStartMetadata = new StartMetadata(StartType.PARTITION, partitionSpi);
    Assert.assertEquals(partitionFromProto, partitionStartMetadata);

    // And
    StartMetadata andFromProto =
      TriggeringInfos.fromProtoToSpi(new RunStartMetadata(RunStartMetadata.Type.AND, andProto));
    CompositeTriggeringInfo compositeSpi
      = new CompositeTriggeringInfo(io.cdap.cdap.spi.events.trigger.TriggeringInfo.Type.AND,
                                    new io.cdap.cdap.spi.events.trigger.ScheduleId(appName, scheduleName),
                                    Arrays.asList(new io.cdap.cdap.spi.events
                                      .trigger.TriggeringInfo[] {partitionSpi, timeSpi}),
                                    getSpiMapping());
    StartMetadata andStartMetadata = new StartMetadata(StartType.AND, compositeSpi);
    Assert.assertEquals(andFromProto, andStartMetadata);
  }

  private static ScheduleId createScheduleId() {
    return new ScheduleId(namespace, appName, version, scheduleName);
  }

  private static io.cdap.cdap.spi.events.trigger.TriggeringPropertyMapping getSpiMapping() {
    return new io.cdap.cdap.spi.events.trigger.TriggeringPropertyMapping(
      Arrays.asList(new io.cdap.cdap.spi.events.trigger.ArgumentMapping[]{}),
      Arrays.asList(new io.cdap.cdap.spi.events.trigger.PluginPropertyMapping[]{
        new io.cdap.cdap.spi.events.trigger.PluginPropertyMapping("File",
                                                                  "sampleSize", "sample-size-arg",
                                                                  namespace, "pipeline")})
    );
  }

  private void verifyProgramStatusTriggeringInfo(TriggeringInfo.ProgramStatusTriggeringInfo a,
                                                 TriggeringInfo.ProgramStatusTriggeringInfo b) {
    Assert.assertEquals(a.getScheduleId(), b.getScheduleId());
    Assert.assertEquals(a.getRuntimeArguments(), b.getRuntimeArguments());
    Assert.assertEquals(a.getProgramRunId().getProgram(), b.getProgramRunId().getProgram());
    Assert.assertEquals(a.getProgramRunId().getApplication(), b.getProgramRunId().getApplication());
    Assert.assertEquals(a.getProgramRunId().getVersion(), b.getProgramRunId().getVersion());
    Assert.assertEquals(a.getProgramRunId().getType(), b.getProgramRunId().getType());
  }
}
