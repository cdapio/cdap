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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.proto.ArgumentMapping;
import io.cdap.cdap.proto.PluginPropertyMapping;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.TriggeringInfo;
import io.cdap.cdap.proto.TriggeringPipelineId;
import io.cdap.cdap.proto.TriggeringPropertyMapping;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.ScheduleId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TriggeringInfoCodecTest {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(TriggeringInfo.class, new TriggeringInfoCodec())
    .create();

  @Test
  public void testCodec() {
    // test for imeTriggeringIngo
    TriggeringInfo time1 = createTimeTriggeringInfo(true);
    TriggeringInfo time2 = createTimeTriggeringInfo(false);
    Assert.assertEquals(time1, GSON.fromJson(GSON.toJson(time1), TriggeringInfo.class));
    Assert.assertNotEquals(time1, GSON.fromJson(GSON.toJson(time2), TriggeringInfo.class));
    // test for ProgramStatusTriggeringInfo
    TriggeringInfo prog1 = createProgramStatusTriggeringInfo(true);
    TriggeringInfo prog2 = createProgramStatusTriggeringInfo(false);
    Assert.assertEquals(prog1, GSON.fromJson(GSON.toJson(prog1), TriggeringInfo.class));
    Assert.assertNotEquals(prog1, GSON.fromJson(GSON.toJson(prog2), TriggeringInfo.class));
    // test for PartitionTriggeringInfo
    TriggeringInfo part1 = createPartitionTriggeringInfo(true);
    TriggeringInfo part2 = createPartitionTriggeringInfo(false);
    Assert.assertEquals(part1, GSON.fromJson(GSON.toJson(part1), TriggeringInfo.class));
    Assert.assertNotEquals(part1, GSON.fromJson(GSON.toJson(part2), TriggeringInfo.class));
    // test for AndTriggeringInfo
    TriggeringInfo and1 = createAndTriggeringInfo(true);
    TriggeringInfo and2 = createAndTriggeringInfo(false);
    Assert.assertEquals(and1, GSON.fromJson(GSON.toJson(and1), TriggeringInfo.class));
    Assert.assertNotEquals(and1, GSON.fromJson(GSON.toJson(and2), TriggeringInfo.class));
    // test for OrTriggeringInfo
    TriggeringInfo or1 = createAndTriggeringInfo(true);
    TriggeringInfo or2 = createAndTriggeringInfo(false);
    Assert.assertEquals(or1, GSON.fromJson(GSON.toJson(or1), TriggeringInfo.class));
    Assert.assertNotEquals(or1, GSON.fromJson(GSON.toJson(or2), TriggeringInfo.class));
  }

  private TriggeringInfo createTimeTriggeringInfo(boolean first) {
    ScheduleId scheduleId;
    Map<String, String> runTimeArgs = new HashMap<>();
    runTimeArgs.put("args1", "val1");
    String cron;
    if (first) {
      scheduleId = new ScheduleId("ns", "app", "ver", "sch1");
      cron = "* * * 1 1";
      runTimeArgs.put("args2", "val2");
    } else {
      scheduleId = new ScheduleId("ns", "app", "ver", "sch2");
      cron = "* * * 1 2";
      runTimeArgs.put("args2", "val3");
    }
    return new TriggeringInfo.TimeTriggeringInfo(scheduleId, runTimeArgs, cron);
  }

  private TriggeringInfo createProgramStatusTriggeringInfo(boolean first) {
    ScheduleId scheduleId = new ScheduleId("ns", "app", "ver", "sch");
    Map<String, String> runTimeArgs = new HashMap<>();
    runTimeArgs.put("args1", "val1");
    ProgramRunId runId;
    if (first) {
      runId = new ProgramRunId("ns", "app",
                               ProgramType.WORKFLOW, "prg", "runid1");
    } else {
      runId = new ProgramRunId("ns", "app",
                               ProgramType.WORKFLOW, "prg", "runid2");
    }
    return new TriggeringInfo.ProgramStatusTriggeringInfo(scheduleId, runTimeArgs, runId);
  }

  private TriggeringInfo createPartitionTriggeringInfo(boolean first) {
    ScheduleId scheduleId = new ScheduleId("ns", "app", "ver", "sch");
    Map<String, String> runTimeArgs = new HashMap<>();
    runTimeArgs.put("args1", "val1");
    if (first) {
      return new TriggeringInfo
        .PartitionTriggeringInfo(scheduleId, runTimeArgs, "ds", "dsns", 5, 12);
    } else {
      return new TriggeringInfo
        .PartitionTriggeringInfo(scheduleId, runTimeArgs, "ds", "dsns", 5, 13);
    }
  }

  private TriggeringInfo createAndTriggeringInfo(boolean first) {
    ScheduleId scheduleId = new ScheduleId("ns", "app", "ver", "sch");
    Map<String, String> runTimeArgs = new HashMap<>();
    runTimeArgs.put("args1", "val1");
    if (first) {
      TriggeringInfo time = createTimeTriggeringInfo(true);
      TriggeringInfo prog = createProgramStatusTriggeringInfo(true);
      return new TriggeringInfo.AndTriggeringInfo(Arrays.asList(new TriggeringInfo[]{time, prog}),
                                                  scheduleId, runTimeArgs, createTriggeringPropertyMapping(true));
    } else {
      TriggeringInfo time = createTimeTriggeringInfo(true);
      TriggeringInfo part = createPartitionTriggeringInfo(true);
      return new TriggeringInfo.AndTriggeringInfo(Arrays.asList(new TriggeringInfo[]{time, part}),
                                                 scheduleId, runTimeArgs, createTriggeringPropertyMapping(false));
    }
  }

  private TriggeringInfo createOrTriggeringInfo(boolean first) {
    ScheduleId scheduleId = new ScheduleId("ns", "app", "ver", "sch");
    Map<String, String> runTimeArgs = new HashMap<>();
    runTimeArgs.put("args1", "val1");
    if (first) {
      TriggeringInfo time = createTimeTriggeringInfo(true);
      TriggeringInfo prog = createProgramStatusTriggeringInfo(true);
      return new TriggeringInfo.OrTriggeringInfo(Arrays.asList(new TriggeringInfo[]{time, prog}),
                                                  scheduleId, runTimeArgs, createTriggeringPropertyMapping(true));
    } else {
      TriggeringInfo time = createTimeTriggeringInfo(true);
      TriggeringInfo part = createPartitionTriggeringInfo(true);
      return new TriggeringInfo.OrTriggeringInfo(Arrays.asList(new TriggeringInfo[]{time, part}),
                                                  scheduleId, runTimeArgs, createTriggeringPropertyMapping(false));
    }
  }

  private TriggeringPropertyMapping createTriggeringPropertyMapping(boolean first) {
    ArgumentMapping arg1 = new ArgumentMapping("src", "tgt1",
                                               new TriggeringPipelineId("ns", "name"));
    ArgumentMapping arg2 = new ArgumentMapping("src", "tgt2",
                                               new TriggeringPipelineId("ns", "name"));
    PluginPropertyMapping plg1 = new PluginPropertyMapping("stgName", "src", "tgt1",
                                                           new TriggeringPipelineId("ns", "name"));
    PluginPropertyMapping plg2 = new PluginPropertyMapping("stgName", "src", "tgt1",
                                                           new TriggeringPipelineId("ns", "name"));
    if (first) {
      return new TriggeringPropertyMapping(
        Arrays.asList(new ArgumentMapping[]{arg1, arg2}),
        Arrays.asList(new PluginPropertyMapping[]{plg1, plg2})
      );
    } else {
      return new TriggeringPropertyMapping(
        Arrays.asList(new ArgumentMapping[]{arg1, arg2}),
        Arrays.asList(new PluginPropertyMapping[]{plg1})
      );
    }
  }
}
