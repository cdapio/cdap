/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.proto;

import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Trigger;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

public class ProtoTriggerCodecTest {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Trigger.class, new ProtoTriggerCodec())
    .registerTypeAdapter(ProtoTrigger.class, new ProtoTriggerCodec())
    .create();

  @Test
  public void testTriggerCodec() {
    ProtoTrigger.PartitionTrigger partitionTrigger =
      new ProtoTrigger.PartitionTrigger(new DatasetId("test", "myds"), 4);
    testTriggerCodec(partitionTrigger);

    ProtoTrigger.TimeTrigger timeTrigger = new ProtoTrigger.TimeTrigger("* * * * *");
    testTriggerCodec(timeTrigger);

    testTriggerCodec(new ProtoTrigger.StreamSizeTrigger(new StreamId("x", "y"), 17));

    ProtoTrigger.ProgramStatusTrigger programStatusTrigger =
      new ProtoTrigger.ProgramStatusTrigger(new ProgramId("test", "myapp", ProgramType.FLOW, "myprog"),
                                            ImmutableSet.of(co.cask.cdap.api.ProgramStatus.FAILED));
    testTriggerCodec(ProtoTrigger.or(ProtoTrigger.and(partitionTrigger,
                                                      programStatusTrigger.or(timeTrigger, programStatusTrigger)),
                                     timeTrigger, programStatusTrigger));
  }

  private void testTriggerCodec(ProtoTrigger trigger) {
    String json = GSON.toJson(trigger);
    Trigger trigger1 = GSON.fromJson(json, Trigger.class);
    Assert.assertEquals(trigger, trigger1);
    json = GSON.toJson(trigger, Trigger.class);
    trigger1 = GSON.fromJson(json, Trigger.class);
    Assert.assertEquals(trigger, trigger1);
  }

  @Test
  public void testObjectContainingTrigger() {
    ScheduleDetail sched1 = new ScheduleDetail("default", "app1", "1.0.0", "sched1", "one partition schedule",
                                               new ScheduleProgramInfo(SchedulableProgramType.WORKFLOW, "ww"),
                                               ImmutableMap.of("prop3", "abc"),
                                               new ProtoTrigger.PartitionTrigger(new DatasetId("test1", "pdfs1"), 1),
                                               ImmutableList.<Constraint>of(), null, "SUSPENDED");
    ScheduleDetail sched2 = new ScheduleDetail("default", "app1", "1.0.0", "schedone", "one time schedule",
                                                 new ScheduleProgramInfo(SchedulableProgramType.WORKFLOW, "wf112"),
                                                 ImmutableMap.of("prop", "all"),
                                                 new ProtoTrigger.TimeTrigger("* * * 1 1"),
                                                 ImmutableList.<Constraint>of(), null, "SUSPENDED");
    Assert.assertEquals(sched1, GSON.fromJson(GSON.toJson(sched1), ScheduleDetail.class));
    Assert.assertEquals(sched2, GSON.fromJson(GSON.toJson(sched2), ScheduleDetail.class));
  }

}
