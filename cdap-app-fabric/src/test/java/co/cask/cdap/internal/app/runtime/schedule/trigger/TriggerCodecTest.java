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

package co.cask.cdap.internal.app.runtime.schedule.trigger;

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.internal.schedule.trigger.Trigger;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ProtoTrigger;
import co.cask.cdap.proto.ProtoTriggerCodec;
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

public class TriggerCodecTest {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Trigger.class, new TriggerCodec())
    .create();

  private static final Gson GSON_PROTO = new GsonBuilder()
    .registerTypeAdapter(Trigger.class, new ProtoTriggerCodec())
    .create();

  @Test
  public void testTimeTriggerValidation() {
    // Cron with wrong number of parts
    assertDeserializeFail(new ProtoTrigger.TimeTrigger("* * * ?"));
    // Quartz doesn't support '?' in both day-of-the-month and day-of-the-week
    assertDeserializeFail(new ProtoTrigger.TimeTrigger("* * * ? 1 ?"));
    // Quartz doesn't support wild-card '*' in day-of-the-month or day-of-the-week if neither of them is '?'
    // Cron entry with resolution in minutes will have wild-card '*' in day-of-the-month or day-of-the-week
    // replaced by '?' if neither of them is '?', before it's parsed by Quartz
    GSON.toJson(new ProtoTrigger.TimeTrigger("* * * 1 *"), Trigger.class);
    GSON.toJson(new ProtoTrigger.TimeTrigger("* * 1 1 *"), Trigger.class);
    GSON.toJson(new ProtoTrigger.TimeTrigger("* * * 1 1"), Trigger.class);
    // Cron entry with resolution in seconds will be parsed directly by Quartz
    assertDeserializeFail(new ProtoTrigger.TimeTrigger("* * * * 1 *"));
    assertDeserializeFail(new ProtoTrigger.TimeTrigger("* * * * 1 1"));
    assertDeserializeFail(new ProtoTrigger.TimeTrigger("* * * 1 1 *"));

    GSON.toJson(new ProtoTrigger.TimeTrigger("* * ? 1 1"), Trigger.class);
    GSON.toJson(new ProtoTrigger.TimeTrigger("* * * 1 ?"), Trigger.class);
    GSON.toJson(new ProtoTrigger.TimeTrigger("* * ? 1 *"), Trigger.class);
    GSON.toJson(new ProtoTrigger.TimeTrigger("* * * ? 1 1"), Trigger.class);
    GSON.toJson(new ProtoTrigger.TimeTrigger("* * * * 1 ?"), Trigger.class);
    GSON.toJson(new ProtoTrigger.TimeTrigger("* * * ? 1 *"), Trigger.class);
  }

  private void assertDeserializeFail(ProtoTrigger.TimeTrigger trigger) {
    // ProtoTriggerCodec only checks whether TimeTrigger contains null cron entry
    GSON_PROTO.toJson(trigger, ProtoTrigger.class);
    try {
      // TriggerCodec checks whether the cron entry in TimeTrigger can be converted to correct Quartz cron format
      GSON.fromJson(GSON.toJson(trigger, Trigger.class), Trigger.class);
      Assert.fail(String.format("Deserializing invalid trigger %s should fail.", trigger));
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testTriggerCodec() {
    testSerDeserYieldsTrigger(new ProtoTrigger.PartitionTrigger(new DatasetId("test", "myds"), 4),
                              new PartitionTrigger(new DatasetId("test", "myds"), 4));
    testSerDeserYieldsTrigger(new ProtoTrigger.TimeTrigger("* * * * *"),
                              new TimeTrigger("* * * * *"));
    testSerDeserYieldsTrigger(new ProtoTrigger.StreamSizeTrigger(new StreamId("test", "str"), 1000),
                              new StreamSizeTrigger(new StreamId("test", "str"), 1000));
    testSerDeserYieldsTrigger(new ProtoTrigger.ProgramStatusTrigger(new ProgramId("test", "myapp",
                                                                                  ProgramType.FLOW, "myprog"),
                                                                    ImmutableSet.of(ProgramStatus.COMPLETED)),
                              new ProgramStatusTrigger(new ProgramId("test", "myapp",
                                                                     ProgramType.FLOW, "myprog"),
                                                       ImmutableSet.of(ProgramStatus.COMPLETED)));
  }

  private void testSerDeserYieldsTrigger(ProtoTrigger proto, Trigger trigger) {
    String jsonOfTrigger = GSON.toJson(trigger);
    String jsonOfTriggerAsTrigger = GSON.toJson(trigger, Trigger.class);
    String jsonOfProto = GSON.toJson(proto);
    String jsonOfProtoAsTrigger = GSON.toJson(proto, Trigger.class);
    String jsonOfTriggerByProto = GSON_PROTO.toJson(trigger);
    String jsonOfTriggerAsTriggerByProto = GSON_PROTO.toJson(trigger, Trigger.class);
    String jsonOfProtoByProto = GSON_PROTO.toJson(proto);
    String jsonOfProtoAsTriggerByProto = GSON_PROTO.toJson(proto, Trigger.class);

    Assert.assertEquals(jsonOfTrigger, jsonOfTriggerAsTrigger);
    Assert.assertEquals(jsonOfTrigger, jsonOfProto);
    Assert.assertEquals(jsonOfTrigger, jsonOfProtoAsTrigger);
    Assert.assertEquals(jsonOfTrigger, jsonOfTriggerByProto);
    Assert.assertEquals(jsonOfTrigger, jsonOfTriggerAsTriggerByProto);
    Assert.assertEquals(jsonOfTrigger, jsonOfProtoByProto);
    Assert.assertEquals(jsonOfTrigger, jsonOfProtoAsTriggerByProto);

    Trigger deserialized = GSON.fromJson(jsonOfTrigger, Trigger.class);
    Trigger deserializedAsProto = GSON_PROTO.fromJson(jsonOfTrigger, Trigger.class);

    Assert.assertEquals(trigger, deserialized);
    Assert.assertEquals(proto, deserializedAsProto);
  }


  @Test
  public void testObjectContainingTrigger() {
    testContainingTrigger(new ProtoTrigger.PartitionTrigger(new DatasetId("test1", "pdfs1"), 1),
                          new PartitionTrigger(new DatasetId("test1", "pdfs1"), 1));

    testContainingTrigger(new ProtoTrigger.TimeTrigger("* * * 1 1"),
                          new TimeTrigger("* * * 1 1"));

    testContainingTrigger(new ProtoTrigger.StreamSizeTrigger(new StreamId("x", "y"), 1),
                          new StreamSizeTrigger(new StreamId("x", "y"), 1));

    testContainingTrigger(new ProtoTrigger.ProgramStatusTrigger(new ProgramId("test", "myapp",
                                                                              ProgramType.FLOW, "myprog"),
                                                                ImmutableSet.of(ProgramStatus.FAILED)),
                          new ProgramStatusTrigger(new ProgramId("test", "myapp",
                                                   ProgramType.FLOW, "myprog"),
                                                   ImmutableSet.of(ProgramStatus.FAILED)));

  }

  private void testContainingTrigger(ProtoTrigger proto, Trigger trigger) {
    ProgramSchedule proto1 = new ProgramSchedule("sched1", "one partition schedule",
                                                 new NamespaceId("test").app("a").worker("ww"),
                                                 ImmutableMap.of("prop3", "abc"), proto,
                                                 ImmutableList.<Constraint>of());

    ProgramSchedule sched1 = new ProgramSchedule("sched1", "one partition schedule",
                                                 new NamespaceId("test").app("a").worker("ww"),
                                                 ImmutableMap.of("prop3", "abc"), trigger,
                                                 ImmutableList.<Constraint>of());

    Assert.assertEquals(sched1, GSON.fromJson(GSON.toJson(proto1), ProgramSchedule.class));
  }
}
