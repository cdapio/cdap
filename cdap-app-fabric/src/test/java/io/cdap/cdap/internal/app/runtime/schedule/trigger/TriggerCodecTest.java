/*
 * Copyright © 2017-2019 Cask Data, Inc.
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
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.ProtoTrigger;
import io.cdap.cdap.proto.ProtoTriggerCodec;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.ProgramReference;
import org.junit.Assert;
import org.junit.Test;

public class TriggerCodecTest {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Trigger.class, new TriggerCodec())
    .registerTypeAdapter(SatisfiableTrigger.class, new TriggerCodec())
    .create();

  private static final Gson GSON_PROTO = new GsonBuilder()
    .registerTypeAdapter(Trigger.class, new ProtoTriggerCodec())
    .registerTypeAdapter(ProtoTrigger.class, new ProtoTriggerCodec())
    .create();

  @Test
  public void testTimeTriggerValidation() {
    // Cron with wrong number of parts
    assertDeserializeFail(new ProtoTrigger.TimeTrigger("* * * ?"), "Cron entry must contain 5 or 6 fields.");
    assertDeserializeFail(new ProtoTrigger.TimeTrigger("* * * * 1 ? *"), "Cron entry must contain 5 or 6 fields.");
    // Quartz doesn't support '?' in both day-of-the-month and day-of-the-week
    assertDeserializeFail(new ProtoTrigger.TimeTrigger("* * * ? 1 ?"),
                          "'?' can only be specfied for Day-of-Month -OR- Day-of-Week.");
    // Quartz doesn't support '0' in day-of-the-week
    assertDeserializeFail(new ProtoTrigger.TimeTrigger("2 6 ? * 0,1,4,5"),
                          "Day-of-Week values must be between 1 and 7");
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

    GSON.toJson(new ProtoTrigger.TimeTrigger("2 6 ? * 1,4,5"), Trigger.class);
    GSON.toJson(new ProtoTrigger.TimeTrigger("* * ? 1 1"), Trigger.class);
    GSON.toJson(new ProtoTrigger.TimeTrigger("* * * 1 ?"), Trigger.class);
    GSON.toJson(new ProtoTrigger.TimeTrigger("* * ? 1 *"), Trigger.class);
    GSON.toJson(new ProtoTrigger.TimeTrigger("* * * ? 1 1"), Trigger.class);
    GSON.toJson(new ProtoTrigger.TimeTrigger("* * * * 1 ?"), Trigger.class);
    GSON.toJson(new ProtoTrigger.TimeTrigger("* * * ? 1 *"), Trigger.class);
  }

  private void assertDeserializeFail(ProtoTrigger.TimeTrigger trigger) {
    assertDeserializeFail(trigger, "");
  }

  private void assertDeserializeFail(ProtoTrigger.TimeTrigger trigger, String errorMessageSubstring) {
    // ProtoTriggerCodec only checks whether TimeTrigger contains null cron entry
    GSON_PROTO.toJson(trigger, ProtoTrigger.class);
    try {
      // TriggerCodec checks whether the cron entry in TimeTrigger can be converted to correct Quartz cron format
      GSON.fromJson(GSON.toJson(trigger, Trigger.class), Trigger.class);
      Assert.fail(String.format("Deserializing invalid trigger %s should fail.", trigger));
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains(errorMessageSubstring));
    }
  }

  @Test
  public void testTriggerCodec() {
    ProtoTrigger.PartitionTrigger protoPartition = new ProtoTrigger.PartitionTrigger(new DatasetId("test", "myds"), 4);
    PartitionTrigger partitionTrigger = new PartitionTrigger(new DatasetId("test", "myds"), 4);
    testSerDeserYieldsTrigger(protoPartition, partitionTrigger);

    ProtoTrigger.TimeTrigger protoTime = new ProtoTrigger.TimeTrigger("* * * * *");
    TimeTrigger timeTrigger = new TimeTrigger("* * * * *");
    testSerDeserYieldsTrigger(protoTime, timeTrigger);

    ProtoTrigger.ProgramStatusTrigger protoProgramStatus =
      new ProtoTrigger.ProgramStatusTrigger(new ProgramReference("test", "myapp", ProgramType.SERVICE, "myprog"),
                                            ImmutableSet.of(ProgramStatus.COMPLETED));
    ProgramStatusTrigger programStatusTrigger =
      new ProgramStatusTrigger(new ProgramReference("test", "myapp", ProgramType.SERVICE, "myprog"),
                             ImmutableSet.of(ProgramStatus.COMPLETED));
    testSerDeserYieldsTrigger(protoProgramStatus, programStatusTrigger);

    ProtoTrigger.OrTrigger protoOr = ProtoTrigger.or(protoPartition, ProtoTrigger.and(protoTime, protoProgramStatus));
    OrTrigger orTrigger =
      new OrTrigger(partitionTrigger, new AndTrigger(timeTrigger, programStatusTrigger));
    testSerDeserYieldsTrigger(protoOr, orTrigger);

    ProtoTrigger.AndTrigger protoAnd =
      ProtoTrigger.and(protoOr, protoTime, ProtoTrigger.or(protoPartition, protoProgramStatus));
    AndTrigger andTrigger =
      new AndTrigger(orTrigger, timeTrigger, new OrTrigger(partitionTrigger, programStatusTrigger));
    testSerDeserYieldsTrigger(protoAnd, andTrigger);
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

    testContainingTrigger(new ProtoTrigger.ProgramStatusTrigger(new ProgramReference("test", "myapp",
                                                                              ProgramType.SERVICE, "myprog"),
                                                                ImmutableSet.of(ProgramStatus.FAILED)),
                          new ProgramStatusTrigger(new ProgramReference("test", "myapp",
                                                   ProgramType.SERVICE, "myprog"),
                                                   ImmutableSet.of(ProgramStatus.FAILED)));

  }

  private void testContainingTrigger(ProtoTrigger proto, Trigger trigger) {
    ProgramSchedule proto1 = new ProgramSchedule("sched1", "one partition schedule",
                                                 new ProgramReference("test", "a", ProgramType.WORKER, "ww"),
                                                 ImmutableMap.of("prop3", "abc"), proto,
                                                 ImmutableList.of());

    ProgramSchedule sched1 = new ProgramSchedule("sched1", "one partition schedule",
                                                 new ProgramReference("test", "a", ProgramType.WORKER, "ww"),
                                                 ImmutableMap.of("prop3", "abc"), trigger,
                                                 ImmutableList.of());

    Assert.assertEquals(sched1, GSON.fromJson(GSON.toJson(proto1), ProgramSchedule.class));
  }
}
