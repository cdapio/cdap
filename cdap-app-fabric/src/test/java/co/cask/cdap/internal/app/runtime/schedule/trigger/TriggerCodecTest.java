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

import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.internal.schedule.trigger.Trigger;
import co.cask.cdap.proto.ProtoTrigger;
import co.cask.cdap.proto.ProtoTriggerCodec;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
  public void testTriggerCodec() {
    testSerDeserYieldsTrigger(new ProtoTrigger.PartitionTrigger(new DatasetId("test", "myds"), 4),
                              new PartitionTrigger(new DatasetId("test", "myds"), 4));
    testSerDeserYieldsTrigger(new ProtoTrigger.TimeTrigger("* * * * *"),
                              new TimeTrigger("* * * * *"));
    testSerDeserYieldsTrigger(new ProtoTrigger.StreamSizeTrigger(new StreamId("test", "str"), 1000),
                              new StreamSizeTrigger(new StreamId("test", "str"), 1000));
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
    ProgramSchedule proto1 = new ProgramSchedule("sched1", "one partition schedule",
                                                 new NamespaceId("test").app("a").worker("ww"),
                                                 ImmutableMap.of("prop3", "abc"),
                                                 new ProtoTrigger.PartitionTrigger(new DatasetId("test1", "pdfs1"), 1),
                                                 ImmutableList.<Constraint>of());
    ProgramSchedule sched1 = new ProgramSchedule("sched1", "one partition schedule",
                                                 new NamespaceId("test").app("a").worker("ww"),
                                                 ImmutableMap.of("prop3", "abc"),
                                                 new PartitionTrigger(new DatasetId("test1", "pdfs1"), 1),
                                                 ImmutableList.<Constraint>of());
    Assert.assertEquals(sched1, GSON.fromJson(GSON.toJson(proto1), ProgramSchedule.class));


    ProgramSchedule proto2 = new ProgramSchedule("schedone", "one time schedule",
                                                 new NamespaceId("test3").app("abc").workflow("wf112"),
                                                 ImmutableMap.of("prop", "all"),
                                                 new ProtoTrigger.TimeTrigger("* * * 1 1"),
                                                 ImmutableList.<Constraint>of());
    ProgramSchedule sched2 = new ProgramSchedule("schedone", "one time schedule",
                                                 new NamespaceId("test3").app("abc").workflow("wf112"),
                                                 ImmutableMap.of("prop", "all"),
                                                 new TimeTrigger("* * * 1 1"),
                                                 ImmutableList.<Constraint>of());
    Assert.assertEquals(sched2, GSON.fromJson(GSON.toJson(proto2), ProgramSchedule.class));

    ProgramSchedule proto3 = new ProgramSchedule("sched3", "one MB schedule",
                                                 new NamespaceId("test3").app("abc").workflow("wf112"),
                                                 ImmutableMap.of("prop", "all"),
                                                 new ProtoTrigger.StreamSizeTrigger(new StreamId("x", "y"), 1),
                                                 ImmutableList.<Constraint>of());
    ProgramSchedule sched3 = new ProgramSchedule("sched3", "one MB schedule",
                                                 new NamespaceId("test3").app("abc").workflow("wf112"),
                                                 ImmutableMap.of("prop", "all"),
                                                 new StreamSizeTrigger(new StreamId("x", "y"), 1),
                                                 ImmutableList.<Constraint>of());
    Assert.assertEquals(sched3, GSON.fromJson(GSON.toJson(proto3), ProgramSchedule.class));
  }

}
