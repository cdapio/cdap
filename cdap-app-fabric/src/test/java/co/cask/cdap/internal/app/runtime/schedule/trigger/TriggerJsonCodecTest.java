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
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

public class TriggerJsonCodecTest {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Trigger.class, new TriggerJsonCodec())
    .create();


  @Test
  public void testTriggerCodec() {

    Trigger trigger = new PartitionTrigger(new DatasetId("test", "myds"), 4);
    String json = GSON.toJson(trigger, Trigger.class);
    Trigger trigger1 = GSON.fromJson(json, Trigger.class);
    Assert.assertEquals(trigger, trigger1);

    trigger = new TimeTrigger("* * * * *");
    json = GSON.toJson(trigger, Trigger.class);
    trigger1 = GSON.fromJson(json, Trigger.class);
    Assert.assertEquals(trigger, trigger1);
  }

  @Test
  public void testObjectContainingTrigger() {
    ProgramSchedule sched1 = new ProgramSchedule("sched1", "one partition schedule",
                                                 new NamespaceId("test").app("a").worker("ww"),
                                                 ImmutableMap.of("prop3", "abc"),
                                                 new PartitionTrigger(new DatasetId("test1", "pdfs1"), 1),
                                                 ImmutableList.<Constraint>of());
    ProgramSchedule sched2 = new ProgramSchedule("schedone", "one time schedule",
                                                 new NamespaceId("test3").app("abc").workflow("wf112"),
                                                 ImmutableMap.of("prop", "all"),
                                                 new TimeTrigger("* * * 1 1"),
                                                 ImmutableList.<Constraint>of());
    Assert.assertEquals(sched1, GSON.fromJson(GSON.toJson(sched1), ProgramSchedule.class));
    Assert.assertEquals(sched2, GSON.fromJson(GSON.toJson(sched2), ProgramSchedule.class));
  }

}
