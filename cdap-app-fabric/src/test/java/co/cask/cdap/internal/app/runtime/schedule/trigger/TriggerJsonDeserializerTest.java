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

import co.cask.cdap.proto.id.DatasetId;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

public class TriggerJsonDeserializerTest {

  @Test
  public void testTriggerCodec() {

    Gson gson = new GsonBuilder()
      .registerTypeAdapter(Trigger.class, new TriggerJsonDeserializer())
      .create();

    Trigger trigger = new PartitionTrigger(new DatasetId("test", "myds"), 4);
    String json = gson.toJson(trigger);
    Trigger trigger1 = gson.fromJson(json, Trigger.class);
    Assert.assertEquals(trigger, trigger1);

    trigger = new TimeTrigger("* * * * *");
    json = gson.toJson(trigger);
    trigger1 = gson.fromJson(json, Trigger.class);
    Assert.assertEquals(trigger, trigger1);
  }
}
