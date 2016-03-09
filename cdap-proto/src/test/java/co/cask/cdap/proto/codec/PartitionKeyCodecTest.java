/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.proto.codec;

import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.partitioned.PartitionKeyCodec;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link PartitionKeyCodec}.
 */
public class PartitionKeyCodecTest {

  @Test
  public void testSerDe() {
    PartitionKey key = PartitionKey.builder()
      .addField("a", "value,")
      .addField("b", 1L)
      .addField("c", -17)
      .addField("d", true)
      .addIntField("e", 42)
      .addLongField("f", 15)
      .addStringField("g", "value]}")
      .build();

    Gson gson = new GsonBuilder().registerTypeAdapter(PartitionKey.class, new PartitionKeyCodec()).create();
    String serialized = gson.toJson(key);
    Assert.assertEquals(key, gson.fromJson(serialized, PartitionKey.class));
  }
}
