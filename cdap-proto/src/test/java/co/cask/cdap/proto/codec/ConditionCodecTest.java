/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.lib.ConditionCodec;
import co.cask.cdap.api.dataset.lib.PartitionFilter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link ConditionCodec}.
 */
public class ConditionCodecTest {
  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(PartitionFilter.Condition.class, new ConditionCodec()).create();

  @Test
  public void testSerDe() {
    PartitionFilter filter = PartitionFilter.builder()
      .addValueCondition("i", 42)
      .addValueCondition("l", 17L)
      .addValueCondition("s", "x")
      .build();
    testSerDe(filter);

    filter = PartitionFilter.builder()
      .addRangeCondition("i", 30, 40)
      .addValueCondition("l", 17L)
      .addValueCondition("s", "x")
      .build();
    testSerDe(filter);


    filter = PartitionFilter.builder()
      .addRangeCondition("i", 30, 40)
      .addValueCondition("s", "x")
      .build();
    testSerDe(filter);

    testSerDe(PartitionFilter.ALWAYS_MATCH);
  }

  private void testSerDe(PartitionFilter filter) {
    String serialized = GSON.toJson(filter);
    Assert.assertEquals(filter, GSON.fromJson(serialized, PartitionFilter.class));
  }
}
