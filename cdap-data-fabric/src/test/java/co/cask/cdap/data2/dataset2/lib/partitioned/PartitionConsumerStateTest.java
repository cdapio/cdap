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

package co.cask.cdap.data2.dataset2.lib.partitioned;

import co.cask.cdap.api.dataset.lib.PartitionConsumerState;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class PartitionConsumerStateTest {

  @Test
  public void testByteSerialization() {
    testSerDe(new PartitionConsumerState(2L, Lists.newArrayList(1L, 2L, 3L)));
    testSerDe(new PartitionConsumerState(0L, Lists.newArrayList(3L, 5L, 100L, 61L, 12L)));
    testSerDe(new PartitionConsumerState(Long.MAX_VALUE, Lists.<Long>newArrayList()));
  }

  private void testSerDe(PartitionConsumerState stateToSerialize) {
    byte[] bytes = stateToSerialize.toBytes();
    // Assert that the serialization format version is 0
    Assert.assertEquals(0, bytes[0]);
    PartitionConsumerState deserializedState = PartitionConsumerState.fromBytes(bytes);
    Assert.assertEquals(stateToSerialize, deserializedState);
  }
}
