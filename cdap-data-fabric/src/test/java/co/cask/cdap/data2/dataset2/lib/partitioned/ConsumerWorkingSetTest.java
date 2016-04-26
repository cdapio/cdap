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

import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.partitioned.ConsumerWorkingSet;
import co.cask.cdap.api.dataset.lib.partitioned.DefaultConsumablePartition;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

public class ConsumerWorkingSetTest {

  @Test
  public void testByteSerialization() {
    ConsumerWorkingSet workingSet = new ConsumerWorkingSet();

    // test with empty partitions lists
    testSerDe(workingSet);

    // test with two elements in AVAILABLE and none in IN_PROGRESS
    workingSet.getPartitions().add(new DefaultConsumablePartition(generateUniqueKey()));
    workingSet.getPartitions().add(new DefaultConsumablePartition(generateUniqueKey()));
    testSerDe(workingSet);

    // test with three elements in partitions and none in inProgressPartitions
    workingSet.getPartitions().add(new DefaultConsumablePartition(generateUniqueKey()));
    testSerDe(workingSet);

    // mark the first element as IN_PROGRESS
    workingSet.getPartitions().get(0).take();
    workingSet.getPartitions().get(0).setTimestamp(System.currentTimeMillis());
    testSerDe(workingSet);
  }

  private void testSerDe(ConsumerWorkingSet stateToSerialize) {
    byte[] bytes = stateToSerialize.toBytes();
    // Assert that the serialization format version is 0
    Assert.assertEquals(0, bytes[0]);
    ConsumerWorkingSet deserializedState = ConsumerWorkingSet.fromBytes(bytes);
    Assert.assertEquals(stateToSerialize, deserializedState);
  }

  private PartitionKey generateUniqueKey() {
    return PartitionKey.builder()
      .addIntField("i", 1)
      .addLongField("l", 17L)
      .addStringField("s", UUID.randomUUID().toString())
      .build();
  }
}
