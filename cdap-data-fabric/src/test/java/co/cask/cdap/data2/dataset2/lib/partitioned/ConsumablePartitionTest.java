/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.lib.partitioned.ConsumablePartition;
import co.cask.cdap.api.dataset.lib.partitioned.DefaultConsumablePartition;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

/**
 * Tests for ConsumablePartition.
 */
public class ConsumablePartitionTest {

  @Test
  public void testSimpleTransitions() {
    // tests simple success case
    ConsumablePartition partition = new DefaultConsumablePartition(generateUniqueKey());

    Assert.assertEquals(0, partition.getNumFailures());
    partition.take();
    partition.retry();
    Assert.assertEquals(1, partition.getNumFailures());
    partition.take();
    // test that untake doesn't increment failure count
    partition.untake();
    Assert.assertEquals(1, partition.getNumFailures());
    partition.take();
    partition.complete();
  }

  @Test(expected = IllegalStateException.class)
  public void testAlreadyTakenTransition() {
    // cannot take a partition that's already taken
    ConsumablePartition partition = new DefaultConsumablePartition(generateUniqueKey());

    partition.take();
    partition.take();
  }

  @Test(expected = IllegalStateException.class)
  public void testRetryWithoutTakenTransition() {
    // cannot take retry a partition without it being taken first
    ConsumablePartition partition = new DefaultConsumablePartition(generateUniqueKey());

    partition.retry();
  }

  @Test(expected = IllegalStateException.class)
  public void testAlreadyCompletedTransition() {
    // cannot complete a partition that has already been completed
    ConsumablePartition partition = new DefaultConsumablePartition(generateUniqueKey());

    partition.take();
    partition.complete();
    partition.complete();
  }

  int counter;
  // generates unique partition keys, where the 'i' field is incrementing from 0 upwards on each returned key
  private PartitionKey generateUniqueKey() {
    return PartitionKey.builder()
      .addIntField("i", counter++)
      .addLongField("l", 17L)
      .addStringField("s", UUID.randomUUID().toString())
      .build();
  }
}
