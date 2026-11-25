/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.logging.meta;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link Checkpoint}.
 */
public class CheckpointTest {

  @Test
  public void testToStringAndGetters() {
    Checkpoint<Long> longCheckpoint = new Checkpoint<>(42L, 1700000000000L);
    Assert.assertEquals(Long.valueOf(42L), longCheckpoint.getOffset());
    Assert.assertEquals(1700000000000L, longCheckpoint.getMaxEventTime());
    Assert.assertEquals("Checkpoint{Offset=42, maxEventTime=1700000000000}",
        longCheckpoint.toString());

    KafkaOffset kafkaOffset = new KafkaOffset(10L, 5000L);
    Checkpoint<KafkaOffset> kafkaCheckpoint = new Checkpoint<>(kafkaOffset, 6000L);
    Assert.assertEquals("Checkpoint{Offset=" + kafkaOffset + ", maxEventTime=6000}",
        kafkaCheckpoint.toString());
  }
}
