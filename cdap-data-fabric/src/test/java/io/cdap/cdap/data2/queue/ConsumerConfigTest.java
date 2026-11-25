/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.data2.queue;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link ConsumerConfig}.
 */
public class ConsumerConfigTest {

  @Test
  public void testToStringEqualsAndHashCode() {
    ConsumerConfig hashConfig1 =
        new ConsumerConfig(7L, 2, 4, DequeueStrategy.HASH, "userId");
    ConsumerGroupConfig groupConfig =
        new ConsumerGroupConfig(7L, 4, DequeueStrategy.HASH, "userId");
    ConsumerConfig hashConfig2 = new ConsumerConfig(groupConfig, 2);
    ConsumerConfig fifoConfig =
        new ConsumerConfig(7L, 0, 4, DequeueStrategy.FIFO, "ignoredKey");

    Assert.assertEquals(
        "ConsumerConfig{groupId=7, instanceId=2, groupSize=4, dequeueStrategy=HASH, hashKey=userId}",
        hashConfig1.toString());
    Assert.assertEquals(
        "ConsumerConfig{groupId=7, instanceId=0, groupSize=4, dequeueStrategy=FIFO, hashKey=null}",
        fifoConfig.toString());

    Assert.assertEquals(hashConfig1, hashConfig2);
    Assert.assertEquals(hashConfig1.hashCode(), hashConfig2.hashCode());
    Assert.assertNotEquals(hashConfig1, fifoConfig);
  }
}
