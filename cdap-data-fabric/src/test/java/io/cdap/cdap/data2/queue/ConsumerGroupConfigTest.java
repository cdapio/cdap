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
 * Unit tests for {@link ConsumerGroupConfig}.
 */
public class ConsumerGroupConfigTest {

  @Test
  public void testToStringCopyConstructorEqualsAndHashCode() {
    ConsumerGroupConfig hashGroup =
        new ConsumerGroupConfig(11L, 8, DequeueStrategy.HASH, "partitionKey");
    ConsumerGroupConfig hashGroupCopy = new ConsumerGroupConfig(hashGroup);
    ConsumerGroupConfig roundRobinGroup =
        new ConsumerGroupConfig(11L, 8, DequeueStrategy.ROUND_ROBIN, "ignored");

    Assert.assertEquals(
        "ConsumerGroupConfig{groupId=11, groupSize=8, dequeueStrategy=HASH, hashKey=partitionKey}",
        hashGroup.toString());
    Assert.assertEquals(
        "ConsumerGroupConfig{groupId=11, groupSize=8, dequeueStrategy=ROUND_ROBIN, hashKey=null}",
        roundRobinGroup.toString());

    Assert.assertEquals(hashGroup, hashGroupCopy);
    Assert.assertEquals(hashGroup.hashCode(), hashGroupCopy.hashCode());
    Assert.assertNotEquals(hashGroup, roundRobinGroup);
  }
}
