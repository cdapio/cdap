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

package io.cdap.cdap.common.zookeeper.coordination;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link PartitionReplica}.
 */
public class PartitionReplicaTest {

  @Test
  public void testToStringEqualsAndComparator() {
    PartitionReplica r0 = new PartitionReplica("part-0", 0);
    PartitionReplica r1 = new PartitionReplica("part-0", 1);
    PartitionReplica p1r0 = new PartitionReplica("part-1", 0);

    Assert.assertEquals("part-0", r0.getName());
    Assert.assertEquals(0, r0.getReplicaId());
    Assert.assertEquals("PartitionReplica{partition=part-0, replica=0}", r0.toString());
    Assert.assertEquals("PartitionReplica{partition=part-0, replica=1}", r1.toString());

    Assert.assertEquals(r0, new PartitionReplica("part-0", 0));
    Assert.assertEquals(r0.hashCode(), new PartitionReplica("part-0", 0).hashCode());
    Assert.assertNotEquals(r0, r1);
    Assert.assertNotEquals(r0, p1r0);

    Assert.assertTrue(PartitionReplica.COMPARATOR.compare(r0, r1) < 0);
    Assert.assertTrue(PartitionReplica.COMPARATOR.compare(r1, p1r0) < 0);
    Assert.assertEquals(0,
        PartitionReplica.COMPARATOR.compare(r0, new PartitionReplica("part-0", 0)));
  }
}
