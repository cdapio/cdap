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
 * Unit tests for {@link ResourceRequirement}.
 */
public class ResourceRequirementTest {

  @Test
  public void testBuilderPartitionAndExactToString() {
    ResourceRequirement.Partition p0 = new ResourceRequirement.Partition("p0", 2);
    Assert.assertEquals("p0", p0.getName());
    Assert.assertEquals(2, p0.getReplicas());
    Assert.assertEquals("Partition{name=p0, replicas=2}", p0.toString());

    ResourceRequirement requirement = ResourceRequirement.builder("my-service")
        .addPartitions("p", 2, 2)
        .build();
    Assert.assertEquals("my-service", requirement.getName());
    Assert.assertEquals(2, requirement.getPartitions().size());
    Assert.assertEquals(
        "ResourceRequirement{name=my-service, "
            + "partitions=[Partition{name=p0, replicas=2}, Partition{name=p1, replicas=2}]}",
        requirement.toString());

    ResourceRequirement same = ResourceRequirement.builder("my-service")
        .addPartitions("p", 2, 2)
        .build();
    Assert.assertEquals(requirement, same);
    Assert.assertEquals(requirement.hashCode(), same.hashCode());

    try {
      ResourceRequirement.builder("dup")
          .addPartition(p0)
          .addPartition(new ResourceRequirement.Partition("p0", 3));
      Assert.fail("Expected IllegalArgumentException for duplicate partition");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }
}
