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

package io.cdap.cdap.data2.dataset2.lib.partitioned;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.dataset.lib.DynamicPartitioner;
import io.cdap.cdap.api.dataset.lib.Partition;
import io.cdap.cdap.api.dataset.lib.PartitionFilter;
import io.cdap.cdap.api.dataset.lib.PartitionKey;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSetArguments;
import io.cdap.cdap.api.dataset.lib.Partitioning;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Tests for {@link PartitionedFileSetArguments}.
 */
public class PartitionedFileSetArgumentsTest {

  private static final Partitioning PARTITIONING = Partitioning.builder()
    .addStringField("s")
    .addIntField("i")
    .addLongField("l")
    .build();

  @Test
  public void testSetGetOutputPartitionKey() throws Exception {
    Map<String, String> arguments = new HashMap<>();
    PartitionKey key = PartitionKey.builder()
      .addIntField("i", 42)
      .addLongField("l", 17L)
      .addStringField("s", "x")
      .build();
    PartitionedFileSetArguments.setOutputPartitionKey(arguments, key);
    Assert.assertEquals(key, PartitionedFileSetArguments.getOutputPartitionKey(arguments, PARTITIONING));
  }

  @Test
  public void testSetGetOutputPartitionMetadata() throws Exception {
    Map<String, String> arguments = new HashMap<>();
    Map<String, String> metadata = ImmutableMap.of("metakey1", "value1",
                                                   "metaKey2", "value3");
    PartitionedFileSetArguments.setOutputPartitionMetadata(arguments, metadata);
    Assert.assertEquals(metadata, PartitionedFileSetArguments.getOutputPartitionMetadata(arguments));

    // test also with empty metadata
    arguments.clear();
    PartitionedFileSetArguments.setOutputPartitionMetadata(arguments, Collections.<String, String>emptyMap());
    Assert.assertEquals(Collections.<String, String>emptyMap(),
                        PartitionedFileSetArguments.getOutputPartitionMetadata(arguments));
  }

  @Test
  public void testSetGetInputPartitionFilter() throws Exception {
    Map<String, String> arguments = new HashMap<>();
    PartitionFilter filter = PartitionFilter.builder()
      .addRangeCondition("i", 30, 40)
      .addValueCondition("l", 17L)
      .addValueCondition("s", "x")
      .build();
    PartitionedFileSetArguments.setInputPartitionFilter(arguments, filter);
    Assert.assertEquals(filter, PartitionedFileSetArguments.getInputPartitionFilter(arguments));


    arguments = new HashMap<>();
    filter = PartitionFilter.builder()
      .addRangeCondition("i", 30, 40)
      .addValueCondition("s", "x")
      .build();
    PartitionedFileSetArguments.setInputPartitionFilter(arguments, filter);
    Assert.assertEquals(filter, PartitionedFileSetArguments.getInputPartitionFilter(arguments));

    arguments = new HashMap<>();
    filter = PartitionFilter.ALWAYS_MATCH;
    PartitionedFileSetArguments.setInputPartitionFilter(arguments, filter);
    Assert.assertEquals(filter, PartitionedFileSetArguments.getInputPartitionFilter(arguments));
  }


  @Test
  public void testGetInputPartitionKeys() throws Exception {
    Map<String, String> arguments = new HashMap<>();
    Assert.assertEquals(0, PartitionedFileSetArguments.getInputPartitionKeys(arguments).size());

    List<? extends Partition> partitions =
      Lists.newArrayList(new BasicPartition(null, "path/doesn't/matter/1", generateUniqueKey()),
                         new BasicPartition(null, "path/doesn't/matter/2", generateUniqueKey()),
                         new BasicPartition(null, "path/doesn't/matter/3", generateUniqueKey()));

    for (Partition partition : partitions) {
      PartitionedFileSetArguments.addInputPartition(arguments, partition);
    }

    List<PartitionKey> inputPartitionKeys = Lists.transform(partitions, new Function<Partition, PartitionKey>() {
      @Nullable
      @Override
      public PartitionKey apply(Partition input) {
        return input.getPartitionKey();
      }
    });

    Assert.assertEquals(inputPartitionKeys, PartitionedFileSetArguments.getInputPartitionKeys(arguments));

    arguments.clear();
    PartitionedFileSetArguments.addInputPartitions(arguments, partitions.iterator());
    Assert.assertEquals(inputPartitionKeys, PartitionedFileSetArguments.getInputPartitionKeys(arguments));
  }

  private int counter;
  // generates unique partition keys, where the 'i' field is incrementing from 0 upwards on each returned key
  private PartitionKey generateUniqueKey() {
    return PartitionKey.builder()
      .addIntField("i", counter++)
      .addLongField("l", 17L)
      .addStringField("s", UUID.randomUUID().toString())
      .build();
  }

  @Test
  public void testGetDynamicPartitionerClass() throws Exception {
    Map<String, String> arguments = new HashMap<>();

    // two ways to set the DynamicPartitioner class - either the class object or the String (name)
    PartitionedFileSetArguments.setDynamicPartitioner(arguments, TestDynamicPartitioner.class);
    Assert.assertEquals(TestDynamicPartitioner.class.getName(),
                        PartitionedFileSetArguments.getDynamicPartitioner(arguments));

    arguments.clear();
    PartitionedFileSetArguments.setDynamicPartitioner(arguments, TestDynamicPartitioner.class.getName());
    Assert.assertEquals(TestDynamicPartitioner.class.getName(),
                        PartitionedFileSetArguments.getDynamicPartitioner(arguments));

  }

  @Test
  public void testDynamicPartitionerWriterConcurrency() {
    Map<String, String> arguments = new HashMap<>();

    // should not be able to get or set the concurrency setting, without a dynamic partitioner set on the arguments
    try {
      PartitionedFileSetArguments.isDynamicPartitionerConcurrencyAllowed(arguments);
      Assert.fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }
    try {
      PartitionedFileSetArguments.setDynamicPartitionerConcurrency(arguments, false);
      Assert.fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }

    // set a DynamicPartitioner
    PartitionedFileSetArguments.setDynamicPartitioner(arguments, TestDynamicPartitioner.class.getName());
    // default value should be true
    Assert.assertTrue(PartitionedFileSetArguments.isDynamicPartitionerConcurrencyAllowed(arguments));

    // try set+get
    PartitionedFileSetArguments.setDynamicPartitionerConcurrency(arguments, false);
    Assert.assertFalse(PartitionedFileSetArguments.isDynamicPartitionerConcurrencyAllowed(arguments));

    PartitionedFileSetArguments.setDynamicPartitionerConcurrency(arguments, true);
    Assert.assertTrue(PartitionedFileSetArguments.isDynamicPartitionerConcurrencyAllowed(arguments));
  }

  private static final class TestDynamicPartitioner extends DynamicPartitioner<Integer, Integer> {
    @Override
    public PartitionKey getPartitionKey(Integer key, Integer value) {
      // implementation doesn't matter, since the object isn't instantiated. Just its class is used in a test case.
      return null;
    }
  }
}
