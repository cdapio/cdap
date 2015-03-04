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

import co.cask.cdap.api.dataset.lib.PartitionFilter;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.data2.dataset2.AbstractDatasetTest;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.SlowTests;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Test partitioned file sets without map/reduce and without explore.
 */
public class PartitionedFileSetTest extends AbstractDatasetTest {

  static final Logger LOG = org.slf4j.LoggerFactory.getLogger(PartitionedFileSetTest.class);

  static final Partitioning PARTITIONING_1 = Partitioning.builder()
    .addStringField("s")
    .addIntField("i")
    .addLongField("l")
    .build();
  static final Partitioning PARTITIONING_2 = Partitioning.builder()
    .addStringField("s")
    .addIntField("i")
    .addLongField("l")
    .addStringField("x")
    .build();

  private static final Id.DatasetInstance pfsInstance =
    DS_NAMESPACE.namespace(Id.DatasetInstance.from(NAMESPACE_ID, "pfs"));

  @Before
  public void before() throws Exception {
    createInstance("partitionedFileSet", pfsInstance, PartitionedFileSetProperties.builder()
      .setPartitioning(PARTITIONING_1)
      .setBasePath("testDir")
      .build());
  }

  @After
  public void after() throws Exception {
    deleteInstance(pfsInstance);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEncodeIncompleteKey() {
    PartitionKey key = PartitionKey.builder()
      .addIntField("i", 42)
      .addStringField("s", "x")
      .build();
    PartitionedFileSetDataset.generateRowKey(key, PARTITIONING_1);
  }

  @Test
  public void testEncodeDecode() {
    // key can be in any order... partitioning dictates the order of fields in row key
    PartitionKey key = PartitionKey.builder()
      .addIntField("i", 42)
      .addLongField("l", 17L)
      .addStringField("s", "x")
      .build();
    byte[] rowKey = PartitionedFileSetDataset.generateRowKey(key, PARTITIONING_1);
    PartitionKey decoded = PartitionedFileSetDataset.parseRowKey(rowKey, PARTITIONING_1);
    Assert.assertEquals(key, decoded);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDecodeIncomplete() {
    // key can be in any order... partitioning dictates the order of fields in row key
    PartitionKey key = PartitionKey.builder()
      .addIntField("i", 42)
      .addLongField("l", 17L)
      .addStringField("s", "x")
      .build();
    byte[] rowKey = PartitionedFileSetDataset.generateRowKey(key, PARTITIONING_1);
    PartitionedFileSetDataset.parseRowKey(rowKey, PARTITIONING_2);
  }

  @Test
  @Category(SlowTests.class)
  public void testAddRemoveGetPartitions() throws Exception {

    final PartitionedFileSet dataset = getInstance(pfsInstance);

    final PartitionKey[][][] keys = new PartitionKey[4][4][4];
    final String[][][] paths = new String[4][4][4];
    final Map<PartitionKey, String> allPartitions = Maps.newHashMap();

    // add a bunch of partitions
    for (int s = 0; s < 4; s++) {
      for (int i = 0; i < 4; i++) {
        for (int l = 0; l < 4; l++) {
          final PartitionKey key = PartitionKey.builder()
            .addField("s", String.format("%c-%d", 'a' + s, s))
            .addField("i", i * 100)
            .addField("l", 15L - 10 * l)
            .build();
          final String path = String.format("%s-%d-%d", s, i, l);
          newTransactionExecutor((TransactionAware) dataset).execute(new TransactionExecutor.Subroutine() {
            @Override
            public void apply() throws Exception {
              dataset.addPartition(key, path);
            }
          });
          keys[s][i][l] = key;
          paths[s][i][l] = path;
          allPartitions.put(key, path);
        }
      }
    }

    // validate getPartition with exact partition key
    for (int s = 0; s < 4; s++) {
      for (int i = 0; i < 4; i++) {
        for (int l = 0; l < 4; l++) {
          final PartitionKey key = keys[s][i][l];
          final String path = paths[s][i][l];
          newTransactionExecutor((TransactionAware) dataset).execute(new TransactionExecutor.Subroutine() {
            @Override
            public void apply() throws Exception {
              Assert.assertEquals(path, dataset.getPartition(key));
            }
          });
          // also test getPartitionPaths() and getPartitions() for the filter matching this
          @SuppressWarnings({"unchecked", "unused"})
          boolean success = testFilter(dataset, allPartitions,
                                       PartitionFilter.builder()
                                         .addValueCondition("l", key.getField("l"))
                                         .addValueCondition("s", key.getField("s"))
                                         .addValueCondition("i", key.getField("i"))
                                         .build());
        }
      }
    }

    // test whether query works without filter
    testFilter(dataset, allPartitions, null);

    // generate an list of partition filters with exhaustive coverage
    List<PartitionFilter> filters = generateFilters();

    // test all kinds of filters
    testAllFilters(dataset, allPartitions, filters);

    // remove a few of the partitions and test again, repeatedly
    PartitionKey[] keysToRemove = { keys[1][2][3], keys[0][1][0], keys[2][3][2], keys[3][1][2] };
    for (PartitionKey key : keysToRemove) {

      // remove in a transaction
      newTransactionExecutor((TransactionAware) dataset).execute(new TransactionExecutor.Procedure<PartitionKey>() {
        @Override
        public void apply(PartitionKey partitionKey) throws Exception {
          dataset.dropPartition(partitionKey);
        }
      }, key);

      // test all filters
      allPartitions.remove(key);
      testAllFilters(dataset, allPartitions, filters);
    }

  }

  private void testAllFilters(PartitionedFileSet dataset,
                              Map<PartitionKey, String> allPartitions,
                              List<PartitionFilter> filters) throws Exception {
    for (PartitionFilter filter : filters) {
      try {
        testFilter(dataset, allPartitions, filter);
      } catch (Throwable e) {
        throw new Exception("testFilter() failed for filter: " + filter, e);
      }
    }
  }

  private boolean testFilter(final PartitionedFileSet dataset,
                             Map<PartitionKey, String> allPartitions,
                             final PartitionFilter filter) throws Exception {

    // determine the keys and paths that match the filter
    final Map<PartitionKey, String> matching = filter == null ? allPartitions :
      Maps.filterEntries(allPartitions, new Predicate<Map.Entry<PartitionKey, String>>() {
        @Override
        public boolean apply(Map.Entry<PartitionKey, String> entry) {
          return filter.match(entry.getKey());
        }
      });
    final Set<String> matchingPaths = Sets.newHashSet(matching.values());

    newTransactionExecutor((TransactionAware) dataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals(matchingPaths, dataset.getPartitionPaths(filter));
        Assert.assertEquals(matching, dataset.getPartitions(filter));
      }
    });

    return true;
  }

  public static List<PartitionFilter> generateFilters() {
    List<PartitionFilter> filters = Lists.newArrayList();
    addSingleConditionFilters(filters, "s", S_CONDITIONS);
    addSingleConditionFilters(filters, "i", I_CONDITIONS);
    addSingleConditionFilters(filters, "l", L_CONDITIONS);
    addTwoConditionFilters(filters, "s", S_CONDITIONS, "i", I_CONDITIONS);
    addTwoConditionFilters(filters, "s", S_CONDITIONS, "l", L_CONDITIONS);
    addTwoConditionFilters(filters, "i", I_CONDITIONS, "l", L_CONDITIONS);
    addThreeConditionFilters(filters, "s", S_CONDITIONS, "i", I_CONDITIONS, "l", L_CONDITIONS);
    LOG.info("Generated " + filters.size() + " filters.");
    return filters;
  }

  private static <T extends Comparable<T>>
  void addSingleConditionFilters(List<PartitionFilter> filters,
                                 String field, T[][] conditions) {
    for (T[] condition : conditions) {
      filters.add(addCondition(PartitionFilter.builder(), field, condition).build());
    }
  }

  private static <T1 extends Comparable<T1>, T2 extends Comparable<T2>>
  void addTwoConditionFilters(List<PartitionFilter> filters,
                              String field1, T1[][] conditions1,
                              String field2, T2[][] conditions2) {
    for (T1[] cond1 : conditions1) {
      for (T2[] cond2 : conditions2) {
        filters.add(addCondition(addCondition(PartitionFilter.builder(), field1, cond1), field2, cond2).build());
      }
    }
  }

  private static <T1 extends Comparable<T1>, T2 extends Comparable<T2>, T3 extends Comparable<T3>>
  void addThreeConditionFilters(List<PartitionFilter> filters,
                                String field1, T1[][] conditions1,
                                String field2, T2[][] conditions2,
                                String field3, T3[][] conditions3) {
    for (T1[] cond1 : conditions1) {
      for (T2[] cond2 : conditions2) {
        for (T3[] cond3 : conditions3) {
          filters.add(addCondition(addCondition(addCondition(
            PartitionFilter.builder(), field1, cond1), field2, cond2), field3, cond3).build());
        }
      }
    }
  }

  private static <T extends Comparable<T>>
  PartitionFilter.Builder addCondition(PartitionFilter.Builder builder, String field, T[] condition) {
    return condition.length == 1
      ? builder.addValueCondition(field, condition[0])
      : builder.addRangeCondition(field, condition[0], condition[1]);
  }

  private static final String[][] S_CONDITIONS = {
    { "", "zzz" }, // match all
    { "b", "d" }, // matches ony s=1,2
    { "a-0", "b-1" }, // matches ony s=0
    { null, "b-1" }, // matches ony s=0
    { "c", null }, // matches only s=2,3
    { "c", "x" }, // matches only s=2,3
    { "a-1", "b-0" }, // matches none
    { "a-1" }, // matches none
    { "" },  // matches none
    { "f" },  // matches none
    { "a-0" }, // matches s=0
    { "d-3" }, // matches s=3
   };

  private static final Integer[][] I_CONDITIONS = {
    { 0, 501 }, // matches all
    { null, 200 }, // matches only i=0,1
    { -100, 200 }, // matches only i=0,1
    { 0, 101 }, // matches only i=0,1
    { 199, null }, // matches only i=2,3
    { 50, 300 }, // matches only i=1,2
    { 0 }, // matches only i=0
    { 200 }, // matches only i=2
    { null, 0 }, // matches none
    { 50, 60 }, // matches none
    { 404 } // matches none
  };

  private static final Long[][] L_CONDITIONS = {
    { Long.MIN_VALUE, Long.MAX_VALUE }, // matches all
    { -50L, 50L }, // matches all
    { null, -4L }, // matches only j=0,1
    { -100L, 5L }, // matches only j=0,1
    { -15L, 100L }, // matches only j=0,1
    { 0L, Long.MAX_VALUE }, // matches only j=2,3
    { 5L, 16L }, // matches only j=2,3
    { -5L, 6L }, // matches only j=1,2
    { -15L }, // matches only l=3
    { 5L }, // matches only l=1
    { null, Long.MIN_VALUE }, // matches none
    { Long.MIN_VALUE, -15L }, // matches none
    { 2L, 3L }, // matches none
    { Long.MAX_VALUE }, // matches none
  };
}
