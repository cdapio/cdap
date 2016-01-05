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

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.PartitionNotFoundException;
import co.cask.cdap.api.dataset.lib.Partition;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionFilter;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionOutput;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.SlowTests;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.inmemory.InMemoryTxSystemClient;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.twill.filesystem.Location;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Test partitioned file sets without map/reduce and without explore.
 */
public class PartitionedFileSetTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();
  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(PartitionedFileSetTest.class);

  private static final Partitioning PARTITIONING_1 = Partitioning.builder()
    .addStringField("s")
    .addIntField("i")
    .addLongField("l")
    .build();
  private static final Partitioning PARTITIONING_2 = Partitioning.builder()
    .addStringField("s")
    .addIntField("i")
    .addLongField("l")
    .addStringField("x")
    .build();

  // key can be in any order... partitioning dictates the order of fields in row key
  private static final PartitionKey PARTITION_KEY = PartitionKey.builder()
    .addIntField("i", 42)
    .addLongField("l", 17L)
    .addStringField("s", "x")
    .build();

  private static final Id.DatasetInstance pfsInstance =
    Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, "pfs");
  private static final Id.DatasetInstance pfsExternalInstance =
    Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, "ext");
  private static Location pfsBaseLocation;

  @Before
  public void before() throws Exception {
    dsFrameworkUtil.createInstance("partitionedFileSet", pfsInstance, PartitionedFileSetProperties.builder()
      .setPartitioning(PARTITIONING_1)
      .setBasePath("testDir")
      .build());
    pfsBaseLocation = ((PartitionedFileSet) dsFrameworkUtil.getInstance(pfsInstance))
      .getEmbeddedFileSet().getBaseLocation();
    Assert.assertTrue(pfsBaseLocation.exists());
  }

  @After
  public void after() throws Exception {
    if (dsFrameworkUtil.getInstance(pfsInstance) != null) {
      dsFrameworkUtil.deleteInstance(pfsInstance);
    }
    if (dsFrameworkUtil.getInstance(pfsExternalInstance) != null) {
      dsFrameworkUtil.deleteInstance(pfsExternalInstance);
    }
    Assert.assertFalse(pfsBaseLocation.exists());
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
    byte[] rowKey = PartitionedFileSetDataset.generateRowKey(PARTITION_KEY, PARTITIONING_1);
    PartitionKey decoded = PartitionedFileSetDataset.parseRowKey(rowKey, PARTITIONING_1);
    Assert.assertEquals(PARTITION_KEY, decoded);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDecodeIncomplete() {
    byte[] rowKey = PartitionedFileSetDataset.generateRowKey(PARTITION_KEY, PARTITIONING_1);
    PartitionedFileSetDataset.parseRowKey(rowKey, PARTITIONING_2);
  }

  @Test
  public void testMetadataForNonexistentPartition() throws Exception {
    PartitionedFileSet pfs = dsFrameworkUtil.getInstance(pfsInstance);
    PartitionKey key = generateUniqueKey();
    try {
      // didn't add any partitions to the dataset, so any partition key should throw a PartitionNotFoundException
      pfs.addMetadata(key, "metaKey", "metaValue");
      Assert.fail("Expected not to find key: " + key);
    } catch (PartitionNotFoundException e) {
      Assert.assertEquals(pfsInstance.getId(), e.getPartitionedFileSetName());
      Assert.assertEquals(key, e.getPartitionKey());
    }
  }

  @Test
  public void testPartitionConsumer() throws Exception {
    // exercises the edge case of partition consumption, when partitions are being consumed, while another in-progress
    // transaction has added a partition, but it has not yet committed, so the partition is not available for the
    // consumer
    // note: each concurrent transaction needs its own instance of the dataset because the dataset holds the txId
    // as an instance variable
    PartitionedFileSet dataset1 = dsFrameworkUtil.getInstance(pfsInstance);
    PartitionedFileSet dataset2 = dsFrameworkUtil.getInstance(pfsInstance);
    PartitionedFileSet dataset3 = dsFrameworkUtil.getInstance(pfsInstance);
    TransactionManager txManager = dsFrameworkUtil.getTxManager();
    InMemoryTxSystemClient txClient = new InMemoryTxSystemClient(txManager);

    // producer simply adds initial partition
    TransactionContext txContext1 = new TransactionContext(txClient, (TransactionAware) dataset1);
    txContext1.start();
    PartitionKey partitionKey1 = generateUniqueKey();
    dataset1.getPartitionOutput(partitionKey1).addPartition();
    txContext1.finish();

    // consumer simply consumes initial partition
    TransactionContext txContext2 = new TransactionContext(txClient, (TransactionAware) dataset2);
    txContext2.start();
    SimplePartitionConsumer partitionConsumer = new SimplePartitionConsumer(dataset2);
    List<PartitionDetail> partitions = partitionConsumer.consumePartitions();
    Assert.assertEquals(1, partitions.size());
    Assert.assertEquals(partitionKey1, partitions.get(0).getPartitionKey());
    txContext2.finish();

    // producer adds a 2nd partition but does not yet commit the transaction
    txContext1.start();
    PartitionKey partitionKey2 = generateUniqueKey();
    dataset1.getPartitionOutput(partitionKey2).addPartition();

    // another producer adds a 3rd partition, but does not yet commit the transaction
    TransactionContext txContext3 = new TransactionContext(txClient, (TransactionAware) dataset3);
    txContext3.start();
    PartitionKey partitionKey3 = generateUniqueKey();
    dataset3.getPartitionOutput(partitionKey3).addPartition();

    // simply start and commit a transaction so the next transaction's read pointer is higher than the previous
    // transaction's write pointer. Otherwise, the previous transaction may not get included in the in-progress list
    txContext2.start();
    txContext2.finish();

    // consumer attempts to consume at a time after the partition was added, but before it committed. Because of this,
    // the partition is not visible and will not be consumed
    txContext2.start();
    Assert.assertTrue(partitionConsumer.consumePartitions().isEmpty());
    txContext2.finish();

    // both producers commit the transaction in which the second partition was added
    txContext1.finish();
    txContext3.finish();

    // the next time the consumer runs, it processes the second partition
    txContext2.start();
    partitions = partitionConsumer.consumePartitions();
    Assert.assertEquals(2, partitions.size());
    // ordering may be different
    Assert.assertEquals(ImmutableSet.of(partitionKey2, partitionKey3),
                        ImmutableSet.of(partitions.get(0).getPartitionKey(), partitions.get(1).getPartitionKey()));
    txContext2.finish();
  }

  @Test
  public void testSimplePartitionConsuming() throws Exception {
    final PartitionedFileSet dataset = dsFrameworkUtil.getInstance(pfsInstance);
    final TransactionAware txAwareDataset = (TransactionAware) dataset;

    final Set<PartitionKey> partitionKeys1 = Sets.newHashSet();
    for (int i = 0; i < 10; i++) {
      partitionKeys1.add(generateUniqueKey());
    }

    final Set<PartitionKey> partitionKeys2 = Sets.newHashSet();
    for (int i = 0; i < 15; i++) {
      partitionKeys2.add(generateUniqueKey());
    }

    final SimplePartitionConsumer partitionConsumer = new SimplePartitionConsumer(dataset);
    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        for (PartitionKey partitionKey : partitionKeys1) {
          dataset.getPartitionOutput(partitionKey).addPartition();
        }
      }
    });

    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // Initial consumption results in the partitions corresponding to partitionKeys1 to be consumed because only
        // those partitions are added to the dataset at this point
        List<Partition> consumedPartitions = Lists.newArrayList();
        Iterables.addAll(consumedPartitions, partitionConsumer.consumePartitions());

        Set<PartitionKey> retrievedKeys = Sets.newHashSet();
        for (Partition consumedPartition : consumedPartitions) {
          retrievedKeys.add(consumedPartition.getPartitionKey());
        }
        Assert.assertEquals(partitionKeys1, retrievedKeys);
      }
    });

    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        for (PartitionKey partitionKey : partitionKeys2) {
          dataset.getPartitionOutput(partitionKey).addPartition();
        }
      }
    });

    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // using the same PartitionConsumer (which remembers the PartitionConsumerState) to consume additional
        // partitions results in only the newly added partitions (corresponding to partitionKeys2) to be returned
        List<Partition> consumedPartitions = Lists.newArrayList();
        Iterables.addAll(consumedPartitions, partitionConsumer.consumePartitions());

        Set<PartitionKey> retrievedKeys = Sets.newHashSet();
        for (Partition consumedPartition : consumedPartitions) {
          retrievedKeys.add(consumedPartition.getPartitionKey());
        }
        Assert.assertEquals(partitionKeys2, retrievedKeys);
      }
    });

    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // consuming the partitions again, without adding any new partitions returns an empty iterator
        Assert.assertTrue(partitionConsumer.consumePartitions().isEmpty());
      }
    });

    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // creating a new PartitionConsumer resets the consumption state. Consuming from it then returns an iterator
        // with all the partition keys
        List<Partition> consumedPartitions = Lists.newArrayList();
        Iterables.addAll(consumedPartitions, new SimplePartitionConsumer(dataset).consumePartitions());

        Set<PartitionKey> retrievedKeys = Sets.newHashSet();
        for (Partition consumedPartition : consumedPartitions) {
          retrievedKeys.add(consumedPartition.getPartitionKey());
        }
        Set<PartitionKey> allKeys = Sets.newHashSet();
        allKeys.addAll(partitionKeys1);
        allKeys.addAll(partitionKeys2);
        Assert.assertEquals(allKeys, retrievedKeys);
      }
    });
  }


  @Test
  public void testPartitionConsumingWithFilterAndLimit() throws Exception {
    final PartitionedFileSet dataset = dsFrameworkUtil.getInstance(pfsInstance);
    final TransactionAware txAwareDataset = (TransactionAware) dataset;

    final Set<PartitionKey> partitionKeys1 = Sets.newHashSet();
    for (int i = 0; i < 10; i++) {
      partitionKeys1.add(generateUniqueKey());
    }

    final Set<PartitionKey> partitionKeys2 = Sets.newHashSet();
    for (int i = 0; i < 15; i++) {
      partitionKeys2.add(generateUniqueKey());
    }

    final SimplePartitionConsumer partitionConsumer = new SimplePartitionConsumer(dataset);
    // add each of partitionKeys1 in separate transaction, so limit can be applied at arbitrary values
    // (consumption only happens at transaction borders)
    for (final PartitionKey partitionKey : partitionKeys1) {
      dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
            dataset.getPartitionOutput(partitionKey).addPartition();
        }
      });
    }

    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // Initial consumption results in the partitions corresponding to partitionKeys1 to be consumed because only
        // those partitions are added to the dataset at this point
        List<Partition> consumedPartitions = Lists.newArrayList();

        // with limit = 1, the returned iterator is only size 1, even though there are more unconsumed partitions
        Iterables.addAll(consumedPartitions, partitionConsumer.consumePartitions(1));
        Assert.assertEquals(1, consumedPartitions.size());

        // ask for 5 more
        Iterables.addAll(consumedPartitions, partitionConsumer.consumePartitions(5));
        Assert.assertEquals(6, consumedPartitions.size());

        // ask for 5 more, but there are only 4 more unconsumed partitions (size of partitionKeys1 is 10).
        Iterables.addAll(consumedPartitions, partitionConsumer.consumePartitions(5));
        Assert.assertEquals(10, consumedPartitions.size());

        Set<PartitionKey> retrievedKeys = Sets.newHashSet();
        for (Partition consumedPartition : consumedPartitions) {
          retrievedKeys.add(consumedPartition.getPartitionKey());
        }
        Assert.assertEquals(partitionKeys1, retrievedKeys);
      }
    });

    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        for (PartitionKey partitionKey : partitionKeys2) {
          dataset.getPartitionOutput(partitionKey).addPartition();
        }
      }
    });

    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // using the same PartitionConsumer (which remembers the PartitionConsumerState) to consume additional
        // partitions results in only the newly added partitions (corresponding to partitionKeys2) to be returned
        List<Partition> consumedPartitions = Lists.newArrayList();
        Iterables.addAll(consumedPartitions, partitionConsumer.consumePartitions(1));

        // even though we set limit to 1 in the previous call to consumePartitions, we get all the elements of
        // partitionKeys2, because they were all added in the same transaction
        Set<PartitionKey> retrievedKeys = Sets.newHashSet();
        for (Partition consumedPartition : consumedPartitions) {
          retrievedKeys.add(consumedPartition.getPartitionKey());
        }
        Assert.assertEquals(partitionKeys2, retrievedKeys);
      }
    });

    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // consuming the partitions again, without adding any new partitions returns an empty iterator
        Assert.assertTrue(partitionConsumer.consumePartitions().isEmpty());
      }
    });

    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // creating a new PartitionConsumer resets the consumption state.
        // test combination of filter and limit
        SimplePartitionConsumer newPartitionConsumer = new SimplePartitionConsumer(dataset);
        List<Partition> consumedPartitions = Lists.newArrayList();
        // the partitionFilter will match partitionKeys [1, 7), of which there are 6
        final PartitionFilter partitionFilter = PartitionFilter.builder().addRangeCondition("i", 1, 7).build();
        final Predicate<PartitionDetail> predicate = new Predicate<PartitionDetail>() {
          @Override
          public boolean apply(PartitionDetail partitionDetail) {
            return partitionFilter.match(partitionDetail.getPartitionKey());
          }
        };

        // apply the filter (narrows it down to 6 elements) and apply a limit of 4 results in 4 consumed partitions
        Iterables.addAll(consumedPartitions, newPartitionConsumer.consumePartitions(4, predicate));
        Assert.assertEquals(4, consumedPartitions.size());

        // apply a limit of 3, using the same filter returns the remaining 2 elements that fit that filter
        Iterables.addAll(consumedPartitions, newPartitionConsumer.consumePartitions(3, predicate));
        Assert.assertEquals(6, consumedPartitions.size());

        // assert that the partitions returned have partition keys, where the i values range from [1, 7]
        Set<Integer> expectedIFields = new HashSet<>();
        for (int i = 1; i < 7; i++) {
          expectedIFields.add(i);
        }
        Set<Integer> actualIFields = new HashSet<>();
        for (Partition consumedPartition : consumedPartitions) {
          actualIFields.add((Integer) consumedPartition.getPartitionKey().getField("i"));
        }
        Assert.assertEquals(expectedIFields, actualIFields);
      }
    });
  }

  private int counter = 0;
  // generates unique partition keys, where the 'i' field is incrementing from 0 upwards on each returned key
  private PartitionKey generateUniqueKey() {
    return PartitionKey.builder()
      .addIntField("i", counter++)
      .addLongField("l", 17L)
      .addStringField("s", UUID.randomUUID().toString())
      .build();
  }

  @Test
  public void testPartitionCreationTime() throws Exception {
    final PartitionedFileSet dataset = dsFrameworkUtil.getInstance(pfsInstance);

    dsFrameworkUtil.newTransactionExecutor((TransactionAware) dataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        PartitionOutput partitionOutput = dataset.getPartitionOutput(PARTITION_KEY);
        long beforeTime = System.currentTimeMillis();
        partitionOutput.addPartition();
        long afterTime = System.currentTimeMillis();

        PartitionDetail partitionDetail = dataset.getPartition(PARTITION_KEY);
        Assert.assertNotNull(partitionDetail);
        long creationTime = partitionDetail.getMetadata().getCreationTime();
        Assert.assertTrue(creationTime >= beforeTime && creationTime <= afterTime);
      }
    });
  }

  @Test
  public void testPartitionMetadata() throws Exception {
    final PartitionedFileSet dataset = dsFrameworkUtil.getInstance(pfsInstance);

    dsFrameworkUtil.newTransactionExecutor((TransactionAware) dataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        PartitionKey partitionKey = PartitionKey.builder()
          .addIntField("i", 42)
          .addLongField("l", 17L)
          .addStringField("s", "x")
          .build();

        ImmutableMap<String, String> metadata = ImmutableMap.of("key1", "value",
                                                                "key2", "value2",
                                                                "key3", "value2");

        PartitionOutput partitionOutput = dataset.getPartitionOutput(partitionKey);
        partitionOutput.setMetadata(metadata);
        partitionOutput.addPartition();

        PartitionDetail partitionDetail = dataset.getPartition(partitionKey);
        Assert.assertNotNull(partitionDetail);
        Assert.assertEquals(metadata, partitionDetail.getMetadata().asMap());
      }
    });
  }

  @Test
  public void testUpdateMetadata() throws Exception {
    final PartitionedFileSet dataset = dsFrameworkUtil.getInstance(pfsInstance);
    dsFrameworkUtil.newTransactionExecutor((TransactionAware) dataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        PartitionOutput partitionOutput = dataset.getPartitionOutput(PARTITION_KEY);
        ImmutableMap<String, String> originalEntries = ImmutableMap.of("key1", "value1");
        partitionOutput.setMetadata(originalEntries);
        partitionOutput.addPartition();

        ImmutableMap<String, String> updatedMetadata = ImmutableMap.of("key2", "value2");
        dataset.addMetadata(PARTITION_KEY, updatedMetadata);

        PartitionDetail partitionDetail = dataset.getPartition(PARTITION_KEY);
        Assert.assertNotNull(partitionDetail);

        HashMap<String, String> combinedEntries = Maps.newHashMap();
        combinedEntries.putAll(originalEntries);
        combinedEntries.putAll(updatedMetadata);
        Assert.assertEquals(combinedEntries, partitionDetail.getMetadata().asMap());

        // adding an entry, for a key that already exists will throw an Exception
        try {
          dataset.addMetadata(PARTITION_KEY, "key2", "value3");
          Assert.fail("Expected not to be able to update an existing metadata entry");
        } catch (DataSetException expected) {
        }

        PartitionKey nonexistentPartitionKey = PartitionKey.builder()
          .addIntField("i", 42)
          .addLongField("l", 17L)
          .addStringField("s", "nonexistent")
          .build();

        try {
          // adding an entry, for a key that already exists will throw an Exception
          dataset.addMetadata(nonexistentPartitionKey, "key2", "value3");
          Assert.fail("Expected not to be able to add metadata for a nonexistent partition");
        } catch (DataSetException expected) {
        }
      }
    });
  }

  @Test
  public void testAddRemoveGetPartition() throws Exception {
    final PartitionedFileSet pfs = dsFrameworkUtil.getInstance(pfsInstance);

    dsFrameworkUtil.newTransactionExecutor((TransactionAware) pfs).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        PartitionOutput output = pfs.getPartitionOutput(PARTITION_KEY);
        Location outputLocation = output.getLocation().append("file");
        OutputStream out = outputLocation.getOutputStream();
        out.close();
        output.addPartition();
        Assert.assertTrue(outputLocation.exists());
        Assert.assertNotNull(pfs.getPartition(PARTITION_KEY));
        Assert.assertTrue(pfs.getPartition(PARTITION_KEY).getLocation().exists());
        pfs.dropPartition(PARTITION_KEY);
        Assert.assertFalse(outputLocation.exists());
        Assert.assertNull(pfs.getPartition(PARTITION_KEY));
        pfs.dropPartition(PARTITION_KEY);
      }
    });
  }

  @Test
  public void testAddRemoveGetPartitionExternal() throws Exception {
    final File absolutePath = tmpFolder.newFolder();
    absolutePath.mkdirs();

    dsFrameworkUtil.createInstance("partitionedFileSet", pfsExternalInstance, PartitionedFileSetProperties.builder()
      .setPartitioning(PARTITIONING_1)
      .setBasePath(absolutePath.getPath())
      .setDataExternal(true)
      .build());
    final PartitionedFileSet pfs = dsFrameworkUtil.getInstance(pfsExternalInstance);

    dsFrameworkUtil.newTransactionExecutor((TransactionAware) pfs).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Location baseLocation = pfs.getEmbeddedFileSet().getBaseLocation();
        Assert.assertTrue(pfsBaseLocation.exists());

        // attempt to write a new partition - should fail
        try {
          PartitionOutput output = pfs.getPartitionOutput(PARTITION_KEY);
          Assert.fail("External partitioned file set should not allow writing files");
        } catch (UnsupportedOperationException e) {
          // expected
        }

        // create an external file and addit as a partition
        File someFile = new File(absolutePath, "some.file");
        OutputStream out = new FileOutputStream(someFile);
        out.close();
        Assert.assertTrue(someFile.exists());
        pfs.addPartition(PARTITION_KEY, "some.file");
        Assert.assertNotNull(pfs.getPartition(PARTITION_KEY));
        Assert.assertTrue(pfs.getPartition(PARTITION_KEY).getLocation().exists());

        // now drop the partition and validate the file is still there
        pfs.dropPartition(PARTITION_KEY);
        Assert.assertNull(pfs.getPartition(PARTITION_KEY));
        Assert.assertTrue(someFile.exists());
      }
    });
    // drop the dataset and validate that the base dir still exists
    dsFrameworkUtil.deleteInstance(pfsExternalInstance);
    Assert.assertTrue(pfsBaseLocation.exists());
    Assert.assertTrue(absolutePath.isDirectory());
  }

  @Test
  @Category(SlowTests.class)
  public void testAddRemoveGetPartitions() throws Exception {

    final PartitionedFileSet dataset = dsFrameworkUtil.getInstance(pfsInstance);

    final PartitionKey[][][] keys = new PartitionKey[4][4][4];
    final String[][][] paths = new String[4][4][4];
    final Set<BasicPartition> allPartitionDetails = Sets.newHashSet();

    // add a bunch of partitions
    for (int s = 0; s < 4; s++) {
      for (int i = 0; i < 4; i++) {
        for (int l = 0; l < 4; l++) {
          final PartitionKey key = PartitionKey.builder()
            .addField("s", String.format("%c-%d", 'a' + s, s))
            .addField("i", i * 100)
            .addField("l", 15L - 10 * l)
            .build();
          BasicPartition basicPartition = dsFrameworkUtil.newTransactionExecutor((TransactionAware) dataset)
            .execute(new Callable<BasicPartition>() {
              @Override
              public BasicPartition call() throws Exception {
                PartitionOutput p = dataset.getPartitionOutput(key);
                p.addPartition();
                return new BasicPartition((PartitionedFileSetDataset) dataset,
                                          p.getRelativePath(), p.getPartitionKey());
              }
            });
          keys[s][i][l] = key;
          paths[s][i][l] = basicPartition.getRelativePath();
          allPartitionDetails.add(basicPartition);
        }
      }
    }

    // validate getPartition with exact partition key
    for (int s = 0; s < 4; s++) {
      for (int i = 0; i < 4; i++) {
        for (int l = 0; l < 4; l++) {
          final PartitionKey key = keys[s][i][l];
          final String path = paths[s][i][l];
          dsFrameworkUtil.newTransactionExecutor((TransactionAware) dataset).execute(
            new TransactionExecutor.Subroutine() {
              @Override
              public void apply() throws Exception {
                PartitionDetail partitionDetail = dataset.getPartition(key);
                Assert.assertNotNull(partitionDetail);
                Assert.assertEquals(path, partitionDetail.getRelativePath());
              }
          });
          // also test getPartitionPaths() and getPartitions() for the filter matching this
          @SuppressWarnings({"unchecked", "unused"})
          boolean success = testFilter(dataset, allPartitionDetails,
                                       PartitionFilter.builder()
                                         .addValueCondition("l", key.getField("l"))
                                         .addValueCondition("s", key.getField("s"))
                                         .addValueCondition("i", key.getField("i"))
                                         .build());
        }
      }
    }

    // test whether query works without filter
    testFilter(dataset, allPartitionDetails, null);

    // generate an list of partition filters with exhaustive coverage
    List<PartitionFilter> filters = generateFilters();

    // test all kinds of filters
    testAllFilters(dataset, allPartitionDetails, filters);

    // remove a few of the partitions and test again, repeatedly
    PartitionKey[] keysToRemove = { keys[1][2][3], keys[0][1][0], keys[2][3][2], keys[3][1][2] };
    for (final PartitionKey key : keysToRemove) {

      // remove in a transaction
      dsFrameworkUtil.newTransactionExecutor((TransactionAware) dataset).execute(
        new TransactionExecutor.Procedure<PartitionKey>() {
          @Override
          public void apply(PartitionKey partitionKey) throws Exception {
            dataset.dropPartition(partitionKey);
          }
      }, key);

      // test all filters
      BasicPartition toRemove = Iterables.tryFind(allPartitionDetails,
                                                  new com.google.common.base.Predicate<BasicPartition>() {
        @Override
        public boolean apply(BasicPartition partition) {
          return key.equals(partition.getPartitionKey());
        }
      }).get();
      allPartitionDetails.remove(toRemove);
      testAllFilters(dataset, allPartitionDetails, filters);
    }

  }

  private void testAllFilters(PartitionedFileSet dataset,
                              Set<BasicPartition> allPartitionDetails,
                              List<PartitionFilter> filters) throws Exception {
    for (PartitionFilter filter : filters) {
      try {
        testFilter(dataset, allPartitionDetails, filter);
      } catch (Throwable e) {
        throw new Exception("testFilter() failed for filter: " + filter, e);
      }
    }
  }

  private boolean testFilter(final PartitionedFileSet dataset,
                             Set<BasicPartition> allPartitionDetails,
                             final PartitionFilter filter) throws Exception {

    // determine the keys and paths that match the filter
    final Set<BasicPartition> matching = filter == null ? allPartitionDetails :
      Sets.filter(allPartitionDetails, new com.google.common.base.Predicate<BasicPartition>() {
        @Override
        public boolean apply(BasicPartition partition) {
          return filter.match(partition.getPartitionKey());
        }
      });

    dsFrameworkUtil.newTransactionExecutor((TransactionAware) dataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Set<PartitionDetail> retrievedPartitionDetails = dataset.getPartitions(filter);
        HashSet<BasicPartition> retrievedBasicPartitions = Sets.newHashSet();
        for (PartitionDetail retrievedPartition : retrievedPartitionDetails) {
          retrievedBasicPartitions.add(new BasicPartition((PartitionedFileSetDataset) dataset,
                                                          retrievedPartition.getRelativePath(),
                                                          retrievedPartition.getPartitionKey()));
        }
        Assert.assertEquals(matching, retrievedBasicPartitions);
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
