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

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.dataset.lib.Partition;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionFilter;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.dataset.lib.partitioned.ConcurrentPartitionConsumer;
import co.cask.cdap.api.dataset.lib.partitioned.ConsumablePartition;
import co.cask.cdap.api.dataset.lib.partitioned.ConsumerConfiguration;
import co.cask.cdap.api.dataset.lib.partitioned.ConsumerWorkingSet;
import co.cask.cdap.api.dataset.lib.partitioned.PartitionAcceptor;
import co.cask.cdap.api.dataset.lib.partitioned.PartitionConsumer;
import co.cask.cdap.api.dataset.lib.partitioned.PartitionConsumerResult;
import co.cask.cdap.api.dataset.lib.partitioned.ProcessState;
import co.cask.cdap.api.dataset.lib.partitioned.StatePersistor;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.inmemory.InMemoryTxSystemClient;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.twill.filesystem.Location;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Tests PartitionConsumer.
 */
public class PartitionConsumerTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();
  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  private static final Partitioning PARTITIONING_1 = Partitioning.builder()
    .addStringField("s")
    .addIntField("i")
    .addLongField("l")
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

  @Test
  public void testPartitionConsumer() throws Exception {
    // exercises the edge case of partition consumption, when partitions are being consumed, while another in-progress
    // transaction has added a partition, but it has not yet committed, so the partition is not available for the
    // consumer
    PartitionedFileSet dataset1 = dsFrameworkUtil.getInstance(pfsInstance);
    PartitionedFileSet dataset2 = dsFrameworkUtil.getInstance(pfsInstance);
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

    PartitionConsumer partitionConsumer = new ConcurrentPartitionConsumer(dataset2, new InMemoryStatePersistor());
    List<? extends PartitionDetail> partitionIterator = partitionConsumer.consumePartitions().getPartitions();
    Assert.assertEquals(1, partitionIterator.size());
    Assert.assertEquals(partitionKey1, partitionIterator.get(0).getPartitionKey());
    txContext2.finish();

    // producer adds a second partition, but does not yet commit the transaction
    txContext1.start();
    PartitionKey partitionKey2 = generateUniqueKey();
    dataset1.getPartitionOutput(partitionKey2).addPartition();

    // consumer attempts to consume at a time after the partition was added, but before it committed. Because of this,
    // the partition is not visible and will not be consumed
    txContext2.start();
    Assert.assertTrue(partitionConsumer.consumePartitions().getPartitions().isEmpty());
    txContext2.finish();

    // producer commits the transaction in which the second partition was added
    txContext1.finish();

    // the next time the consumer runs, it processes the second partition
    txContext2.start();
    partitionIterator = partitionConsumer.consumePartitions().getPartitions();
    Assert.assertEquals(1, partitionIterator.size());
    Assert.assertEquals(partitionKey2, partitionIterator.get(0).getPartitionKey());
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

    final PartitionConsumer partitionConsumer =
      new ConcurrentPartitionConsumer(dataset, new InMemoryStatePersistor());
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
        Iterables.addAll(consumedPartitions, partitionConsumer.consumePartitions().getPartitions());

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
        Iterables.addAll(consumedPartitions, partitionConsumer.consumePartitions().getPartitions());

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
        Assert.assertTrue(partitionConsumer.consumePartitions().getPartitions().isEmpty());
      }
    });

    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // creating a new PartitionConsumer resets the consumption state. Consuming from it then returns an iterator
        // with all the partition keys
        List<Partition> consumedPartitions = Lists.newArrayList();
        List<? extends PartitionDetail> partitionDetails =
          new ConcurrentPartitionConsumer(dataset, new InMemoryStatePersistor())
            .consumePartitions().getPartitions();
        Iterables.addAll(consumedPartitions, partitionDetails);

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
  public void testConsumeAfterDelete() throws Exception {
    final PartitionedFileSet dataset = dsFrameworkUtil.getInstance(pfsInstance);
    final TransactionAware txAwareDataset = (TransactionAware) dataset;

    final Set<PartitionKey> partitionKeys1 = Sets.newHashSet();
    for (int i = 0; i < 3; i++) {
      partitionKeys1.add(generateUniqueKey());
    }

    // need to ensure that our consumerConfiguration is larger than the amount we consume initially, so that
    // additional partitions (which will be deleted afterwards) are brought into the working set
    ConsumerConfiguration consumerConfiguration = ConsumerConfiguration.builder().setMaxWorkingSetSize(100).build();

    final PartitionConsumer partitionConsumer =
      new ConcurrentPartitionConsumer(dataset, new InMemoryStatePersistor(), consumerConfiguration);
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
        // add 2 more partitions after the first 3. We do not need to keep track of these, because they will be dropped
        // and not consumed
        for (int i = 0; i < 2; i++) {
          dataset.getPartitionOutput(generateUniqueKey()).addPartition();
        }
      }
    });

    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // consume 3 of the 5 initial partitions
        List<Partition> consumedPartitions = Lists.newArrayList();
        Iterables.addAll(consumedPartitions, partitionConsumer.consumePartitions(3).getPartitions());

        Set<PartitionKey> retrievedKeys = Sets.newHashSet();
        for (Partition consumedPartition : consumedPartitions) {
          retrievedKeys.add(consumedPartition.getPartitionKey());
        }
        Assert.assertEquals(partitionKeys1, retrievedKeys);
      }
    });


    final Set<PartitionKey> partitionKeys2 = Sets.newHashSet();
    for (int i = 0; i < 5; i++) {
      partitionKeys2.add(generateUniqueKey());
    }

    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // drop all existing partitions (2 of which are not consumed)
        for (PartitionDetail partitionDetail : dataset.getPartitions(PartitionFilter.ALWAYS_MATCH)) {
          dataset.dropPartition(partitionDetail.getPartitionKey());
        }
        // add 5 new ones
        for (PartitionKey partitionKey : partitionKeys2) {
          dataset.getPartitionOutput(partitionKey).addPartition();
        }
      }
    });

    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        List<Partition> consumedPartitions = Lists.newArrayList();
        Iterables.addAll(consumedPartitions, partitionConsumer.consumePartitions().getPartitions());

        Set<PartitionKey> retrievedKeys = Sets.newHashSet();
        for (Partition consumedPartition : consumedPartitions) {
          retrievedKeys.add(consumedPartition.getPartitionKey());
        }
        // the consumed partition keys should correspond to partitionKeys2, and not include the dropped, but unconsumed
        // partitions added before them
        Assert.assertEquals(partitionKeys2, retrievedKeys);
      }
    });

    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // consuming the partitions again, without adding any new partitions returns an empty iterator
        Assert.assertTrue(partitionConsumer.consumePartitions().getPartitions().isEmpty());
      }
    });

    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // creating a new PartitionConsumer resets the consumption state. Consuming from it then returns an iterator
        // with all the partition keys added after the deletions
        List<Partition> consumedPartitions = Lists.newArrayList();
        List<? extends PartitionDetail> partitionDetails =
          new ConcurrentPartitionConsumer(dataset, new InMemoryStatePersistor())
            .consumePartitions().getPartitions();
        Iterables.addAll(consumedPartitions, partitionDetails);

        Set<PartitionKey> retrievedKeys = Sets.newHashSet();
        for (Partition consumedPartition : consumedPartitions) {
          retrievedKeys.add(consumedPartition.getPartitionKey());
        }
        Set<PartitionKey> allKeys = Sets.newHashSet();
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

    final PartitionConsumer partitionConsumer = new ConcurrentPartitionConsumer(dataset, new InMemoryStatePersistor());
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
        Iterables.addAll(consumedPartitions, partitionConsumer.consumePartitions(1).getPartitions());
        Assert.assertEquals(1, consumedPartitions.size());

        // ask for 5 more
        Iterables.addAll(consumedPartitions, partitionConsumer.consumePartitions(5).getPartitions());
        Assert.assertEquals(6, consumedPartitions.size());

        // ask for 5 more, but there are only 4 more unconsumed partitions (size of partitionKeys1 is 10).
        Iterables.addAll(consumedPartitions, partitionConsumer.consumePartitions(5).getPartitions());
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
        Iterables.addAll(consumedPartitions, partitionConsumer.consumePartitions().getPartitions());

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
        Assert.assertTrue(partitionConsumer.consumePartitions().getPartitions().isEmpty());
      }
    });

    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // creating a new PartitionConsumer resets the consumption state.
        // test combination of filter and limit

        // the partitionFilter will match partitionKeys [1, 7), of which there are 6
        final PartitionFilter partitionFilter = PartitionFilter.builder().addRangeCondition("i", 1, 7).build();
        final Predicate<PartitionDetail> predicate = new Predicate<PartitionDetail>() {
          @Override
          public boolean apply(PartitionDetail partitionDetail) {
            return partitionFilter.match(partitionDetail.getPartitionKey());
          }
        };

        ConsumerConfiguration configuration =
          ConsumerConfiguration.builder().setPartitionPredicate(predicate).build();
        PartitionConsumer newPartitionConsumer =
          new ConcurrentPartitionConsumer(dataset, new InMemoryStatePersistor(), configuration);
        List<Partition> consumedPartitions = Lists.newArrayList();

        // apply the filter (narrows it down to 6 elements) and apply a limit of 4 results in 4 consumed partitions
        Iterables.addAll(consumedPartitions, newPartitionConsumer.consumePartitions(4).getPartitions());
        Assert.assertEquals(4, consumedPartitions.size());

        // apply a limit of 3, using the same filter returns the remaining 2 elements that fit that filter
        Iterables.addAll(consumedPartitions, newPartitionConsumer.consumePartitions(3).getPartitions());
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

  private static final class InMemoryStatePersistor implements StatePersistor {
    private byte[] state;

    @Override
    public void persistState(byte[] state) {
      this.state = state;
    }

    @Nullable
    @Override
    public byte[] readState() {
      return state;
    }
  }

  @Test
  public void testSimpleConcurrency() throws Exception {
    final PartitionedFileSet dataset = dsFrameworkUtil.getInstance(pfsInstance);
    final TransactionAware txAwareDataset = (TransactionAware) dataset;

    final Set<PartitionKey> partitionKeys = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      partitionKeys.add(generateUniqueKey());
    }

    // have ConcurrentPartitionConsumers that share the same state.
    InMemoryStatePersistor persistor = new InMemoryStatePersistor();
    ConsumerConfiguration configuration = ConsumerConfiguration.builder().setMaxRetries(3).build();

    final PartitionConsumer partitionConsumer1 = new ConcurrentPartitionConsumer(dataset, persistor, configuration);
    final PartitionConsumer partitionConsumer2 = new ConcurrentPartitionConsumer(dataset, persistor, configuration);
    final PartitionConsumer partitionConsumer3 = new ConcurrentPartitionConsumer(dataset, persistor, configuration);

    // add all ten keys to the partitioned fileset
    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        for (final PartitionKey partitionKey : partitionKeys) {
          dataset.getPartitionOutput(partitionKey).addPartition();
        }
      }
    });

    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // with limit = 1, the returned iterator is only size 1, even though there are more unconsumed partitions
        List<PartitionDetail> consumedBy1 = partitionConsumer1.consumePartitions(1).getPartitions();
        Assert.assertEquals(1, consumedBy1.size());

        // partitionConsumer2 asks for 10 partitions, but 1 is currently in progress by partitionConsumer1, so it only
        // gets the remaining 9 partitions
        List<PartitionDetail> consumedBy2 = partitionConsumer2.consumePartitions(10).getPartitions();
        Assert.assertEquals(9, consumedBy2.size());

        // partitionConsumer3 tries to consume partitions, but all are marked in-progress by partitionConsumer 1 and 2
        Assert.assertEquals(0, partitionConsumer3.consumePartitions().getPartitions().size());

        // partitionConsumer1 aborts its partition, so it then becomes available for partitionConsumer3
        partitionConsumer1.onFinish(consumedBy1, false);
        consumedBy1.clear();

        // queries with limit=2, but only the 1 is available that partitionConsumer1 released
        List<PartitionDetail> consumedBy3 = partitionConsumer3.consumePartitions(2).getPartitions();
        Assert.assertEquals(1, consumedBy3.size());

        // partitionConsumers 2 and 3 marks that it successfully processed the partitions
        partitionConsumer3.onFinish(consumedBy3, true);

        // test onFinishWithKeys API
        List<PartitionKey> keysConsumedBy2 =
          Lists.transform(consumedBy2, new Function<PartitionDetail, PartitionKey>() {
          @Override
          public PartitionKey apply(PartitionDetail input) {
            return input.getPartitionKey();
          }
        });
        partitionConsumer2.onFinishWithKeys(keysConsumedBy2, true);

        // at this point, all partitions are processed, so no additional partitions are available for consumption
        Assert.assertEquals(0, partitionConsumer3.consumePartitions().getPartitions().size());


        List<PartitionDetail> allProcessedPartitions = new ArrayList<>();
        allProcessedPartitions.addAll(consumedBy1);
        allProcessedPartitions.addAll(consumedBy2);
        allProcessedPartitions.addAll(consumedBy3);

        List<PartitionKey> allProcessedKeys =
          Lists.transform(allProcessedPartitions, new Function<PartitionDetail, PartitionKey>() {
            @Override
            public PartitionKey apply(PartitionDetail input) {
              return input.getPartitionKey();
            }
          });

        // ordering may be different, since all the partitions were added in the same transaction
        Assert.assertEquals(partitionKeys, Sets.newHashSet(allProcessedKeys));
      }
    });
  }

  @Test
  public void testOnFinishWithInvalidPartition() throws Exception {
    // tests:
    //     - attempts to abort a Partition that is not IN_PROGRESS
    //     - attempts to commit a Partition that is already committed
    // both of these throw IllegalArgumentException
    final PartitionedFileSet dataset = dsFrameworkUtil.getInstance(pfsInstance);
    final TransactionAware txAwareDataset = (TransactionAware) dataset;

    ConsumerConfiguration configuration = ConsumerConfiguration.builder().setMaxRetries(3).build();
    final PartitionConsumer partitionConsumer =
      new ConcurrentPartitionConsumer(dataset, new InMemoryStatePersistor(), configuration);

    final PartitionKey partitionKey = generateUniqueKey();

    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.getPartitionOutput(partitionKey).addPartition();
      }
    });

    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        List<PartitionDetail> partitionDetails = partitionConsumer.consumePartitions(1).getPartitions();
        Assert.assertEquals(1, partitionDetails.size());

        // aborting the processing of the partition
        partitionConsumer.onFinish(partitionDetails, false);

        // calling abort on the partition again throws IllegalArgumentException, because the partitions passed in to
        // abort were not found to have IN_PROGRESS state
        try {
          partitionConsumer.onFinish(partitionDetails, false);
          Assert.fail("Expected not to be able to abort a partition that is not IN_PROGRESS");
        } catch (IllegalStateException expected) {
        }

        // try to process the partition again, this time marking it as complete (by passing in true)
        partitionDetails = partitionConsumer.consumePartitions(1).getPartitions();
        Assert.assertEquals(1, partitionDetails.size());

        partitionConsumer.onFinish(partitionDetails, true);

        // attempting to mark it as complete a second time will an IllegalArgumentException, because the partition
        // is not found to have an IN_PROGRESS state
        try {
          partitionConsumer.onFinish(partitionDetails, true);
          Assert.fail("Expected not to be able to call onFinish on a partition is not IN_PROGRESS");
        } catch (IllegalArgumentException expected) {
        }
      }
    });
  }

  @Test
  public void testNumRetries() throws Exception {
    final PartitionedFileSet dataset = dsFrameworkUtil.getInstance(pfsInstance);
    final TransactionAware txAwareDataset = (TransactionAware) dataset;

    final int numRetries = 1;
    ConsumerConfiguration configuration = ConsumerConfiguration.builder().setMaxRetries(numRetries).build();
    final PartitionConsumer partitionConsumer = new ConcurrentPartitionConsumer(dataset, new InMemoryStatePersistor(),
                                                                                configuration);

    final PartitionKey partitionKey = generateUniqueKey();

    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        dataset.getPartitionOutput(partitionKey).addPartition();
      }
    });

    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {

        // consuming and aborting the partition numRetries times plus one (for the first attempt) makes it get removed
        // from the working set
        for (int i = 0; i < numRetries + 1; i++) {
          List<PartitionDetail> partitionDetails = partitionConsumer.consumePartitions(1).getPartitions();
          Assert.assertEquals(1, partitionDetails.size());
          Assert.assertEquals(partitionKey, partitionDetails.get(0).getPartitionKey());

          // aborting the processing of the partition
          partitionConsumer.onFinish(partitionDetails, false);
        }

        // after the 2nd abort, the partition is discarded entirely, and so no partitions are available for consuming
        PartitionConsumerResult result = partitionConsumer.consumePartitions(1);
        Assert.assertEquals(0, result.getPartitions().size());
        Assert.assertEquals(1, result.getFailedPartitions().size());
        Assert.assertEquals(partitionKey, result.getFailedPartitions().get(0).getPartitionKey());
      }
    });
  }


  /**
   * Custom implementation of {@link ConcurrentPartitionConsumer} that returns only a single partition if it is that
   * partition's last attempt at processing.
   */
  public static final class CustomConsumer extends ConcurrentPartitionConsumer {

    public CustomConsumer(PartitionedFileSet partitionedFileSet, StatePersistor statePersistor,
                          ConsumerConfiguration configuration) {
      super(partitionedFileSet, statePersistor, configuration);
    }

    @Override
    public PartitionConsumerResult doConsume(ConsumerWorkingSet workingSet, PartitionAcceptor acceptor) {
      doExpiry(workingSet);
      workingSet.populate(getPartitionedFileSet(), getConfiguration());

      long now = System.currentTimeMillis();
      List<PartitionDetail> toConsume = new ArrayList<>();


      // check if the first available partition is on its last attempt. If so, return it as a single element.
      List<? extends ConsumablePartition> partitions = workingSet.getPartitions();
      if (partitions.size() >= 1) {
        ConsumablePartition firstPartition = partitions.get(0);
        if (isLastAttempt(firstPartition)) {
          firstPartition.take();
          firstPartition.setTimestamp(now);
          toConsume.add(getPartitionedFileSet().getPartition(firstPartition.getPartitionKey()));
          return new PartitionConsumerResult(toConsume, removeDiscardedPartitions(workingSet));
        }
      }

      for (ConsumablePartition consumablePartition : partitions) {
        if (ProcessState.AVAILABLE != consumablePartition.getProcessState()) {
          continue;
        }
        // if the first available partition is not on its last attempt, perform the regular partition consuming,
        // but skipping any partitions that are on their last attempt
        if (isLastAttempt(consumablePartition)) {
          continue;
        }
        PartitionDetail partition = getPartitionedFileSet().getPartition(consumablePartition.getPartitionKey());
        PartitionAcceptor.Return accept = acceptor.accept(partition);
        switch (accept) {
          case ACCEPT:
            consumablePartition.take();
            consumablePartition.setTimestamp(now);
            toConsume.add(partition);
            continue;
          case SKIP:
            continue;
          case STOP:
            break;
        }
      }
      return new PartitionConsumerResult(toConsume, removeDiscardedPartitions(workingSet));
    }

    // returns true if the given partition only has one more attempt at processing before it is discarded
    private boolean isLastAttempt(ConsumablePartition partition) {
      return partition.getNumFailures() == getConfiguration().getMaxRetries() - 1;
    }
  }

  @Test
  public void testCustomOperations() throws Exception {
    final PartitionedFileSet dataset = dsFrameworkUtil.getInstance(pfsInstance);
    final TransactionAware txAwareDataset = (TransactionAware) dataset;

    ConsumerConfiguration configuration =
      ConsumerConfiguration.builder()
        .setMaxRetries(3)
        .build();

    final PartitionConsumer partitionConsumer =
      new CustomConsumer(dataset, new InMemoryStatePersistor(), configuration);

    final int numPartitions = 3;
    final List<PartitionKey> partitionKeys = new ArrayList<>(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      partitionKeys.add(generateUniqueKey());
    }

    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        for (PartitionKey partitionKey : partitionKeys) {
          dataset.getPartitionOutput(partitionKey).addPartition();
        }
      }
    });

    dsFrameworkUtil.newInMemoryTransactionExecutor(txAwareDataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        List<PartitionDetail> partitions = partitionConsumer.consumePartitions().getPartitions();
        Assert.assertEquals(numPartitions, partitions.size());
        partitionConsumer.onFinish(partitions, false);

        partitions = partitionConsumer.consumePartitions().getPartitions();
        Assert.assertEquals(numPartitions, partitions.size());
        partitionConsumer.onFinish(partitions, false);

        // after two failure attempts, the partitions are now returned individually
        partitions = partitionConsumer.consumePartitions().getPartitions();
        Assert.assertEquals(1, partitions.size());
        partitionConsumer.onFinish(partitions, true);

        partitions = partitionConsumer.consumePartitions().getPartitions();
        Assert.assertEquals(1, partitions.size());
        partitionConsumer.onFinish(partitions, true);

        partitions = partitionConsumer.consumePartitions().getPartitions();
        Assert.assertEquals(1, partitions.size());
        partitionConsumer.onFinish(partitions, true);
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
}
