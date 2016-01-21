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

package co.cask.cdap.api.dataset.lib.partitioned;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.DatasetStatePersistor;
import co.cask.cdap.api.dataset.lib.Partition;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.tephra.TransactionFailureException;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An implementation of {@link PartitionConsumer} that uses a {@link Transactional} to execute its methods
 * in a new transaction. For each call to the methods it implements, it starts its own, new transaction.
 */
@Beta
public final class TransactionalPartitionConsumer implements PartitionConsumer {
  private final Transactional transactional;
  private final ConsumerConfiguration consumerConfiguration;
  private final String partitionedFileSetName;
  private final DatasetStatePersistor statePersistor;

  /**
   * @param transactional object used to start the new transactions
   * @param partitionedFileSetName the name of the {@link PartitionedFileSet} to consume partitions from
   * @param statePersistor a {@link DatasetStatePersistor} responsible for defining how the partition consumer state is
   *                       managed
   */
  public TransactionalPartitionConsumer(Transactional transactional, String partitionedFileSetName,
                                        DatasetStatePersistor statePersistor) {
    this(transactional, partitionedFileSetName, statePersistor, ConsumerConfiguration.DEFAULT);
  }

  /**
   * @param transactional object used to start the new transactions
   * @param partitionedFileSetName the name of the {@link PartitionedFileSet} to consume partitions from
   * @param statePersistor a {@link DatasetStatePersistor} responsible for defining how the partition consumer state is
   *                       managed
   * @param consumerConfiguration defines parameters for the partition consumption
   */
  public TransactionalPartitionConsumer(Transactional transactional, String partitionedFileSetName,
                                        DatasetStatePersistor statePersistor,
                                        ConsumerConfiguration consumerConfiguration) {
    this.transactional = transactional;
    this.partitionedFileSetName = partitionedFileSetName;
    this.statePersistor = statePersistor;
    this.consumerConfiguration = consumerConfiguration;
  }

  @Override
  public PartitionConsumerResult consumePartitions() {
    return consumePartitions(Integer.MAX_VALUE);
  }

  @Override
  public PartitionConsumerResult consumePartitions(final int limit) {
    return consumePartitions(new PartitionAcceptor.Limit(limit));
  }

  @Override
  public PartitionConsumerResult consumePartitions(final PartitionAcceptor acceptor) {

    final AtomicReference<PartitionConsumerResult> partitions = new AtomicReference<>();

    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          partitions.set(getPartitionConsumer(context).consumePartitions(acceptor));
        }
      });
    } catch (TransactionFailureException e) {
      throw new RuntimeException(e);
    }
    return partitions.get();
  }

  @Override
  public void onFinish(final List<? extends Partition> partitions, final boolean succeeded) {
    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          getPartitionConsumer(context).onFinish(partitions, succeeded);
        }
      });
    } catch (TransactionFailureException e) {
      throw new RuntimeException(e);
    }
  }

  private PartitionConsumer getPartitionConsumer(DatasetContext context) {
    PartitionedFileSet lines = context.getDataset(partitionedFileSetName);
    return new ConcurrentPartitionConsumer(lines, new DelegatingStatePersistor(context, statePersistor),
                                           consumerConfiguration);
  }
}
