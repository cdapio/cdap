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

import co.cask.cdap.api.dataset.lib.Partition;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;

import java.util.AbstractList;
import java.util.List;

/**
 * Abstract implementation of PartitionConsumer, which manages state persistence and serialization/deserialization
 * before delegating to the abstract methods.
 */
public abstract class AbstractPartitionConsumer implements PartitionConsumer {

  private final PartitionedFileSet partitionedFileSet;
  private final ConsumerConfiguration configuration;
  private final StatePersistor statePersistor;

  /**
   * This method will be called whenever a {@link PartitionConsumer} requests partitions, to consume partitions
   * from the working set, while marking them as IN_PROGRESS
   *
   * @param acceptor a {@link PartitionAcceptor} which defines which partitions to accept/consume
   * @return a {@link PartitionConsumerResult} representing the consumed partitions
   */
  public abstract PartitionConsumerResult doConsume(ConsumerWorkingSet workingSet, PartitionAcceptor acceptor);

  /**
   * This method will be called on any partitions returned by the {@code #consumePartitions} method.
   *
   * @param workingSet the working set of partitions to operate on
   * @param partitionKeys list of partition keys to mark as either succeeded or failed processing
   * @param succeeded whether or not processing of the specified partitions was successful
   */
  public abstract void doFinish(ConsumerWorkingSet workingSet, List<PartitionKey> partitionKeys, boolean succeeded);


  /**
   * Creates an instance of PartitionConsumer.
   *
   * @param partitionedFileSet the PartitionedFileSet to consume from
   * @param statePersistor defines how the state of the PartitionConsumer will be maintained
   */
  public AbstractPartitionConsumer(PartitionedFileSet partitionedFileSet, StatePersistor statePersistor) {
    this(partitionedFileSet, statePersistor, ConsumerConfiguration.DEFAULT);
  }

  /**
   * Creates an instance of PartitionConsumer.
   *
   * @param partitionedFileSet the PartitionedFileSet to consume from
   * @param statePersistor defines how the state of the PartitionConsumer will be maintained
   * @param configuration the PartitionedConsumerConfiguration, defining parameters of consumption
   */
  public AbstractPartitionConsumer(PartitionedFileSet partitionedFileSet,
                                   StatePersistor statePersistor, ConsumerConfiguration configuration) {
    this.partitionedFileSet = partitionedFileSet;
    this.configuration = configuration;
    this.statePersistor = statePersistor;
  }

  public PartitionedFileSet getPartitionedFileSet() {
    return partitionedFileSet;
  }

  public ConsumerConfiguration getConfiguration() {
    return configuration;
  }

  public StatePersistor getStatePersistor() {
    return statePersistor;
  }

  @Override
  public PartitionConsumerResult consumePartitions() {
    return consumePartitions(Integer.MAX_VALUE);
  }

  @Override
  public PartitionConsumerResult consumePartitions(int limit) {
    return consumePartitions(new PartitionAcceptor.Limit(limit));
  }

  @Override
  public PartitionConsumerResult consumePartitions(PartitionAcceptor acceptor) {
    ConsumerWorkingSet workingSet = readState();

    PartitionConsumerResult partitionsResult = doConsume(workingSet, acceptor);

    statePersistor.persistState(workingSet.toBytes());
    return partitionsResult;
  }

  @Override
  public void onFinish(final List<? extends Partition> partitions, boolean succeeded) {
    List<PartitionKey> partitionKeys = new AbstractList<PartitionKey>() {

      @Override
      public int size() {
        return partitions.size();
      }

      @Override
      public PartitionKey get(int index) {
        return partitions.get(index).getPartitionKey();
      }

      @Override
      public void clear() {
        partitions.clear();
      }

      @Override
      public PartitionKey remove(int index) {
        return partitions.remove(index).getPartitionKey();
      }
    };

    ConsumerWorkingSet workingSet = readState();

    doFinish(workingSet, partitionKeys, succeeded);
    statePersistor.persistState(workingSet.toBytes());
  }

  private ConsumerWorkingSet readState() {
    byte[] bytes = statePersistor.readState();
    return bytes == null ? new ConsumerWorkingSet() : ConsumerWorkingSet.fromBytes(bytes);
  }
}
