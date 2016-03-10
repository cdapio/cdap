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

import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A {@link PartitionConsumer} that supports multiple instances consuming the same set of partitions by using a
 * working set of partitions, and keeping track of their progress state during processing of those partitions.
 */
public class ConcurrentPartitionConsumer extends AbstractPartitionConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(ConcurrentPartitionConsumer.class);

  public ConcurrentPartitionConsumer(PartitionedFileSet partitionedFileSet, StatePersistor statePersistor) {
    super(partitionedFileSet, statePersistor);
  }

  public ConcurrentPartitionConsumer(PartitionedFileSet partitionedFileSet, StatePersistor statePersistor,
                                     ConsumerConfiguration configuration) {
    super(partitionedFileSet, statePersistor, configuration);
  }

  @Override
  public PartitionConsumerResult doConsume(ConsumerWorkingSet workingSet, PartitionAcceptor acceptor) {
    doExpiry(workingSet);
    workingSet.populate(getPartitionedFileSet(), getConfiguration());

    long now = System.currentTimeMillis();
    List<PartitionDetail> toConsume = new ArrayList<>();

    List<? extends ConsumablePartition> partitions = workingSet.getPartitions();
    for (ConsumablePartition consumablePartition : partitions) {
      if (ProcessState.AVAILABLE != consumablePartition.getProcessState()) {
        continue;
      }
      PartitionDetail partition = getPartitionedFileSet().getPartition(consumablePartition.getPartitionKey());
      if (partition == null) {
        // no longer exists
        continue;
      }
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

  @Override
  public void doFinish(ConsumerWorkingSet workingSet, List<? extends PartitionKey> partitionKeys, boolean succeeded) {
    doExpiry(workingSet);
    if (succeeded) {
      commit(workingSet, partitionKeys);
    } else {
      abort(workingSet, partitionKeys);
    }
  }

  /**
   * Removes the given partition keys from the working set, as they have been successfully processed.
   */
  protected void commit(ConsumerWorkingSet workingSet, List<? extends PartitionKey> partitionKeys) {
    for (PartitionKey key : partitionKeys) {
      ConsumablePartition consumablePartition = workingSet.lookup(key);
      assertInProgress(consumablePartition);
      workingSet.remove(key);
    }
  }

  /**
   * Resets the process state of the given partition keys, as they were not successfully processed, or discards the
   * partition if it has already been attempted the configured number of attempts.
   */
  protected void abort(ConsumerWorkingSet workingSet, List<? extends PartitionKey> partitionKeys) {
    List<PartitionKey> discardedPartitions = new ArrayList<>();
    for (PartitionKey key : partitionKeys) {
      ConsumablePartition consumablePartition = workingSet.lookup(key);
      assertInProgress(consumablePartition);
      // either reset its processState, or remove it from the workingSet, depending on how many tries it already has
      if (consumablePartition.getNumFailures() < getConfiguration().getMaxRetries()) {
        consumablePartition.retry();
      } else {
        discardedPartitions.add(key);
        workingSet.lookup(key).discard();
      }
    }
    if (!discardedPartitions.isEmpty()) {
      LOG.warn("Discarded keys due to being retried {} times: {}",
               getConfiguration().getMaxRetries(), discardedPartitions);
    }
  }

  /**
   * ensure that caller doesn't try to commit/abort a partition that isn't in progress
   * @throws IllegalArgumentException if the given partition is in progress
   */
  protected void assertInProgress(ConsumablePartition consumablePartition) {
    if (!(consumablePartition.getProcessState() == ProcessState.IN_PROGRESS)) {
      throw new IllegalStateException(String.format("Partition not in progress: %s",
                                                    consumablePartition.getPartitionKey()));
    }
  }

  /**
   * Removes the list of partitions that have failed processing the configured number of times from the working set and
   * returns them.
   */
  protected List<PartitionDetail> removeDiscardedPartitions(ConsumerWorkingSet workingSet) {
    List<PartitionDetail> failedPartitions = new ArrayList<>();
    Iterator<ConsumablePartition> iter = workingSet.getPartitions().iterator();
    while (iter.hasNext()) {
      ConsumablePartition partition = iter.next();
      if (partition.getProcessState() == ProcessState.DISCARDED) {
        failedPartitions.add(getPartitionedFileSet().getPartition(partition.getPartitionKey()));
        iter.remove();
      }
    }
    return failedPartitions;
  }

  /**
   * @return a timestamp which determines partition expiry. Partitions with a timestamp smaller (older) than this value
   * are considered 'expired'.
   */
  protected long getExpiryBorder() {
    long now = System.currentTimeMillis();
    long expirationTimeoutMillis = TimeUnit.SECONDS.toMillis(getConfiguration().getTimeout());
    return now - expirationTimeoutMillis;
  }

  /**
   * Goes through all partitions. If any IN_PROGRESS partition is older than the configured timeout, reset its state
   * to AVAILABLE, unless it has already been retried the configured number of times, in which case it is discarded.
   */
  protected void doExpiry(ConsumerWorkingSet workingSet) {
    long expiryTime = getExpiryBorder();
    List<PartitionKey> expiredPartitions = new ArrayList<>();
    List<PartitionKey> discardedPartitions = new ArrayList<>();
    for (ConsumablePartition partition : workingSet.getPartitions()) {
      if (partition.getProcessState() == ProcessState.IN_PROGRESS && partition.getTimestamp() < expiryTime) {
        // either reset its processState, or remove it from the workingSet, depending on how many tries it already has
        if (partition.getNumFailures() < getConfiguration().getMaxRetries()) {
          partition.retry();
        } else {
          partition.discard();
        }
        expiredPartitions.add(partition.getPartitionKey());
      }
    }
    if (!expiredPartitions.isEmpty()) {
      LOG.warn("Expiring in progress partitions: {}", expiredPartitions);
      if (!discardedPartitions.isEmpty()) {
        LOG.warn("Discarded keys due to being retried {} times: {}",
                 getConfiguration().getMaxRetries(), discardedPartitions);
      }
    }
  }
}
