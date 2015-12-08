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

package co.cask.cdap.api.dataset.lib.partitioned;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.dataset.lib.PartitionDetail;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Configuration parameters to be used by a {@link PartitionConsumer}.
 */
public class ConsumerConfiguration {
  /**
   * Instance of ConsumerConfiguration with default parameters
   */
  public static final ConsumerConfiguration DEFAULT = builder().build();

  private final Predicate<PartitionDetail> partitionPredicate;
  private final int maxWorkingSetSize;
  private final long timeout;
  private final int maxRetries;

  private ConsumerConfiguration(Predicate<PartitionDetail> partitionPredicate,
                                int maxWorkingSetSize, long timeout, int maxRetries) {
    this.partitionPredicate = partitionPredicate;
    this.maxWorkingSetSize = maxWorkingSetSize;
    this.timeout = timeout;
    this.maxRetries = maxRetries;
  }

  /**
   * @return A predicate to be applied on {@link PartitionDetail}s to determine which partitions to include in the
   *         partition consumption.
   */
  public Predicate<PartitionDetail> getPartitionPredicate() {
    return partitionPredicate;
  }

  /**
   * @return An upper bound on the size of the working set of partitions that get serialized as part of the consumer's
   *         state.
   */
  public int getMaxWorkingSetSize() {
    return maxWorkingSetSize;
  }

  /**
   * Defines an expiration timeout, in seconds, of IN_PROGRESS partitions
   * @return number of seconds that a partition can be in progress before it is deemed failed. Once a partition is
   *         deemed failed, it is either marked as AVAILABLE for retry or discarded, depending on the configured number
   *         of retries.
   */
  public long getTimeout() {
    return timeout;
  }

  /**
   * @return The maximum number of retries that a partition can be attempted to be processed. Once a partition has
   *         reached this many failures, it is discarded, rather than marked as AVAILABLE for reprocessing.
   */
  public int getMaxRetries() {
    return maxRetries;
  }

  /**
   * @return a {@link Builder} instance to build an instance of a ConsumerConfiguration.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * A Builder to construct ConsumerConfiguration instances.
   */
  public static class Builder {
    /**
     * Default values
     */
    private Predicate<PartitionDetail> partitionPredicate = new Predicate<PartitionDetail>() {
      @Override
      public boolean apply(@Nullable PartitionDetail input) {
        return true;
      }
    };
    private int maxWorkingSetSize = 1000;
    // 12 hour timeout
    private long timeout = TimeUnit.HOURS.toSeconds(12);
    private int maxRetries = 1;

    private Builder() {
    }

    /**
     * Sets the Predicate of the ConsumerConfiguration. See {@link #getPartitionPredicate()} ()}.
     */
    public Builder setPartitionPredicate(Predicate<PartitionDetail> partitionPredicate) {
      this.partitionPredicate = partitionPredicate;
      return this;
    }

    /**
     * Sets the maximum working set size of the ConsumerConfiguration. See {@link #getMaxWorkingSetSize()}.
     */
    public Builder setMaxWorkingSetSize(int maxWorkingSetSize) {
      this.maxWorkingSetSize = maxWorkingSetSize;
      return this;
    }

    /**
     * Sets the timeout of the ConsumerConfiguration. See {@link #getTimeout()}.
     */
    public Builder setTimeout(long timeout) {
      this.timeout = timeout;
      return this;
    }

    /**
     * Sets the number of retries of the ConsumerConfiguration. See {@link #getMaxRetries()}.
     */
    public Builder setMaxRetries(int maxRetries) {
      this.maxRetries = maxRetries;
      return this;
    }

    /**
     * Create a ConsumerConfiguration from this builder, using the private ConsumerConfiguration
     * constructor.
     */
    public ConsumerConfiguration build() {
      return new ConsumerConfiguration(partitionPredicate, maxWorkingSetSize, timeout, maxRetries);
    }
  }
}
