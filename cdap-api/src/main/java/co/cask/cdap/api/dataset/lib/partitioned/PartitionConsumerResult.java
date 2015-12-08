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

import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionKey;

import java.util.List;

/**
 * Returns access to a list of partitions for consuming, as well as a list of {@link PartitionKey}s corresponding
 * to partitions that have previously failed processing the configured number of tries.
 */
public class PartitionConsumerResult {
  private final List<PartitionDetail> partitions;
  private final List<PartitionDetail> failedPartitions;

  public PartitionConsumerResult(List<PartitionDetail> partitions, List<PartitionDetail> failedPartitions) {
    this.partitions = partitions;
    this.failedPartitions = failedPartitions;
  }

  /**
   * @return a list of {@link PartitionDetail}s, available for consuming.
   */
  public List<PartitionDetail> getPartitions() {
    return partitions;
  }

  /**
   * @return a list of {@link PartitionDetail}s, corresponding to partitions that have failed processing the configured
   *         number of times.
   */
  public List<PartitionDetail> getFailedPartitions() {
    return failedPartitions;
  }
}
