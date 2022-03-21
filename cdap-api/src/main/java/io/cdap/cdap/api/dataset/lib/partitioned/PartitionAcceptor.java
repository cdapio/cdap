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

package io.cdap.cdap.api.dataset.lib.partitioned;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.dataset.lib.PartitionDetail;

/**
 * Defines whether to accept {@link PartitionDetail}s, while iterating over a collection of them.
 */
@Beta
public interface PartitionAcceptor {
  /**
   * Return value, determining what to do with a Partition.
   */
  enum Return {
    /**
     * Accept the partition.
     */
    ACCEPT,
    /**
     * Skip over the partition. The skipped partition will remain as-is.
     */
    SKIP,
    /**
     * Will not include the partition. Additionally, the iteration over the partitions will end.
     */
    STOP
  }

  Return accept(PartitionDetail partitionDetail);


  /**
   * An implementation of PartitionAcceptor which limits the number of accepted partitions to a given value.
   */
  final class Limit implements PartitionAcceptor {

    private int count;
    private int limit;

    public Limit(int limit) {
      this.limit = limit;
    }

    @Override
    public Return accept(PartitionDetail partitionDetail) {
      if (count++ < limit) {
        return Return.ACCEPT;
      }
      return Return.STOP;
    }
  }
}
