/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tephra.txprune;

import com.google.common.annotations.Beta;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Interface to manage the invalid transaction list.
 *
 * <p/>
 * An invalid transaction can only be removed from the invalid list after the data written
 * by the invalid transactions has been removed from all the data stores.
 * The term data store is used here to represent a set of tables in a database that have
 * the same data clean up policy, like all Apache Phoenix tables in an HBase instance.
 *
 * <p/>
 * Typically every data store will have a background job which cleans up the data written by invalid transactions.
 * Prune upper bound for a data store is defined as the largest invalid transaction whose data has been
 * cleaned up from that data store.
 *
 * <p/>
 * There will be one such plugin per data store. The plugins will be executed as part of the Transaction Service.
 * Each plugin will be invoked periodically to fetch the prune upper bound for its data store.
 * Invalid transaction list can pruned up to the minimum of prune upper bounds returned by all the plugins.
 */
@Beta
public interface TransactionPruningPlugin {
  /**
   * Called once when the Transaction Service starts up.
   *
   * @param conf configuration for the plugin
   */
  void initialize(Configuration conf) throws IOException;

  /**
   * Called periodically to fetch prune upper bound for a data store. The plugin examines the state of data cleanup
   * in the data store, and determines an upper bound for invalid transactions such that any invalid transaction
   * smaller than or equal to this upper bound is guaranteed to have all its writes removed from the data store.
   * It then returns this upper bound as the prune upper bound for this data store.
   *
   * @param time start time of this prune iteration in milliseconds
   * @param inactiveTransactionBound the largest invalid transaction that can be possibly removed
   *                                 from the invalid list for the given time. This is an upper bound determined
   *                                 by the Transaction Service, based on its knowledge of in-progress and invalid
   *                                 transactions that may still have active processes and therefore future writes.
   *                                 The plugin will typically return a reduced upper bound based on the state of
   *                                 the invalid transaction data clean up in the data store.
   * @return prune upper bound for the data store
   */
  long fetchPruneUpperBound(long time, long inactiveTransactionBound) throws IOException;

  /**
   * Called after successfully pruning the invalid list using the prune upper bound returned by
   * {@link #fetchPruneUpperBound(long, long)}.
   * The largest invalid transaction that was removed from the invalid list is passed as a parameter in this call.
   * The plugin can use this information to clean up its state.
   *
   * @param time start time of this prune iteration in milliseconds (same value as passed to
   *             {@link #fetchPruneUpperBound(long, long)} in the same run)
   * @param maxPrunedInvalid the largest invalid transaction that was removed from the invalid list
   */
  void pruneComplete(long time, long maxPrunedInvalid) throws IOException;

  /**
   * Called once during the shutdown of the Transaction Service.
   */
  void destroy();
}
