/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.hbase.coprocessor;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.tephra.Transaction;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Factory class for providing {@link Filter} instances.
 */
public class TransactionFilters {
  /**
   * Creates a new {@link org.apache.hadoop.hbase.filter.Filter} for returning data only from visible transactions.
   *
   * @param tx the current transaction to apply.  Only data visible to this transaction will be returned.
   * @param ttlByFamily map of time-to-live (TTL) (in milliseconds) by column family name
   * @param allowEmptyValues if {@code true} cells with empty {@code byte[]} values will be returned, if {@code false}
   *                         these will be interpreted as "delete" markers and the column will be filtered out
   * @param scanType the type of scan operation being performed
   */
  public static Filter getVisibilityFilter(Transaction tx, Map<byte[], Long> ttlByFamily, boolean allowEmptyValues,
                                           ScanType scanType) {
    return new CellSkipFilter(new TransactionVisibilityFilter(tx, ttlByFamily, allowEmptyValues, false,
                                                              scanType, null));
  }

  /**
   * Creates a new {@link org.apache.hadoop.hbase.filter.Filter} for returning data only from visible transactions.
   *
   * @param tx the current transaction to apply.  Only data visible to this transaction will be returned.
   * @param ttlByFamily map of time-to-live (TTL) (in milliseconds) by column family name
   * @param allowEmptyValues if {@code true} cells with empty {@code byte[]} values will be returned, if {@code false}
   *                         these will be interpreted as "delete" markers and the column will be filtered out
   * @param readNonTxnData whether data written before Tephra was enabled on a table should be readable
   * @param scanType the type of scan operation being performed
   * @param cellFilter if non-null, this filter will be applied to all cells visible to the current transaction, by
   *                   calling {@link Filter#filterKeyValue(org.apache.hadoop.hbase.Cell)}.  If null, then
   *                   {@link Filter.ReturnCode#INCLUDE_AND_NEXT_COL} will be returned instead.
   */
  public static Filter getVisibilityFilter(Transaction tx, Map<byte[], Long> ttlByFamily, boolean allowEmptyValues,
                                           boolean readNonTxnData, ScanType scanType, @Nullable Filter cellFilter) {
    return new CellSkipFilter(new TransactionVisibilityFilter(tx, ttlByFamily, allowEmptyValues, readNonTxnData,
                                                              scanType, cellFilter));
  }
}
