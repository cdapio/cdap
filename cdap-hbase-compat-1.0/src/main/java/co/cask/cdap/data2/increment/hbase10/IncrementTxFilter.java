/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.data2.increment.hbase10;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.tephra.Transaction;
import org.apache.tephra.hbase.coprocessor.TransactionVisibilityFilter;

import java.util.Map;

/**
 * {@link TransactionVisibilityFilter}'s default behavior is to give only latest valid version for the transactional
 * cell to its sub-filters. However the {@link IncrementFilter} need to see all previous valid versions for readless
 * increments, since increments are stored as just the different versions of the same cell. This {@link Filter}
 * extends the {@link TransactionVisibilityFilter} and overrides the
 * {@link TransactionVisibilityFilter#determineReturnCode} method to achieve this.
 */
public class IncrementTxFilter extends TransactionVisibilityFilter {
  /**
   * Creates a new instance of the {@link Filter}.
   * @param tx the current transaction to apply. Only data visible to this transaction will be returned
   * @param ttlByFamily map of time-to-live (TTL) (in milliseconds) by column family name
   * @param allowEmptyValues if {@code true} cells with empty {@code byte[]} values will be returned, if {@code false}
   *                         these will be interpreted as "delete" markers and the column will be filtered out
   * @param readNonTxnData whether data written before Tephra was enabled on a table should be readable
   * @param scanType the type of scan operation being performed
   * @param cellFilter if non-null, this filter will be applied to all cells visible to the current transaction, by
   *                   calling {@link Filter#filterKeyValue(org.apache.hadoop.hbase.Cell)}.  If null, then
   *                   {@link Filter.ReturnCode#INCLUDE_AND_NEXT_COL} will be returned instead.
   */
  public IncrementTxFilter(Transaction tx, Map<byte[], Long> ttlByFamily, boolean allowEmptyValues,
                           boolean readNonTxnData, ScanType scanType, Filter cellFilter) {
    super(tx, ttlByFamily, allowEmptyValues, readNonTxnData, scanType,
          Filters.combine(new IncrementFilter(), cellFilter));
  }

  @Override
  protected Filter.ReturnCode determineReturnCode(Filter.ReturnCode txFilterCode, Filter.ReturnCode subFilterCode) {
    switch (subFilterCode) {
      case INCLUDE:
        return Filter.ReturnCode.INCLUDE;
      case INCLUDE_AND_NEXT_COL:
        return Filter.ReturnCode.INCLUDE_AND_NEXT_COL;
      case SKIP:
        return txFilterCode == Filter.ReturnCode.INCLUDE ? Filter.ReturnCode.SKIP : Filter.ReturnCode.NEXT_COL;
      default:
        return subFilterCode;
    }
  }
}
