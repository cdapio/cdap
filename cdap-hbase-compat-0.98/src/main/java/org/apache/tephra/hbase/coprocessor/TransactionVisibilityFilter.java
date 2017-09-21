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

import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tephra.Transaction;
import org.apache.tephra.TxConstants;
import org.apache.tephra.util.TxUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Applies filtering of data based on transactional visibility (HBase 0.98+ specific version).
 * Note: this is intended for server-side use only, as additional properties need to be set on
 * any {@code Scan} or {@code Get} operation performed.
 */
public class TransactionVisibilityFilter extends FilterBase {
  private final Transaction tx;
  // oldest visible timestamp by column family, used to apply TTL when reading
  private final Map<byte[], Long> oldestTsByFamily;
  // if false, empty values will be interpreted as deletes
  private final boolean allowEmptyValues;
  // whether or not we can remove delete markers
  // these can only be safely removed when we are traversing all storefiles
  private final boolean clearDeletes;
  // optional sub-filter to apply to visible cells
  private final Filter cellFilter;

  // since we traverse KVs in order, cache the current oldest TS to avoid map lookups per KV
  private byte[] currentFamily = new byte[0];
  private long currentOldestTs;

  private DeleteTracker deleteTracker = new DeleteTracker();

  /**
   * Creates a new {@link org.apache.hadoop.hbase.filter.Filter} for returning data only from visible transactions.
   *
   * @param tx the current transaction to apply.  Only data visible to this transaction will be returned.
   * @param ttlByFamily map of time-to-live (TTL) (in milliseconds) by column family name
   * @param allowEmptyValues if {@code true} cells with empty {@code byte[]} values will be returned, if {@code false}
   *                         these will be interpreted as "delete" markers and the column will be filtered out
   * @param scanType the type of scan operation being performed
   */
  public TransactionVisibilityFilter(Transaction tx, Map<byte[], Long> ttlByFamily, boolean allowEmptyValues,
                                     ScanType scanType) {
    this(tx, ttlByFamily, allowEmptyValues, false, scanType, null);
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
  public TransactionVisibilityFilter(Transaction tx, Map<byte[], Long> ttlByFamily, boolean allowEmptyValues,
                                     boolean readNonTxnData, ScanType scanType, @Nullable Filter cellFilter) {
    this.tx = tx;
    this.oldestTsByFamily = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], Long> ttlEntry : ttlByFamily.entrySet()) {
      long familyTTL = ttlEntry.getValue();
      oldestTsByFamily.put(ttlEntry.getKey(),
                           TxUtils.getOldestVisibleTimestamp(familyTTL, tx, readNonTxnData));
    }
    this.allowEmptyValues = allowEmptyValues;
    this.clearDeletes =
      scanType == ScanType.COMPACT_DROP_DELETES ||
        (scanType == ScanType.USER_SCAN && tx.getVisibilityLevel() != Transaction.VisibilityLevel.SNAPSHOT_ALL);
    this.cellFilter = cellFilter;
  }

  @Override
  public ReturnCode filterKeyValue(Cell cell) throws IOException {
    if (!CellUtil.matchingFamily(cell, currentFamily)) {
      // column family changed
      currentFamily = CellUtil.cloneFamily(cell);
      Long familyOldestTs = oldestTsByFamily.get(currentFamily);
      currentOldestTs = familyOldestTs != null ? familyOldestTs : 0;
      deleteTracker.reset();
    }
    // need to apply TTL for the column family here
    long kvTimestamp = cell.getTimestamp();
    if (TxUtils.getTimestampForTTL(kvTimestamp) < currentOldestTs) {
      // passed TTL for this column, seek to next
      return ReturnCode.NEXT_COL;
    } else if (tx.isVisible(kvTimestamp)) {
      // Return all writes done by current transaction (including deletes) for VisibilityLevel.SNAPSHOT_ALL
      if (tx.getVisibilityLevel() == Transaction.VisibilityLevel.SNAPSHOT_ALL && tx.isCurrentWrite(kvTimestamp)) {
        // cell is visible
        // visibility SNAPSHOT_ALL needs all matches
        return runSubFilter(ReturnCode.INCLUDE, cell);
      }
      if (DeleteTracker.isFamilyDelete(cell)) {
        deleteTracker.addFamilyDelete(cell);
        if (clearDeletes) {
          return ReturnCode.NEXT_COL;
        } else {
          // cell is visible
          // as soon as we find a KV to include we can move to the next column
          return runSubFilter(ReturnCode.INCLUDE_AND_NEXT_COL, cell);
        }
      }
      // check if masked by family delete
      if (deleteTracker.isDeleted(cell)) {
        return ReturnCode.NEXT_COL;
      }
      // check for column delete
      if (isColumnDelete(cell)) {
        if (clearDeletes) {
          // skip "deleted" cell
          return ReturnCode.NEXT_COL;
        } else {
          // keep the marker but skip any remaining versions
          return runSubFilter(ReturnCode.INCLUDE_AND_NEXT_COL, cell);
        }
      }
      // cell is visible
      // as soon as we find a KV to include we can move to the next column
      return runSubFilter(ReturnCode.INCLUDE_AND_NEXT_COL, cell);
    } else {
      return ReturnCode.SKIP;
    }
  }

  private ReturnCode runSubFilter(ReturnCode txFilterCode, Cell cell) throws IOException {
    if (cellFilter != null) {
      ReturnCode subFilterCode = cellFilter.filterKeyValue(cell);
      return determineReturnCode(txFilterCode, subFilterCode);
    }
    return txFilterCode;
  }

  /**
   * Determines the return code of TransactionVisibilityFilter based on sub-filter's return code.
   * Sub-filter can only exclude cells included by TransactionVisibilityFilter, i.e., sub-filter's
   * INCLUDE will be ignored. This behavior makes sure that sub-filter only sees cell versions valid for the
   * given transaction. If sub-filter needs to see older versions of cell, then this method can be overridden.
   *
   * @param txFilterCode return code from TransactionVisibilityFilter
   * @param subFilterCode return code from sub-filter
   * @return final return code
   */
  protected ReturnCode determineReturnCode(ReturnCode txFilterCode, ReturnCode subFilterCode) {
    // Return the more restrictive of the two filter responses
    switch (subFilterCode) {
      case INCLUDE:
        return txFilterCode;
      case INCLUDE_AND_NEXT_COL:
        return ReturnCode.INCLUDE_AND_NEXT_COL;
      case SKIP:
        return txFilterCode == ReturnCode.INCLUDE ? ReturnCode.SKIP : ReturnCode.NEXT_COL;
      default:
        return subFilterCode;
    }
  }

  @Override
  public boolean filterRow() throws IOException {
    if (cellFilter != null) {
      return cellFilter.filterRow();
    }
    return super.filterRow();
  }

  @Override
  public Cell transformCell(Cell cell) throws IOException {
    // Convert Tephra deletes back into HBase deletes
    if (tx.getVisibilityLevel() == Transaction.VisibilityLevel.SNAPSHOT_ALL) {
      if (DeleteTracker.isFamilyDelete(cell)) {
        return new KeyValue(CellUtil.cloneRow(cell), CellUtil.cloneFamily(cell), null, cell.getTimestamp(),
                            KeyValue.Type.DeleteFamily);
      } else if (isColumnDelete(cell)) {
        // Note: in some cases KeyValue.Type.Delete is used in Delete object,
        // and in some other cases KeyValue.Type.DeleteColumn is used.
        // Since Tephra cannot distinguish between the two, we return KeyValue.Type.DeleteColumn.
        // KeyValue.Type.DeleteColumn makes both CellUtil.isDelete and CellUtil.isDeleteColumns return true, and will
        // work in both cases.
        return new KeyValue(CellUtil.cloneRow(cell), CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell),
                            cell.getTimestamp(), KeyValue.Type.DeleteColumn);
      }
    }
    return cell;
  }

  @Override
  public void reset() throws IOException {
    deleteTracker.reset();
    if (cellFilter != null) {
      cellFilter.reset();
    }
  }

  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) throws IOException {
    if (cellFilter != null) {
      return cellFilter.filterRowKey(buffer, offset, length);
    }
    return super.filterRowKey(buffer, offset, length);
  }

  @Override
  public boolean filterAllRemaining() throws IOException {
    if (cellFilter != null) {
      return cellFilter.filterAllRemaining();
    }
    return super.filterAllRemaining();
  }

  @Override
  public void filterRowCells(List<Cell> kvs) throws IOException {
    if (cellFilter != null) {
      cellFilter.filterRowCells(kvs);
    } else {
      super.filterRowCells(kvs);
    }
  }

  @Override
  public boolean hasFilterRow() {
    if (cellFilter != null) {
      return cellFilter.hasFilterRow();
    }
    return super.hasFilterRow();
  }

  @SuppressWarnings("deprecation")
  @Override
  public KeyValue getNextKeyHint(KeyValue currentKV) throws IOException {
    if (cellFilter != null) {
      return cellFilter.getNextKeyHint(currentKV);
    }
    return super.getNextKeyHint(currentKV);
  }

  @Override
  public Cell getNextCellHint(Cell currentKV) throws IOException {
    if (cellFilter != null) {
      return cellFilter.getNextCellHint(currentKV);
    }
    return super.getNextCellHint(currentKV);
  }

  @Override
  public boolean isFamilyEssential(byte[] name) throws IOException {
    if (cellFilter != null) {
      return cellFilter.isFamilyEssential(name);
    }
    return super.isFamilyEssential(name);
  }

  @Override
  public byte[] toByteArray() throws IOException {
    return super.toByteArray();
  }

  private boolean isColumnDelete(Cell cell) {
    return !TxUtils.isPreExistingVersion(cell.getTimestamp()) && cell.getValueLength() == 0 && !allowEmptyValues;
  }

  private static final class DeleteTracker {
    private long familyDeleteTs;
    private byte[] rowKey;

    public static boolean isFamilyDelete(Cell cell) {
      return !TxUtils.isPreExistingVersion(cell.getTimestamp()) &&
        CellUtil.matchingQualifier(cell, TxConstants.FAMILY_DELETE_QUALIFIER) &&
        CellUtil.matchingValue(cell, HConstants.EMPTY_BYTE_ARRAY);
    }

    public void addFamilyDelete(Cell delete) {
      this.familyDeleteTs = delete.getTimestamp();
      this.rowKey = Bytes.copy(delete.getRowArray(), delete.getRowOffset(), delete.getRowLength());
    }

    public boolean isDeleted(Cell cell) {
      return rowKey != null && Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), 
        cell.getRowLength(), rowKey, 0, rowKey.length) == 0 && cell.getTimestamp() <= familyDeleteTs;
    }

    public void reset() {
      this.familyDeleteTs = 0;
      this.rowKey = null;
    }
  }
}
