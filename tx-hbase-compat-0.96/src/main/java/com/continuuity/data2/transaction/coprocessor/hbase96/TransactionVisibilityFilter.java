package com.continuuity.data2.transaction.coprocessor.hbase96;

import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TxConstants;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class TransactionVisibilityFilter extends FilterBase {
  private final Transaction tx;
  // oldest visible timestamp by column family, used to apply TTL when reading
  private final Map<byte[], Long> oldestTsByFamily;

  // since we traverse KVs in order, cache the current oldest TS to avoid map lookups per KV
  private byte[] currentFamily = new byte[0];
  private long currentOldestTs;

  public TransactionVisibilityFilter(Transaction tx, Map<byte[], Long> ttlByFamily) {
    this.tx = tx;
    this.oldestTsByFamily = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], Long> ttlEntry : ttlByFamily.entrySet()) {
      long familyTTL = ttlEntry.getValue();
      oldestTsByFamily.put(ttlEntry.getKey(),
                           familyTTL <= 0 ? 0 : tx.getVisibilityUpperBound() - familyTTL * TxConstants.MAX_TX_PER_MS);
    }
  }

  @Override
  public ReturnCode filterKeyValue(Cell cell) {
    if (!CellUtil.matchingFamily(cell, currentFamily)) {
      // column family changed
      currentFamily = CellUtil.cloneFamily(cell);
      Long familyOldestTs = oldestTsByFamily.get(currentFamily);
      currentOldestTs = familyOldestTs != null ? familyOldestTs : 0;
    }
    // need to apply TTL for the column family here
    long kvTimestamp = cell.getTimestamp();
    if (kvTimestamp < currentOldestTs) {
      // passed TTL for this column, seek to next
      return ReturnCode.NEXT_COL;
    } else if (tx.isVisible(kvTimestamp)) {
      // as soon as we find a KV to include we can move to the next column
      return ReturnCode.INCLUDE_AND_NEXT_COL;
    } else {
      return ReturnCode.SKIP;
    }
  }

  @Override
  public byte[] toByteArray() throws IOException {
    return super.toByteArray();
  }
}
