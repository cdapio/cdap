package com.continuuity.data2.transaction.util;

import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TxConstants;

import java.util.Map;

/**
 * Utility methods supporting transaction operations.
 */
public class TxUtils {

  /**
   * Returns the oldest visible timestamp for the given transaction, based on the TTLs configured for each column
   * family.  If no TTL is set on any column family, the oldest visible timestamp will be {@code 0}.
   * @param ttlByFamily A map of column family name to TTL value (in milliseconds)
   * @param tx The current transaction
   * @return The oldest timestamp that will be visible for the given transaction and TTL configuration
   */
  public static long getOldestTsVisible(Map<byte[], Long> ttlByFamily, Transaction tx) {
    long oldestVisible = tx.getVisibilityUpperBound();
    // we know that data will not be cleaned up while this tx is running up to this point as janitor uses it
    for (Long familyTTL : ttlByFamily.values()) {
      oldestVisible =
        Math.min(familyTTL <= 0 ? 0 : tx.getVisibilityUpperBound() - familyTTL * TxConstants.MAX_TX_PER_MS,
                 oldestVisible);
    }
    return oldestVisible;
  }

  /**
   * Returns the maximum timestamp to use for time-range operations, based on the given transaction.
   * @param tx The current transaction
   * @return The maximum timestamp (exclusive) to use for time-range operations
   */
  public static long getMaxStamp(Transaction tx) {
    // NOTE: +1 here because we want read up to readpointer inclusive, but timerange's end is exclusive
    return tx.getReadPointer() + 1;
  }
}
