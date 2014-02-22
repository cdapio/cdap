package com.continuuity.data2.transaction;

/**
 * Transaction system constants
 */
public class TxConstants {
  /**
   * property set for {@link org.apache.hadoop.hbase.HColumnDescriptor}
   */
  public static final String PROPERTY_TTL = "dataset.table.ttl";

  /**
   * This is how many tx we allow per millisecond, if you care about the system for 100 years:
   * Long.MAX_VALUE / (System.currentTimeMillis() + TimeUnit.DAYS.toMillis(365 * 100)) =
   * (as of Feb 20, 2014) 2,028,653. It is safe and convenient to use 1,000,000 as multiplier:
   * <ul>
   *   <li>
   *     we hardly can do more than 1 billion txs per second
   *   </li>
   *   <li>
   *     long value will not overflow for 200 years
   *   </li>
   *   <li>
   *     makes reading & debugging easier if multiplier is 10^n
   *   </li>
   * </ul>
   */
  public static final long MAX_TX_PER_MS = 1000000;
}
