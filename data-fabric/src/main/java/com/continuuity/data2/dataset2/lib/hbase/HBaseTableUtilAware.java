package com.continuuity.data2.dataset2.lib.hbase;

import com.continuuity.data2.util.hbase.HBaseTableUtil;

/**
 * Implementation of this interface needs to access HBase thru {@link HBaseTableUtil}
 */
public interface HBaseTableUtilAware {
  /**
   * Sets {@link HBaseTableUtil}
   * @param util {@link HBaseTableUtil} to set
   */
  void setHBaseTableUtil(HBaseTableUtil util);
}
