package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.data2.util.hbase.HBaseVersionSpecificFactory;

/**
 * Factory for HBase version-specific instances of {@link HBaseQueueUtil}.
 */
public class HBaseQueueUtilFactory extends HBaseVersionSpecificFactory<HBaseQueueUtil> {
  @Override
  protected String getHBase94Classname() {
    return "com.continuuity.data2.transaction.queue.hbase.HBase94QueueUtil";
  }

  @Override
  protected String getHBase96Classname() {
    return "com.continuuity.data2.transaction.queue.hbase.HBase96QueueUtil";
  }
}
