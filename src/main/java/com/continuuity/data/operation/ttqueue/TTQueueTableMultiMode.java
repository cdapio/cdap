package com.continuuity.data.operation.ttqueue;

import org.apache.hadoop.conf.Configuration;

import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.table.VersionedColumnarTable;

/**
 * A table of {@link TTQueue}s that supports multiple modes of operation for
 * different types of queues.
 * 
 * Currently, two types of queues are supported:
 * 
 * <ol>
 *  <li>Stream Queues, which are currently 
 * @author jgray
 *
 */
public class TTQueueTableMultiMode extends TTQueueTableOnVCTable {

  public TTQueueTableMultiMode(VersionedColumnarTable table,
      TimestampOracle timeOracle, Configuration conf) {
    super(table, timeOracle, conf);
    // TODO Auto-generated constructor stub
  }

}
