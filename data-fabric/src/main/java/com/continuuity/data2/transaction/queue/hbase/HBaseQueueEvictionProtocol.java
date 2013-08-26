/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.hbase;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import java.io.IOException;

/**
 *
 */
public interface HBaseQueueEvictionProtocol extends CoprocessorProtocol {

  /**
   * Evicts queue entries.
   * @param scan The scan object for scanning table.
   * @param readPointer
   * @param excludes Sorted list of excluded pointers
   * @param numGroups
   * @return
   * @throws IOException
   */
  int evict(Scan scan, long readPointer, long[] excludes, int numGroups) throws IOException;
}
