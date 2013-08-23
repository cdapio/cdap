/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import java.io.IOException;

/**
 *
 */
public interface HBaseQueueEvictionProtocol extends CoprocessorProtocol {

  /**
   * Evicts queue entries.
   * @param scan The scan object for scanning table. The assumption is the scan object should be only
   *             giving KeyValue of the state columns only.
   * @param readPointer
   * @param smallestExclude
   * @param numGroups
   * @return
   * @throws IOException
   */
  int evict(Scan scan, long readPointer, long smallestExclude, int numGroups) throws IOException;
}
