/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.hbase.coprocessor;

import java.util.Map;

/**
 * Holder class for queue consumer configuration information
 */
final class QueueConsumerConfig {
  // Map from consumer instance to
  private final Map<ConsumerInstance, byte[]> startRows;
  private final int numGroups;

  QueueConsumerConfig(Map<ConsumerInstance, byte[]> startRows, int numGroups) {
    this.startRows = startRows;
    this.numGroups = numGroups;
  }

  byte[] getStartRow(ConsumerInstance consumerInstance) {
    return startRows.get(consumerInstance);
  }

  int getNumGroups() {
    return numGroups;
  }
}
