package com.continuuity.data.operation;

import com.continuuity.api.data.BatchCollectionClient;
import com.continuuity.api.data.BatchCollector;

/**
 * Simplest possible implementation of a BatchCollectionClient
 */
public class SimpleBatchCollectionClient implements BatchCollectionClient {

  // the current batch collector
  BatchCollector collector = null;

  @Override
  public void setCollector(BatchCollector collector) {
    this.collector = collector;
  }

  @Override
  public BatchCollector getCollector() {
    return this.collector;
  }
}
