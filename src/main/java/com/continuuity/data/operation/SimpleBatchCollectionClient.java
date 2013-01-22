package com.continuuity.data.operation;

import com.continuuity.api.data.BatchCollectionClient;
import com.continuuity.api.data.BatchCollector;

/**
 * helper class for the prupose of demonstration. flow can define its own
 * implementation of this, using its output collector.
 */
public class SimpleBatchCollectionClient implements BatchCollectionClient {

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
