package com.continuuity.data.dataset;

import com.continuuity.api.data.BatchCollectionClient;
import com.continuuity.api.data.BatchCollector;

public class SimpleBatchCollectionClient implements BatchCollectionClient {

  BatchCollector collector = null;

  @Override
  public void setCollector(BatchCollector collector) {
    this.collector = collector;
  }

  BatchCollector getCollector() {
    return this.collector;
  }
}
