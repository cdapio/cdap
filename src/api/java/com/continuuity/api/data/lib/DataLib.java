package com.continuuity.api.data.lib;

import com.continuuity.api.data.BatchCollectionClient;
import com.continuuity.api.data.BatchCollectionRegistry;
import com.continuuity.api.data.BatchCollector;
import com.continuuity.api.data.DataFabric;

public abstract class DataLib implements BatchCollectionClient {

  protected final String tableName;

  protected final DataFabric fabric;

  protected BatchCollector collector;

  protected DataLib(String tableName, DataFabric fabric,
      BatchCollectionRegistry registry) {
    this.tableName = tableName;
    this.fabric = fabric;
    if (registry != null) registry.register(this);
  }

  @Override
  public final void setCollector(BatchCollector collector) {
    this.collector = collector;
  }

  public final BatchCollector getCollector() {
    return this.collector;
  }

  public final DataFabric getDataFabric() {
    return this.fabric;
  }

  public final String getTableName() {
    return this.tableName;
  }
}
