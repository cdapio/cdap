package com.continuuity.payvment.lib;

import com.continuuity.api.data.BatchCollectionClient;
import com.continuuity.api.data.BatchCollector;
import com.continuuity.api.data.DataFabric;
import com.continuuity.api.flow.flowlet.FlowletContext;

public abstract class DataLib implements BatchCollectionClient {

  protected final String tableName;

  protected final FlowletContext context;

  protected final DataFabric fabric;

  protected BatchCollector collector;

  protected DataLib(String tableName, FlowletContext context) {
    this.tableName = tableName;
    this.context = context;
    this.fabric = context.getDataFabric();
    context.register(this);
  }

  @Override
  public final void setCollector(BatchCollector collector) {
    this.collector = collector;
  }

  public final BatchCollector getCollector() {
    return this.collector;
  }

  public final FlowletContext getContext() {
    return this.context;
  }

  public final DataFabric getDataFabric() {
    return this.fabric;
  }

  public final String getTableName() {
    return this.tableName;
  }
}
