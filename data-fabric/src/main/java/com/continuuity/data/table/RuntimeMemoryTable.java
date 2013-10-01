package com.continuuity.data.table;

import com.continuuity.data.DataFabric;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.dataset.lib.table.inmemory.InMemoryOcTableClient;
import com.continuuity.data2.dataset.lib.table.inmemory.InMemoryOcTableManager;
import com.google.common.base.Throwables;

/**
 * Base class for runtime implementations of MemoryTable.
 */
public class RuntimeMemoryTable extends RuntimeTable {
  /**
   * Creates instance of Runtime Table. This constructor is called by DataSetInstantiator at runtime only,
   * hence the table name doesn't matter, as it'll get initialized
   * in the {@link #initialize(com.continuuity.api.data.DataSetSpecification, com.continuuity.api.data.DataSetContext)}
   * method.
   */
  public RuntimeMemoryTable(DataFabric dataFabric, String metricName) {
    super(dataFabric, metricName);
  }

  @Override
  protected OrderedColumnarTable getOcTable(DataFabric dataFabric) {
    DataSetManager dataSetManager = dataFabric.getDataSetManager(InMemoryOcTableManager.class);

    try {
      // We want to ensure table is there before creating table client
      if (!dataSetManager.exists(getName())) {
        dataSetManager.create(getName());
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

    return dataFabric.getDataSetClient(getName(), InMemoryOcTableClient.class, null);
  }
}
