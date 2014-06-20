package com.continuuity.data2.dataset.lib.table;

import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.dataset.api.DataSetManager;

import java.util.Properties;

/**
 * Common utility for managing system metadata tables needed by various services.
 */
public abstract class MetaTableUtil {

  protected final DataSetAccessor dataSetAccessor;

  public MetaTableUtil(DataSetAccessor dataSetAccessor) {
    this.dataSetAccessor = dataSetAccessor;
  }

  public OrderedColumnarTable getMetaTable() throws Exception {
    DataSetManager dsManager = dataSetAccessor.getDataSetManager(OrderedColumnarTable.class,
                                                                 DataSetAccessor.Namespace.SYSTEM);
    String tableName = getMetaTableName();
    if (!dsManager.exists(tableName)) {
      dsManager.create(tableName);
    }

    return dataSetAccessor.getDataSetClient(tableName,
                                            OrderedColumnarTable.class, DataSetAccessor.Namespace.SYSTEM);
  }

  public void upgrade() throws Exception {
    DataSetManager dsManager = dataSetAccessor.getDataSetManager(OrderedColumnarTable.class,
                                                                 DataSetAccessor.Namespace.SYSTEM);
    dsManager.upgrade(getMetaTableName(), new Properties());
  }

  /**
   * Returns the name of the metadata table to be used.
   */
  public abstract String getMetaTableName();
}
