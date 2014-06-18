package com.continuuity.logging.save;

import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.logging.LoggingConfiguration;
import com.google.inject.Inject;

import java.util.Properties;

/**
 * Helper class for working with the dataset table used by {@link LogSaver}.
 */
public class LogSaverTableUtil {
  private static final String TABLE_NAME = LoggingConfiguration.LOG_META_DATA_TABLE;

  private final DataSetAccessor dataSetAccessor;

  @Inject
  public LogSaverTableUtil(DataSetAccessor dataSetAccessor) {
    this.dataSetAccessor = dataSetAccessor;
  }

  public OrderedColumnarTable getMetaTable() throws Exception {
    DataSetManager dsManager = dataSetAccessor.getDataSetManager(OrderedColumnarTable.class,
                                                                 DataSetAccessor.Namespace.SYSTEM);
    if (!dsManager.exists(TABLE_NAME)) {
      dsManager.create(TABLE_NAME);
    }

    return dataSetAccessor.getDataSetClient(TABLE_NAME, OrderedColumnarTable.class, DataSetAccessor.Namespace.SYSTEM);
  }

  public void upgrade() throws Exception {
    DataSetManager dsManager = dataSetAccessor.getDataSetManager(OrderedColumnarTable.class,
                                                                 DataSetAccessor.Namespace.SYSTEM);
    dsManager.upgrade(TABLE_NAME, new Properties());
  }
}
