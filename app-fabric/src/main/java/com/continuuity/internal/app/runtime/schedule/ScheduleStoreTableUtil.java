package com.continuuity.internal.app.runtime.schedule;

import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.google.inject.Inject;

import java.util.Properties;

/**
 * Helper class for working with the dataset table used by
 * {@link com.continuuity.internal.app.runtime.schedule.DataSetBasedScheduleStore}.
 */
public class ScheduleStoreTableUtil {
  public static final String SCHEDULE_STORE_DATASET_NAME = "schedulestore";

  private final DataSetAccessor dataSetAccessor;

  @Inject
  public ScheduleStoreTableUtil(DataSetAccessor dataSetAccessor) {
    this.dataSetAccessor = dataSetAccessor;
  }

  private void createDatasetIfNotExists() throws Exception {
    DataSetManager dataSetManager = dataSetAccessor.getDataSetManager(OrderedColumnarTable.class,
                                                                      DataSetAccessor.Namespace.SYSTEM);
    if (!dataSetManager.exists(SCHEDULE_STORE_DATASET_NAME)) {
      dataSetManager.create(SCHEDULE_STORE_DATASET_NAME);
    }
  }

  public OrderedColumnarTable getTable() throws Exception {
    createDatasetIfNotExists();
    return dataSetAccessor.getDataSetClient(SCHEDULE_STORE_DATASET_NAME,
                                            OrderedColumnarTable.class,
                                            DataSetAccessor.Namespace.SYSTEM);

  }

  public void upgrade() throws Exception {
    DataSetManager dsManager = dataSetAccessor.getDataSetManager(OrderedColumnarTable.class,
                                                                 DataSetAccessor.Namespace.SYSTEM);
    dsManager.upgrade(SCHEDULE_STORE_DATASET_NAME, new Properties());
  }
}
