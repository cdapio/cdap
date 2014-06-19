package com.continuuity.internal.app.runtime.schedule;

import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.dataset.lib.table.MetaTableUtil;
import com.google.inject.Inject;

/**
 * Helper class for working with the dataset table used by
 * {@link com.continuuity.internal.app.runtime.schedule.DataSetBasedScheduleStore}.
 */
public class ScheduleStoreTableUtil extends MetaTableUtil {
  public static final String SCHEDULE_STORE_DATASET_NAME = "schedulestore";

  @Inject
  public ScheduleStoreTableUtil(DataSetAccessor dataSetAccessor) {
    super(dataSetAccessor);
  }

  @Override
  public String getMetaTableName() {
    return SCHEDULE_STORE_DATASET_NAME;
  }
}
