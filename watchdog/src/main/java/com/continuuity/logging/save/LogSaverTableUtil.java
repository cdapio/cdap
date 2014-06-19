package com.continuuity.logging.save;

import com.continuuity.data.DataSetAccessor;
import com.continuuity.logging.LoggingConfiguration;
import com.google.inject.Inject;

/**
 * Helper class for working with the dataset table used by {@link LogSaver}.
 */
public class LogSaverTableUtil extends com.continuuity.data2.dataset.lib.table.MetaTableUtil {
  private static final String TABLE_NAME = LoggingConfiguration.LOG_META_DATA_TABLE;

  @Inject
  public LogSaverTableUtil(DataSetAccessor dataSetAccessor) {
    super(dataSetAccessor);
  }

  @Override
  public String getMetaTableName() {
    return TABLE_NAME;
  }
}
