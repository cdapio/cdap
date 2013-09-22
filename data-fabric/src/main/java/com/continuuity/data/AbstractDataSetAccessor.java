package com.continuuity.data;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.ConflictDetection;
import com.continuuity.data2.dataset.lib.table.MetricsTable;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * TODO: Having this class is a bad design: we want to add more dataset types to a system without changing core code.
 *       We need to review that during creating dataset management service
 */
public abstract class AbstractDataSetAccessor extends NamespacingDataSetAccessor {
  protected AbstractDataSetAccessor(CConfiguration conf) {
    super(conf);
  }

  protected abstract <T> T getOcTableClient(String name,
                                            ConflictDetection level) throws Exception;
  protected abstract DataSetManager getOcTableManager() throws Exception;

  protected abstract <T> T getMetricsTableClient(String name) throws Exception;
  protected abstract DataSetManager getMetricsTableManager() throws Exception;

  @Override
  protected  final <T> T getDataSetClient(String name,
                                          Class<? extends T> type,
                                          @Nullable Properties props) throws Exception {
    if (type == OrderedColumnarTable.class) {
      ConflictDetection level = null;
      if (props != null) {
        String levelProperty = props.getProperty("conflict.level");
        level = levelProperty == null ? null : ConflictDetection.valueOf(levelProperty);
      }
      // using ROW by default
      level = level == null ? ConflictDetection.ROW : level;
      return getOcTableClient(name, level);
    }
    if (type == MetricsTable.class) {
      return getMetricsTableClient(name);
    }

    return null;
  }

  @Override
  protected final DataSetManager getDataSetManager(Class type) throws Exception {
    if (type == OrderedColumnarTable.class) {
      return getOcTableManager();
    }
    if (type == MetricsTable.class) {
      return getMetricsTableManager();
    }
    return null;
  }

}
