package com.continuuity.data;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset.api.DataSetClient;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.dataset.lib.table.hbase.HBaseOcTableClient;
import com.continuuity.data2.dataset.lib.table.hbase.HBaseOcTableManager;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 *
 */
public class DistributedDataSetAccessor implements DataSetAccessor {
  private final CConfiguration cConf;
  private final Configuration hConf;

  @Inject
  public DistributedDataSetAccessor(@Named("HBaseOVCTableHandleCConfig") CConfiguration cConf,
                                    @Named("HBaseOVCTableHandleHConfig") Configuration hConf)
    throws IOException {
    this.cConf = cConf;
    this.hConf = hConf;
  }

  @Override
  public DataSetClient getDataSetClient(String name, Class type) throws Exception {
    if (type == OrderedColumnarTable.class) {
      return new HBaseOcTableClient(name, cConf, hConf);
    }

    return null;
  }

  @Override
  public DataSetManager getDataSetManager(Class type) throws Exception {
    if (type == OrderedColumnarTable.class) {
      return new HBaseOcTableManager(cConf, hConf);
    }

    return null;
  }
}

